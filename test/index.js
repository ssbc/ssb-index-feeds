// SPDX-FileCopyrightText: 2021 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const OS = require('os')
const FS = require('fs')
const Path = require('path')
const pull = require('pull-stream')
const ssbKeys = require('ssb-keys')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const run = require('promisify-tuple')
const sleep = require('util').promisify(setTimeout)
const generateFixture = require('ssb-fixtures')
const {
  where,
  and,
  authorIsBendyButtV1,
  author,
  type,
  count,
  toPromise,
} = require('ssb-db2/operators')

const dir = Path.join(OS.tmpdir(), 'index-feeds')
rimraf.sync(dir, { maxBusyTries: 3 })
mkdirp.sync(dir)

let mainKey
let VOTES_COUNT = 0

test('setup', async (t) => {
  await generateFixture({
    outputDir: dir,
    seed: 'ssbindexfeedwriter',
    messages: 50,
    authors: 1,
    slim: false,
  })

  t.true(
    FS.existsSync(Path.join(dir, 'flume', 'log.offset')),
    'generated fixture with flumelog-offset'
  )
  mainKey = ssbKeys.loadOrCreateSync(Path.join(dir, 'secret'))

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db2'))
    .call(null, {
      keys: mainKey,
      path: dir,
      db2: {
        dangerouslyKillFlumeWhenMigrated: true,
      },
    })

  sbot.db2migrate.start()

  await new Promise((resolve, reject) => {
    pull(
      sbot.db2migrate.progress(),
      pull.filter((x) => x === 1),
      pull.take(1),
      pull.collect((err) => {
        if (err) reject(err)
        else resolve()
      })
    )
  })
  t.pass('migrated flumelog to ssb-db2')

  await sleep(500)

  const votesCount = await sbot.db.query(
    where(and(author(sbot.id), type('vote'))),
    count(),
    toPromise()
  )
  t.true(votesCount > 3, 'more than 3 votes exist')
  VOTES_COUNT = votesCount

  await run(sbot.close)(true)
  t.end()
})

let sbot
let indexFeedID

test('update index feed for votes a bit', (t) => {
  sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db2'))
    .use(require('ssb-bendy-butt'))
    .use(require('ssb-meta-feeds'))
    .use(require('../'))
    .call(null, {
      keys: mainKey,
      path: dir,
    })

  // Make it slow so we can cancel in between
  let published = 0
  const originalCreate = sbot.db.create
  sbot.db.create = function (opts, cb) {
    if (opts.content.type !== 'metafeed/index') {
      return originalCreate.call(sbot.db, opts, cb)
    }

    setTimeout(() => {
      if (published >= 3) return

      originalCreate.call(sbot.db, opts, cb)
      published += 1

      if (published >= 3) {
        sbot.indexFeeds.stop({
          author: sbot.id,
          type: 'vote',
          private: false,
        })
        t.pass('stopped task')
        sbot.close(true, t.end)
      }
    })
  }

  // Lets check that creation of the root meta feed is lazy
  setTimeout(async () => {
    const msgs = await sbot.db.query(where(authorIsBendyButtV1()), toPromise())
    t.equals(msgs.length, 0, 'zero bendy butt messages')

    sbot.indexFeeds.start(
      { author: sbot.id, type: 'vote', private: false },
      (err, indexFeed) => {
        t.pass('started task')
        t.error(err, 'no err')
        t.ok(indexFeed, 'index feed returned')
        indexFeedID = indexFeed.id
      }
    )
  }, 3000)
})

test('restarting sbot continues writing index where left off', async (t) => {
  sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db2'))
    .use(require('ssb-bendy-butt'))
    .use(require('ssb-meta-feeds'))
    .use(require('../'))
    .call(null, {
      keys: mainKey,
      path: dir,
    })

  const votes = await sbot.db.query(where(author(indexFeedID)), toPromise())
  t.equals(votes.length, 3, '3 votes previously indexed')

  t.pass('started task')
  const [err, indexFeed] = await run(sbot.indexFeeds.start)(
    JSON.stringify({
      author: sbot.id,
      type: 'vote',
      private: false,
    })
  )
  t.error(err, 'no err')
  t.ok(indexFeed, 'index feed returned')
  t.equals(indexFeed.id, indexFeedID, 'it is the same as before')

  await run(sbot.indexFeeds.doneOld)(
    JSON.stringify({
      author: sbot.id,
      type: 'vote',
      private: false,
    })
  )
  t.pass('doneOld called')

  const allVotes = await sbot.db.query(where(author(indexFeedID)), toPromise())
  t.equals(allVotes.length, VOTES_COUNT, 'all votes were indexed')

  const indexMsgs = await sbot.db.query(
    where(type('metafeed/index')),
    toPromise()
  )
  t.equals(indexMsgs.length, VOTES_COUNT, 'index msgs exist')

  t.end()
})

test('live updates get written to the index', (t) => {
  sbot.db.publish({ type: 'vote', vote: { value: 1 } }, async (err) => {
    t.error(err, 'no err')

    await sleep(300)

    const allVotes = await sbot.db.query(
      where(author(indexFeedID)),
      toPromise()
    )
    t.equals(allVotes.length, VOTES_COUNT + 1, 'one more vote was indexed')

    t.end()
  })
})

test('doneOld is called immediately on an empty index feed', async (t) => {
  const [err, indexFeed] = await run(sbot.indexFeeds.start)({
    author: sbot.id,
    type: 'other',
    private: false,
  })
  t.error(err, 'no err')
  t.ok(indexFeed, 'index feed returned')

  await run(sbot.indexFeeds.doneOld)({
    author: sbot.id,
    type: 'other',
    private: false,
  })
  t.pass('doneOld called')

  t.end()
})

test('autostart calls start on each array item', (t) => {
  sbot.close(true, () => {
    sbot = SecretStack({ appKey: caps.shs })
      .use(require('ssb-db2'))
      .use(require('ssb-bendy-butt'))
      .use(require('ssb-meta-feeds'))
      .use(require('../'))
      .call(null, {
        keys: mainKey,
        path: dir,
        indexFeeds: {
          autostart: [
            { type: 'vote', private: false },
            { type: 'post', private: false },
            { type: null, private: true },
          ],
        },
      })

    const expected = [
      { author: sbot.id, type: 'vote', private: false },
      { author: sbot.id, type: 'post', private: false },
      { author: sbot.id, type: null, private: true },
    ]

    sbot.indexFeeds.start = function fakeStart(query, cb) {
      t.true(expected.length > 0, 'we expect a start() to be called')
      t.deepEquals(query, expected.shift(), 'start() arg is correct')
      cb(null, null)

      if (expected.length === 0) {
        setTimeout(t.end, 1000)
      }
    }
  })
})

test('teardown', (t) => {
  sbot.close(true, t.end)
})
