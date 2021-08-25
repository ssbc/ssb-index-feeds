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
const fromEvent = require('pull-stream-util/from-event')
const sleep = require('util').promisify(setTimeout)
const generateFixture = require('ssb-fixtures')
const {
  where,
  and,
  author,
  type,
  count,
  toPromise,
} = require('ssb-db2/operators')

const dir = Path.join(OS.tmpdir(), 'index-feed-writer')
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
      fromEvent('ssb:db2:migrate:progress', sbot),
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
    .use(require('ssb-meta-feeds'))
    .use(require('../'))
    .call(null, {
      keys: mainKey,
      path: dir,
    })

  // Make it slow so we can cancel in between
  let stopped = false
  const PERIOD = 200
  const originalPublishAs = sbot.db.publishAs
  sbot.db.publishAs = function (keys, content, cb) {
    setTimeout(() => {
      if (stopped) return
      originalPublishAs.call(sbot.db, keys, content, cb)
    }, PERIOD)
  }

  sbot.indexFeedWriter.start(
    { author: sbot.id, type: 'vote' },
    (err, indexFeed) => {
      t.pass('started task')
      t.error(err, 'no err')
      t.ok(indexFeed, 'index feed returned')
      indexFeedID = indexFeed.subfeed

      setTimeout(() => {
        sbot.indexFeedWriter.stop({ author: sbot.id, type: 'vote' })
        stopped = true
        t.pass('stopped task')
        sbot.close(true, t.end)
      }, PERIOD * 3.5)
    }
  )
})

test('restarting sbot continues writing index where left off', async (t) => {
  sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db2'))
    .use(require('ssb-meta-feeds'))
    .use(require('../'))
    .call(null, {
      keys: mainKey,
      path: dir,
    })

  const votes = await sbot.db.query(where(author(indexFeedID)), toPromise())
  t.equals(votes.length, 3, '3 votes previously indexed')

  t.pass('started task')
  const [err, indexFeed] = await run(sbot.indexFeedWriter.start)({
    author: sbot.id,
    type: 'vote',
  })
  t.error(err, 'no err')
  t.ok(indexFeed, 'index feed returned')
  t.equals(indexFeed.subfeed, indexFeedID, 'it is the same as before')

  await sleep(300)

  const allVotes = await sbot.db.query(where(author(indexFeedID)), toPromise())
  t.equals(allVotes.length, VOTES_COUNT, 'all votes were indexed')

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

test('teardown', (t) => {
  sbot.close(true, t.end)
})
