// SPDX-FileCopyrightText: 2021 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: LGPL-3.0-only

const debug = require('debug')('ssb:index-feed-writer')
const pull = require('pull-stream')
const cat = require('pull-cat')
const {
  where,
  and,
  gt,
  author,
  descending,
  paginate,
  batch,
  live,
  toPullStream,
  toCallback,
} = require('ssb-db2/operators')
const pify = require('promisify-4loc')
const { QL0 } = require('ssb-subset-ql')

exports.name = 'indexFeedWriter'
exports.version = '1.0.0'
exports.manifest = {
  start: 'async',
  doneOld: 'async',
  stop: 'sync',
}

exports.init = function init(sbot, config) {
  if (!sbot.db || !sbot.db.query) {
    throw new Error('ssb-index-feed-writer requires ssb-db2')
  }
  if (!sbot.metafeeds) {
    throw new Error('ssb-index-feed-writer requires ssb-meta-feeds')
  }

  let indexesMetafeedP

  if (
    config &&
    config.indexFeedWriter &&
    Array.isArray(config.indexFeedWriter.autostart)
  ) {
    setTimeout(() => {
      debug('autostart is enabled with %j', config.indexFeedWriter.autostart)
      for (const incompleteQuery of config.indexFeedWriter.autostart) {
        const query = { ...incompleteQuery, author: sbot.id }
        sbot.indexFeedWriter.start(query, (err) => {
          if (err) console.error(err)
        })
      }
    })
  }

  /**
   * Data structure that tracks all indexes being synchronized.
   * Map of "stringified query" -> "pull-stream drainer"
   */
  const tasks = new Map()
  let taskCount = 0

  const tasksDoneWithOld = new Set()
  const doneOldListeners = new Map()

  function schedule(indexFeed) {
    const queryQL0 = QL0.parse(indexFeed.metadata.query)
    const queryID = QL0.stringify(queryQL0)

    if (tasks.has(queryID)) {
      console.warn(
        'ssb-index-feed-writer: Not scheduling writing ' +
          queryID +
          ' because there already is one'
      )
      return
    }

    const drainer = pull.drain(
      () => {},
      (err) => {
        if (err) {
          console.warn(
            'ssb-index-feed-writer: task for query ' + queryID + ' failed: ',
            err
          )
          tasks.delete(queryID)
        }
      }
    )
    tasks.set(queryID, drainer)

    taskCount += 1
    const debugTask = debug.extend('task' + taskCount)
    debugTask('setup for query %s', queryID)

    pull(
      // start chain with a dummy value
      pull.values([null]),

      // fetch the last message in the index feed
      pull.asyncMap(function getLastIndexMsg(x, cb) {
        debugTask('setup: get last index msg from the db')
        sbot.db.query(
          where(author(indexFeed.subfeed)),
          descending(),
          paginate(1),
          toCallback((err, answer) => {
            if (err) cb(err)
            else cb(null, answer.results[0])
          })
        )
      }),

      // fetch the `msg.value.sequence` for the last indexed `msg`
      pull.asyncMap(function getLatestSequence(latestIndexMsg, cb) {
        if (!latestIndexMsg) {
          debugTask('setup: latest sequence is 0')
          return cb(null, 0)
        }
        const key = latestIndexMsg.value.content.indexed.key
        sbot.db.get(key, (err, msgVal) => {
          if (err) return cb(err)
          const latestSequence = msgVal.sequence
          debugTask('setup: latest sequence is %d', latestSequence)
          cb(null, latestSequence)
        })
      }),

      // stream all subsequent indexable messages that match the query
      pull.map(function expandStream(latestSequence) {
        const matchesQuery = QL0.toOperator(queryQL0, true)
        return cat([
          // Old
          sbot.db.query(
            where(and(matchesQuery, gt(latestSequence, 'sequence'))),
            batch(75),
            toPullStream()
          ),
          pull.values([{ sync: true }]),
          // Live
          sbot.db.query(where(matchesQuery), live(), toPullStream()),
        ])
      }),
      pull.flatten(),

      // For each indexable message, write to the index feed
      pull.asyncMap(function writeToIndexFeed(msg, cb) {
        if (msg.sync) {
          tasksDoneWithOld.add(queryID)
          const listeners = doneOldListeners.get(queryID) || []
          doneOldListeners.delete(queryID)
          for (const listener of listeners) {
            listener()
          }
          cb()
        } else {
          const content = {
            type: 'metafeed/index',
            indexed: { key: msg.key, sequence: msg.value.sequence },
          }
          debugTask('write index msg for %o', content.indexed)
          sbot.db.publishAs(indexFeed.keys, content, cb)
        }
      }),

      drainer
    )
  }

  /**
   * @param {string | import('./ql0').QueryQL0} query ssb-ql-0 query
   * @param {Function} cb callback function
   */
  async function start(query, cb) {
    try {
      QL0.validate(query)
    } catch (err) {
      cb(err)
      return
    }
    const author = QL0.parse(query).author
    if (author !== sbot.id) {
      cb(new Error('Can only index our own messages, but got author ' + author))
    }
    debug('start() requested for %o', query)

    if (!indexesMetafeedP) {
      debug('loading up indexes meta feed')
      const rootMetafeedP = pify(sbot.metafeeds.findOrCreate)()
      indexesMetafeedP = rootMetafeedP.then((metafeed) =>
        pify(sbot.metafeeds.findOrCreate)(
          metafeed,
          (f) => f.feedpurpose === 'indexes',
          { feedpurpose: 'indexes', feedformat: 'bendybutt-v1' }
        )
      )
    }
    const indexesMF = await indexesMetafeedP

    sbot.metafeeds.findOrCreate(
      indexesMF,
      (f) =>
        f.feedpurpose === 'index' &&
        f.metadata.querylang === 'ssb-ql-0' &&
        QL0.isEquals(f.metadata.query, query),
      {
        feedpurpose: 'index',
        feedformat: 'classic',
        metadata: { querylang: 'ssb-ql-0', query: QL0.stringify(query) },
      },
      (err, indexSubfeed) => {
        if (err) return cb(err)
        cb(null, indexSubfeed)
        schedule(indexSubfeed)
      }
    )
  }

  /**
   * @param {string | import('./ql0').QueryQL0} query ssb-ql-0 query
   * @param {Function} cb callback function
   */
  function doneOld(query, cb) {
    try {
      QL0.validate(query)
    } catch (err) {
      cb(err)
      return
    }
    const queryQL0 = QL0.parse(query)
    const queryID = QL0.stringify(queryQL0)

    if (tasksDoneWithOld.has(queryID)) {
      cb()
    } else {
      const listeners = doneOldListeners.get(queryID) || []
      listeners.push(cb)
      doneOldListeners.set(queryID, listeners)
    }
  }

  /**
   * @param {string | import('./ql0').QueryQL0} query ssb-ql-0 query
   */
  function stop(query) {
    try {
      QL0.validate(query)
    } catch (err) {
      console.warn(err)
      return
    }
    debug('stop() requested for %o', query)

    const queryQL0 = QL0.parse(query)
    const queryID = QL0.stringify(queryQL0)
    if (!tasks.has(queryID)) {
      console.warn(
        'ssb-index-feed-writer: unnecessary stop() for query ' +
          queryID +
          ' which wasnt running anyway'
      )
      return
    }

    const task = tasks.get(queryID)
    task.abort()
    tasks.delete(queryID)
  }

  // When sbot closes, stop all tasks
  sbot.close.hook(function (fn, args) {
    debug('teardown by cancelling all tasks')
    for (const task of tasks.values()) task.abort()
    tasks.clear()
    fn.apply(this, args)
  })

  return {
    start,
    stop,
    doneOld,
  }
}
