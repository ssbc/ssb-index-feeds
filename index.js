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
  stop: 'sync',
}

exports.init = function init(sbot) {
  if (!sbot.db || !sbot.db.query) {
    throw new Error('ssb-index-feed-writer requires ssb-db2')
  }
  if (!sbot.metafeeds) {
    throw new Error('ssb-index-feed-writer requires ssb-meta-feeds')
  }

  const rootMetafeedP = pify(sbot.metafeeds.findOrCreate)()

  const indexesMetafeedP = rootMetafeedP.then((metafeed) =>
    pify(sbot.metafeeds.findOrCreate)(
      metafeed,
      (f) => f.feedpurpose === 'indexes',
      { feedpurpose: 'indexes', feedformat: 'bendybutt-v1' }
    )
  )

  /**
   * Data structure that tracks all indexes being synchronized.
   * Map of "stringified query" -> "pull-stream drainer"
   */
  const tasks = new Map()

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

    pull(
      // start chain with a dummy value
      pull.values([null]),

      // fetch the last message in the index feed
      pull.asyncMap(function getLastIndexMsg(x, cb) {
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
        if (!latestIndexMsg) return cb(null, 0)
        const { indexed } = latestIndexMsg.value.content
        sbot.db.get(indexed, (err, msgVal) => {
          if (err) return cb(err)
          const latestSequence = msgVal.sequence
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
          // Live
          sbot.db.query(where(matchesQuery), live(), toPullStream()),
        ])
      }),
      pull.flatten(),

      // For each indexable message, write to the index feed
      pull.asyncMap(function writeToIndexFeed(msg, cb) {
        sbot.db.publishAs(
          indexFeed.keys,
          { type: 'metafeed/index', indexed: msg.key },
          cb
        )
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
   */
  function stop(query) {
    try {
      QL0.validate(query)
    } catch (err) {
      console.warn(err)
      return
    }

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
    for (const task of tasks.values()) task.abort()
    tasks.clear()
    fn.apply(this, args)
  })

  return {
    start,
    stop,
  }
}
