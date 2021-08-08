const deepEqual = require('nano-equal')

exports.name = 'indexFeedWriter'
exports.version = '1.0.0'
exports.manifest = {
  start: 'sync',
  stop: 'sync',
}

const QL0 = {
  parse(query) {
    if (!query) return null
    if (typeof query === 'string') {
      try {
        const parsed = JSON.parse(q1)
        if (!parsed.author || !parsed.messageType) return null
        else return parsed
      } catch (err) {
        console.warn('Error parsing QL0 query: ' + query)
        return null
      }
    } else if (typeof query === 'object') {
      if (!query.author || !query.messageType) return null
      else return query
    }
  },

  stringify(query) {
    return JSON.stringify(query)
  },

  isEquals(q1, q2) {
    const q1Obj = this.parse(q1)
    const q2Obj = this.parse(q2)
    return deepEqual(q1Obj, q2Obj)
  },
}

exports.init = function init(sbot) {
  if (!sbot.metafeeds) {
    throw new Error('ssb-index-feed-writer requires ssb-meta-feeds')
  }

  const rootMetafeedP = new Promise((resolve, reject) => {
    sbot.metafeeds.findOrCreate((err, rootMF) => {
      if (err) return reject(err)
      else resolve(rootMF)
    })
  })

  const indexesMetafeedP = rootMetafeedP.then(
    (mf) =>
      new Promise((resolve, reject) => {
        sbot.metafeeds.findOrCreate(
          mf,
          (f) => f.feedpurpose === 'indexes',
          { feedpurpose: 'indexes', feedformat: 'bendy butt' },
          (err, indexesMF) => {
            if (err) return reject(err)
            else resolve(indexesMF)
          }
        )
      })
  )

  // data-structure that tracks all indexes being synchronized

  function schedule(indexFeed) {
    // add to the data structure

    // get the `ops` JSON from indexFeed

    // convert the ops
    const jitdbOps = decodeQL1(ops)

    // make a `live` ssb-db2 query

    // on emission: write to the indexFeed
  }

  /**
   * @param {string | Record<string, any>} query ssb-ql-0 query
   */
  async function start(query) {
    if (!QL0.parse(query)) {
      // TODO warning
      return
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
        console.log(err, indexSubfeed)
        // TODO schedule(indexFeed)
      }
    )
  }

  function stop(ops) {
    // find the indexFeed from the ops
    // if no indexFeed found: return
    // else: remove from the data-structure and stop the sync
  }

  return {
    start,
    stop,
  }
}
