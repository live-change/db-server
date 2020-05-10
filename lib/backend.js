const fs = require('fs')
const path = require('path')
const rimraf = require("rimraf-promise")

function createBackend(config) {
  if(config.backend == 'leveldb') {
    return {
      levelup: require('levelup'),
      leveldown: require('leveldown'),
      subleveldown: require('subleveldown'),
      encoding: require('encoding-down'),
      Store: require('@live-change/db-store-level'),
      createDb(path, options) {
        const db = this.levelup(this.leveldown(path, options), options)
        db.path = path
        return db
      },
      closeDb(db) {
        db.close()
      },
      async deleteDb(db) {
        db.close()
        await rimraf(db.path)
      },
      createStore(db, name, options) {
        return new this.Store(this.subleveldown(db, name,
            { ...options, keyEncoding: 'ascii', valueEncoding: 'json' }))
      },
      closeStore(store) {
      },
      async deleteStore(store) {
        await store.clear()
      }
    }
  } else if(config.backend == 'rocksdb') {
    return {
      levelup: require('levelup'),
      rocksdb: require('level-rocksdb'),
      subleveldown: require('subleveldown'),
      encoding: require('encoding-down'),
      Store: require('@live-change/db-store-level'),
      createDb(path, options) {
        const db = this.levelup(this.rocksdb(path, options), options)
        db.path = path
        return db
      },
      closeDb(db) {
        db.close()
      },
      async deleteDb(db) {
        db.close()
        await rimraf(db.path)
      },
      createStore(db, name, options) {
        return new this.Store(this.subleveldown(db, name,
            { ...options, keyEncoding: 'ascii', valueEncoding: 'json' }))
      },
      closeStore(store) {
      },
      async deleteStore(store) {
        await store.clear()
      }
    }
  } else if(config.backend == 'mem') {
    return {
      levelup: require('levelup'),
      memdown: require('memdown'),
      subleveldown: require('subleveldown'),
      encoding: require('encoding-down'),
      Store: require('@live-change/db-store-level'),
      createDb(path, options) {
        const db = this.levelup(this.memdown(path, options), options)
        db.path = path
        return db
      },
      closeDb(db) {
        db.close()
      },
      async deleteDb(db) {
        db.close()
        await rimraf(db.path)
      },
      createStore(db, name, options) {
        return new this.Store(this.subleveldown(db, name,
            { ...options, keyEncoding: 'ascii', valueEncoding: 'json' }))
      },
      closeStore(store) {
      },
      async deleteStore(store) {
        await store.clear()
      }
    }
  } else if(config.backend == 'lmdb') {
    return {
      lmdb: require('node-lmdb'),
      Store: require('@live-change/db-store-lmdb'),
      createDb(path, options) {
        fs.mkdirSync(path, { recursive: true })
        const env = new this.lmdb.Env()
        env.open({
          path: path,
          maxDbs: 1000,
          mapSize: 10 * 1024 * 1024 * 1024,
          ...options
        })
        env.path = path
        return env
      },
      closeDb(db) {
        db.close()
      },
      async deleteDb(db) {
        db.close()
        await rimraf(db.path)
      },
      createStore(db, name, options) {
        return new this.Store(db,
            db.openDbi({
              name,
              create: true
            })
        )
      },
      closeStore(store) {
        store.lmdb.close()
      },
      async deleteStore(store) {
        store.lmdb.drop()
      }
    }
  }
}

module.exports = createBackend
