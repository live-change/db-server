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
  } else if(config.backend == 'memdown') {
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
  } else if(config.backend == 'mem') {
    return {
      Store: require('@live-change/db-store-rbtree'),
      createDb(path, options) {
        const db = {}
        db.path = path
        return db
      },
      closeDb(db) {
      },
      async deleteDb(db) {
      },
      createStore(db, name, options) {
        return new this.Store()
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
        const envConfig = {
          path: path,
          maxDbs: config.maxDbs || 1000,
          mapSize: config.mapSize || (10 * 1024 * 1024 * 1024),
          ...options
        }
        env.open(envConfig)
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
  } else if(config.backend == 'observabledb') {
    const Store = require('@live-change/db-store-observable-db')
    const connection = new Store.Connection(config.backendUrl || 'ws://localhost:3530/api/ws')
    return {
      Store,
      connection,
      createDb(path, options) {
        const pathSep = path.lastIndexOf('/')
        const dbName = path.slice(pathSep > 0 ? pathSep+1 : 0)
        console.log("CREATE DATABASE!", dbName, options)
        const openPromise = connection.createDatabase(dbName, options || {}).then(ok=>{
          console.log("database", dbName, "created")
        }).catch(err => {
          if(err == 'exists') console.log("database", dbName, "already exists")
            else console.error("CREATE DB ERROR", err)
        })
        return dbName
      },
      closeDb(db) {
        /// remote database - ignore
      },
      async deleteDb(db) {
        return connection.deleteDatabase(db)
      },
      createStore(db, name, options) {
        console.log("CREATE STORE", db, name)
        connection.createStore(db, name, options || {}).then(ok=>{
          console.log("database", db, "store", name, "created")
        }).catch(err => {
          if(err == 'exists') console.log("database", db, "store", name, "already exists")
            else console.error("CREATE STORE ERROR", err)
        })
        return new Store(connection, db, name)
      },
      closeStore(store) {
        return store.close()
      },
      deleteStore(store) {
        return connection.deleteStore(store.databaseName, store.storeName)
      }
    }
  } else throw new Error("Unknown backend " + config.backend)
}

module.exports = createBackend
