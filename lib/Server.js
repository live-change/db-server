const fs = require('fs')
const path = require('path')
const rimraf = require("rimraf")
const http = require("http")
const express = require("express")
const sockjs = require('sockjs')
const WebSocketServer = require('websocket').server
const ReactiveDaoWebsocketServer = require("@live-change/dao-websocket").server
const ScriptContext = require('@live-change/db/lib/ScriptContext.js')


const ReactiveDao = require("@live-change/dao")

const Database = require('@live-change/db').Database

class DatabaseStore {
  constructor(path, backend, options) {
    this.path = path
    this.backend = backend
    this.stores = new Map()

    this.db = backend.createDb(path, options)
  }
  close() {
    return this.backend.closeDb(this.db)
  }
  delete() {
    return this.backend.deleteDb(this.db)
  }
  getStore(name, options = {}) {
    let store = this.stores.get(name)
    if(store) return store
    store = this.backend.createStore(this.db, name, options)
    this.stores.set(name, store)
    return store
  }
  closeStore(name) {
    let store = this.stores.get(name)
    if(!store) return;
    return this.backend.closeStore(store)
  }
  deleteStore(name) {
    let store = this.getStore(name)
    return this.backend.deleteStore(store)
  }
}

class Server {
  constructor(config) {
    this.config = config
    this.databases = new Map()
    this.metadata = null
    this.databaseStores = new Map()

    this.databasesListObservable = new ReactiveDao.ObservableList([])

    this.apiServer = new ReactiveDao.ReactiveServer((sessionId) => this.createDao(sessionId))

    if(config.backend == 'leveldb') {
      this.backend = {
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
        closeStore(store) {},
        async deleteStore(store) {
          await store.clear()
        }
      }
    } else if(config.backend == 'rocksdb') {
      this.backend = {
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
        closeStore(store) {},
        async deleteStore(store) {
          await store.clear()
        }
      }
    } else if(config.backend == 'mem') {
      this.backend = {
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
        closeStore(store) {},
        async deleteStore(store) {
          await store.clear()
        }
      }
    } else if(config.backend == 'lmdb') {
      this.backend = {
        lmdb: require('node-lmdb'),
        Store: require('@live-change/db-store-lmdb'),
        createDb(path, options) {
          fs.mkdirSync(path, { recursive: true })
          const env = new this.lmdb.Env()
          env.open({
            path: path,
            maxDbs: 1000,
            mapSize: 1 * 1024 * 1024 * 1024,
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
  createDao(session) {
    const scriptContext = new ScriptContext({
      /// TODO: script available routines
    })
    return new ReactiveDao(session, {
      database: {
        type: 'local',
        source: new ReactiveDao.SimpleDao({
          methods: {
            createDatabase: async (dbName, options = {}) => {
              if(this.metadata.databases[dbName]) throw new Error("databaseAlreadyExists")
              this.metadata.databases[dbName] = options
              const database = await this.initDatabase(dbName, options)
              this.databases.set(dbName, database)
              this.databasesListObservable.push(dbName)
              await this.saveMetadata()
              return 'ok'
            },
            deleteDatabase: async (dbName) => {
              if(!this.metadata.databases[dbName]) throw new Error("databaseNotFound")
              delete this.metadata.databases[dbName]
              this.databases.get(dbName).delete()
              this.databaseStores.get(dbName).delete()
              this.databasesListObservable.remove(dbName)
              return 'ok'
            },
            createTable: async (dbName, tableName, options = {}) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              await db.createTable(tableName, options)
              return 'ok'
            },
            deleteTable: async (dbName, tableName, options) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              await db.deleteTable(tableName)
            },
            renameTable: async (dbName, tableName, newTableName) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const table = db.table(tableName)
              if(!table) throw new Error("tableNotFound")
              return db.renameTable(tableName, newTableName)
            },
            createIndex: async (dbName, indexName, code, params, options = {}) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              await db.createIndex(indexName, code, params, options)
              return 'ok'
            },
            deleteIndex: async (dbName, indexName, options) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              await db.deleteIndex(indexName)
              return 'ok'
            },
            renameIndex: async (dbName, indexName, newIndexName) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const index = db.index(indexName)
              if(!index) throw new Error("indexNotFound")
              return db.renameIndex(indexName, newIndexName)
            },
            createLog: async (dbName, logName, options = {}) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              await db.createLog(logName, options)
              return 'ok'
            },
            deleteLog: async (dbName, logName, options) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              await db.deleteLog(logName)
              return 'ok'
            },
            renameLog: async (dbName, logName, newLogName) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const log = db.log(logName)
              if(!log) throw new Error("logNotFound")
              return db.renameLog(logName, newLogName)
            },
            put: (dbName, tableName, object) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const table = db.table(tableName)
              if(!table) throw new Error("tableNotFound")
              return table.put(object)
            },
            delete: (dbName, tableName, id) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const table = db.table(tableName)
              if(!table) throw new Error("tableNotFound")
              return table.delete(id)
            }, 
            update: (dbName, tableName, id, operations) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const table = db.table(tableName)
              if(!table) throw new Error("tableNotFound")
              return table.update(id, operations)
            },
            putLog: (dbName, logName, object) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const log = db.log(logName)
              if(!log) throw new Error("logNotFound")
              return log.put(object)
            },
            flagLog: (dbName, logName, id, flags) => {
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const log = db.log(logName)
              if(!log) throw new Error("logNotFound")
              return log.flag(id, flags)
            },
            query: (dbName, code, params) => {
              if(!dbName) throw new Error("databaseNameRequired")
              const db = this.databases.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const queryFunction = scriptContext.run(code, 'query')
              return db.queryGet((input, output) => queryFunction(input, output, params))
            }
          },
          values: {
            databasesList: {
              observable: () => this.databasesListObservable,
              get: async () => this.databasesListObservable.list
            },
            databaseConfig: {
              observable: (dbName) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.configObservable
              },
              get: async (dbName) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.configObservable.value
              }
            },
            tablesList: {
              observable: (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.tablesListObservable
              },
              get: async (dbName, tableName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.tablesListObservable.list
              }
            },
            indexesList: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.indexesListObservable
              },
              get: async (dbName, indexName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.indexesListObservable.list
              }
            },
            logsList: {
              observable: (dbName, logName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                return db.logsListObservable
              },
              get: async (dbName, logName, id) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                return db.logsListObservable.list
              }
            },
            tableConfig: {
              observable: (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.configObservable
              },
              get: async (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.configObservable.value
              }
            },
            indexConfig: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.configObservable
              },
              get: async (dbName, indexName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.configObservable.value
              }
            },
            indexCode: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.codeObservable
              },
              get: async (dbName, indexName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.codeObservable.value
              }
            },
            logConfig: {
              observable: (dbName, logName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const log = db.log(logName)
                if(!log) return new ReactiveDao.ObservableError('logNotFound')
                return log.configObservable
              },
              get: async (dbName, logName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const log = db.log(logName)
                if(!log) throw new Error("logNotFound")
                return log.configObservable.value
              }
            },
            tableObject: {
              observable: (dbName, tableName, id) => {
                if(!id) return new ReactiveDao.ObservableError("id is required")
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.objectObservable(id)
              },
              get: async (dbName, tableName, id) =>{
                if(!id) throw new Error("id is required")
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.objectGet(id)
              }
            },
            tableRange: {
              observable: (dbName, tableName, range) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.rangeObservable(range)
              },
              get: async (dbName, tableName, range) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.rangeGet(range)
              }
            },
            tableOpLogObject: {
              observable: (dbName, tableName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.opLog.objectObservable(id)
              },
              get: async (dbName, tableName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.opLog.objectGet(id)
              }
            },
            tableOpLogRange: {
              observable: (dbName, tableName, range) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const table = db.table(tableName)
                if(!table) return new ReactiveDao.ObservableError('tableNotFound')
                return table.opLog.rangeObservable(range)
              },
              get: async (dbName, tableName, range) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const table = db.table(tableName)
                if(!table) throw new Error("tableNotFound")
                return table.opLog.rangeGet(range)
              }
            },
            indexObject: {
              observable: async (dbName, indexName, id) => {
                if(!id) return new ReactiveDao.ObservableError("id is required")
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = await db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.objectObservable(id)
              },
              get: async (dbName, indexName, id) => {
                if(!id) throw new Error("id is required")
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.objectGet(id)
              }
            },
            indexRange: {
              observable: async (dbName, indexName, range) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = await db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.rangeObservable(range)
              },
              get: async (dbName, indexName, range) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = await db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.rangeGet(range)
              }
            },
            indexOpLogObject: {
              observable: (dbName, indexName, id) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.opLog.objectObservable(id)
              },
              get: async (dbName, indexName, id) =>{
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.opLog.objectGet(id)
              }
            },
            indexOpLogRange: {
              observable: (dbName, indexName, range) => {
                if(!id) return new ReactiveDao.ObservableError("id is required")
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const index = db.index(indexName)
                if(!index) return new ReactiveDao.ObservableError('indexNotFound')
                return index.opLog.rangeObservable(range)
              },
              get: async (dbName, indexName, range) => {
                if(!id) throw new Error("id is required")
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const index = db.index(indexName)
                if(!index) throw new Error("indexNotFound")
                return index.opLog.rangeGet(range)
              }
            },
            logObject: {
              observable: (dbName, logName, id) => {

                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const log = db.log(logName)
                if(!log) return new ReactiveDao.ObservableError('logNotFound')
                return log.objectObservable(id)
              },
              get: (dbName, logName, id) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const log = db.log(logName)
                if(!log) throw new Error("logNotFound")
                return log.objectGet(id)
              }
            },
            logRange: {
              observable: (dbName, logName, range) => {
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const log = db.log(logName)
                if(!log) return new ReactiveDao.ObservableError('logNotFound')
                return log.rangeObservable(range)
              },
              get: async (dbName, logName, range) => {
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const log = db.log(logName)
                if(!log) throw new Error("logNotFound")
                return log.rangeGet(range)
              }
            },
            query: {
              observable: (dbName, code, params = {}) => {
                if(!dbName) return new ReactiveDao.ObservableError("databaseNameRequired")
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const queryFunction = scriptContext.run(code, 'query')
                return db.queryObservable(async (input, output) => {
                  return queryFunction(input, output, params)
                })
              },
              get: async (dbName, code, params = {}) => {
                if(!dbName) throw new Error("databaseNameRequired")
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const queryFunction = scriptContext.run(code, 'query')
                return db.queryGet((input, output) => queryFunction(input, output, params))
              }
            },
            queryObject: {
              observable: (dbName, code, params = {}) => {
                if(!dbName) return new ReactiveDao.ObservableError("databaseNameRequired")
                const db = this.databases.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const queryFunction = scriptContext.run(code, 'query')
                return db.queryObjectObservable(async (input, output) => {
                  return queryFunction(input, output, params)
                })
              },
              get: async (dbName, code, params = {}) => {
                if(!dbName) throw new Error("databaseNameRequired")
                const db = this.databases.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const queryFunction = scriptContext.run(code, 'query')
                return db.queryObjectGet((input, output) => queryFunction(input, output, params))
              }
            }
          }
        })
      },
      store: { /// Low level data access
        type: 'local',
        source: new ReactiveDao.SimpleDao({
          methods: {
            put: (dbName, storeName, object) => {
              const db = this.databaseStores.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const store = db.stores.get(storeName)
              if(!store) throw new Error('storeNotFound')
              return store.put(object)
            },
            delete: (dbName, storeName, id) => {
              const db = this.databaseStores.get(dbName)
              if(!db) throw new Error('databaseNotFound')
              const store = db.stores.get(storeName)
              if(!store) throw new Error('storeNotFound')
              return store.delete(id)
            }
          },
          values: {
            object: {
              observable(dbName, storeName, id) {
                const db = this.databaseStores.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const store = db.stores.get(storeName)
                if(!store) return new ReactiveDao.ObservableError('storeNotFound')
                return store.objectObservable(id)
              },
              get: (dbName, storeName, id) => {
                const db = this.databaseStores.get(dbName)
                if(!db) throw new Error('databaseNotFound')
                const store = db.stores.get(storeName)
                if(!store) throw new Error('storeNotFound')
                return store.objectGet(id)
              }
            },
            range: {
              observable(dbName, storeName, range) {
                const db = this.databaseStores.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const store = db.stores.get(storeName)
                if(!store) return new ReactiveDao.ObservableError('storeNotFound')
                return storeName.rangeObservable(range)
              },
              get: async (dbName, storeName, range) => {
                const db = this.databaseStores.get(dbName)
                if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
                const store = db.stores.get(storeName)
                if(!store) return new ReactiveDao.ObservableError('storeNotFound')
                return storeName.rangeGet(range)
              }
            }
          }
        })
      }
    })
  }
  async initialize(initOptions = {}) {
    const normalMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json')
    const backupMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json.bak')
    const normalMetadataExists = await fs.promises.access(normalMetadataPath).catch(err => false)
    const backupMetadataExists = await fs.promises.access(backupMetadataPath).catch(err => false)
    if(initOptions.forceNew && (normalMetadataExists || backupMetadataExists)) 
      throw new Error("database already exists")
    const normalMetadata = await fs.promises.readFile(normalMetadataPath, "utf8")
        .then(json => JSON.parse(json)).catch(err => null)
    const backupMetadata = await fs.promises.readFile(backupMetadataPath, "utf8")
        .then(json => JSON.parse(json)).catch(err => null)
    if((normalMetadataExists || backupMetadataExists) && !normalMetadata && !backupMetadata)
      throw new Error("database is broken")
    this.metadata = normalMetadata
    if(!normalMetadata) this.metadata = backupMetadata
    if(this.metadata && backupMetadata && this.metadata.timestamp < backupMetadata.timestamp)
      this.metadata = backupMetadata
    if(!this.metadata) {
      this.metadata = {
        databases: {}
      }
    }
    await this.saveMetadata()
    for(const dbName in this.metadata.databases) {
      const dbConfig = this.metadata.databases[dbName]
      this.databases.set(dbName, await this.initDatabase(dbName, dbConfig))
      this.databasesListObservable.push(dbName)
    }
  }
  async initDatabase(dbName, dbConfig) {
    const dbPath = path.resolve(this.config.dbRoot, dbName+'.db')
    let dbStore = this.databaseStores.get(dbName)
    if(!dbStore) {
      dbStore = new DatabaseStore(dbPath, this.backend, dbConfig.storage)
      this.databaseStores.set(dbName, dbStore)
    }
    return new Database(
        dbConfig,
        (name, config) => dbStore.getStore(name, config),
        (configToSave) => {
          this.metadata.databases[dbName] = configToSave
          this.saveMetadata()
        })
  }
  async saveMetadata() {
    //console.log("SAVE METADATA\n"+JSON.stringify(this.metadata, null, "  "))
    const normalMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json')
    const backupMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json.bak')
    this.metadata.timestamp = Date.now()
    await fs.promises.writeFile(normalMetadataPath, JSON.stringify(this.metadata))
    await fs.promises.writeFile(backupMetadataPath, JSON.stringify(this.metadata))
  }

  getHttp() {
    if(!this.http) {
      const app = express()
      const sockJsServer = sockjs.createServer({});
      sockJsServer.on('connection', function (conn) {
        console.log("SOCKJS connection")
        this.apiServer.handleConnection(conn)
      })
      const server = http.createServer(app)
      let wsServer = new WebSocketServer({ httpServer: server, autoAcceptConnections: false })
      wsServer.on("request",(request) => {
        console.log("WS URI", request.httpRequest.url)
        if(request.httpRequest.url != "/api/ws") return request.reject();
        let serverConnection = new ReactiveDaoWebsocketServer(request)
        this.apiServer.handleConnection(serverConnection)
      })
      sockJsServer.installHandlers(server, {prefix: '/api/sockjs'})
      this.http = {
        app,
        sockJsServer,
        wsServer,
        server
      }
    }
    return this.http
  }

  listen(...args) {
    this.getHttp().server.listen(...args)
  }

  async close() {
    for(const db of this.databaseStores.values()) db.close()
  }
}

module.exports = Server
