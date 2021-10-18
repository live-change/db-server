const fs = require('fs')
const path = require('path')
const http = require("http")
const express = require("express")
const sockjs = require('sockjs')
const WebSocketServer = require('websocket').server
const ReactiveDaoWebsocketServer = require("@live-change/dao-websocket").server
const ReactiveDaoWebsocketClient = require("@live-change/dao-websocket").client
const ScriptContext = require('@live-change/db/lib/ScriptContext.js')
const dbDao = require('./dbDao.js')
const storeDao = require('./storeDao.js')
const createBackend = require("./backend.js")
const Replicator = require("./Replicator.js")
const { profileLog } = require("@live-change/db")

const ReactiveDao = require("@live-change/dao")

const Database = require('@live-change/db').Database

class DatabaseStore {
  constructor(path, backend, options) {
    this.path = path
    this.backend = backend
    this.options = options
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

    this.metadataSavePromise = null

    this.databasesListObservable = new ReactiveDao.ObservableList([])

    this.apiServer = new ReactiveDao.ReactiveServer((sessionId) => this.createDao(sessionId))

    this.backend = createBackend(config)

    if(this.config.master) {
      this.masterDao = new ReactiveDao('app', {
        remoteUrl: this.config.master,
        protocols: {
          'ws': ReactiveDaoWebsocketClient
        },
        connectionSettings: {
          queueRequestsWhenDisconnected: true,
          requestSendTimeout: 2000,
          requestTimeout: 5000,
          queueActiveRequestsOnDisconnect: false,
          autoReconnectDelay: 200,
          logLevel: 1
        },
        database: {
          type: 'remote',
          generator: ReactiveDao.ObservableList
        },
        store: {
          type: 'remote',
          generator: ReactiveDao.ObservableList
        }
      })
      this.replicator = new Replicator(this)
    }
  }
  createDao(session) {
    const scriptContext = new ScriptContext({
      /// TODO: script available routines
      console
    })
    if(this.config.master) {
      return new ReactiveDao(session, {
        remoteUrl: this.config.master,
        database: {
          type: 'local',
          source: new ReactiveDao.SimpleDao({
            methods: {
              ...(profileLog.started
                  ? profileLog.profileFunctions(dbDao.remoteRequests(this))
                  : dbDao.remoteRequests(this))
            },
            values: {
              ...dbDao.localReads(this, scriptContext)
            }
          })
        },
        /*store: { /// Low level data access
          type: 'remote',
          generator: ReactiveDao.ObservableList
        }*/
        store: { /// Low level data access
          type: 'local',
          source: new ReactiveDao.SimpleDao({
            methods: { // No write access to replica store
            },
            values: {
              ...storeDao.localReads(this)
            }
          })
        }
      })
    } else {
      return new ReactiveDao(session, {
        database: {
          type: 'local',
          source: new ReactiveDao.SimpleDao({
            methods: {
              ...(profileLog.started
                  ? profileLog.profileFunctions(dbDao.localRequests(this, scriptContext))
                  : dbDao.localRequests(this, scriptContext))
            },
            values: {
              ...dbDao.localReads(this, scriptContext)
            }
          })
        },
        store: { /// Low level data access
          type: 'local',
          source: new ReactiveDao.SimpleDao({
            methods: {
              ...(profileLog.started
                  ? profileLog.profileFunctions(storeDao.localRequests(this))
                  : storeDao.localRequests(this))
            },
            values: {
              ...storeDao.localReads(this)
            }
          })
        }
      })
    }
  }
  async initialize(initOptions = {}) {
    if(!this.config.temporary) {
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
      this.metadata = normalMetadata
      if(!normalMetadata) this.metadata = backupMetadata
      if(this.metadata && backupMetadata && this.metadata.timestamp < backupMetadata.timestamp)
        this.metadata = backupMetadata
      if(!this.metadata && (normalMetadataExists || backupMetadataExists)) throw new Error("database is broken")
    }
    if(!this.metadata) {
      this.metadata = {
        databases: {
          system: {
            tables: {},
            indexes: {},
            logs: {}
          }
        }
      }
    }
    for(const dbName in this.metadata.databases) {
      const dbConfig = this.metadata.databases[dbName]
      this.databases.set(dbName, await this.initDatabase(dbName, dbConfig))
      this.databasesListObservable.push(dbName)
    }
    await this.checkInfoIntegrity()
    if(this.config.master) {
      await this.replicator.start()
    }
  }
  async checkInfoIntegrity() {
    for(const dbName in this.metadata.databases) {
      if(dbName != 'system') {
        const metadata = this.metadata.databases[dbName]
        const indexesTable = this.databases.get('system').table(dbName + '_indexes')
        const indexesInfo = await indexesTable.rangeGet({})
        const infoByName = new Map()
        for(const indexInfo of indexesInfo) {
          const indexMeta = metadata.indexes[indexInfo.name]
          if(!indexMeta || (indexMeta.uid != indexInfo.id)) {
            console.error("CORRUPTED INDEX INFO", indexInfo.name, indexInfo.id)
            console.log("DELETING CORRUPTED INFO")
            await indexesTable.delete(indexInfo.id)
            continue
          } else if(infoByName.has(indexInfo.name)) {
            console.error("INDEX INFO DUPLIACTED", indexInfo.name)
            console.log("DB META", )
            console.log("INFO", indexInfo)
          }
          infoByName.set(indexInfo.name, indexInfo)
        }
      }
    }
  }
  async initDatabase(dbName, dbConfig) {
    const dbPath = this.config.temporary ? 'memory' : path.resolve(this.config.dbRoot, dbName+'.db')
    let dbStore = this.databaseStores.get(dbName)
    if(!dbStore) {
      console.log("CREATE DB", dbPath, dbConfig.storage)
      dbStore = new DatabaseStore(dbPath, this.backend, dbConfig.storage)
      this.databaseStores.set(dbName, dbStore)
    }
    const database = new Database(
      dbConfig,
      (name, config) => dbStore.getStore(name, config),
      (configToSave) => {
        this.metadata.databases[dbName] = configToSave
        this.saveMetadata()
      },
      (name) => dbStore.deleteStore(name),
      dbName,
      (context) => new ScriptContext(context)
    )
    database.onAutoRemoveIndex = (name, uid) => {
      this.databases.get('system').table(dbName+'_indexes').delete(uid)
    }
    this.initializingDatabase = database
    await database.start(this.config)
    this.initializingDatabase = undefined
    return database
  }

  async doSaveMetadata() {
    if(this.config.temporary) return
    //console.log("SAVE METADATA\n"+JSON.stringify(this.metadata, null, "  "))
    const normalMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json')
    const backupMetadataPath = path.resolve(this.config.dbRoot, 'metadata.json.bak')
    this.metadata.timestamp = Date.now()
    await fs.promises.writeFile(normalMetadataPath, JSON.stringify(this.metadata, null, "  "))
    await fs.promises.writeFile(backupMetadataPath, JSON.stringify(this.metadata, null, "  "))
  }
  async saveMetadata() {
    if(this.metadataSavePromise) await this.metadataSavePromise
    this.metadataSavePromise = this.doSaveMetadata()
    await this.metadataSavePromise
  }

  getHttp() {
    if(!this.http) {
      const app = express()
      const sockJsServer = sockjs.createServer({});
      sockJsServer.on('connection', (conn) => {
        console.log("SOCKJS connection")
        this.apiServer.handleConnection(conn)
      })
      const server = http.createServer(app)
      let wsServer = new WebSocketServer({ httpServer: server, autoAcceptConnections: false })
      wsServer.on("request",(request) => {
        console.log("WS URI", request.httpRequest.url, "FROM", request.remoteAddress)
        if(request.httpRequest.url != "/api/ws") return request.reject();
        let serverConnection = new ReactiveDaoWebsocketServer(request)
        this.apiServer.handleConnection(serverConnection)
      })
      sockJsServer.installHandlers(server, { prefix: '/api/sockjs' })
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
    if(this.http) {
      this.http.server.close()
    }
    for(const db of this.databaseStores.values()) db.close()
  }

  handleUnhandledRejectionInQuery(reason, promise) {
    console.error("ERROR IN USER CODE:", reason.stack)
    let rest = reason.stack
    while(true) {
      const match = rest.match(/\s(userCode:([a-z0-9_.\/-]+):([0-9]+):([0-9]+))\n/i)
      if(match) {
        const path = match[2]
        const line = match[3]
        const column = match[4]

        const pathParts = path.split('/')

        const databaseName = pathParts[0]
        const objectDir = pathParts[1]
        if(objectDir == 'query.js') {
          console.error("error in query to database", databaseName)
          return
        }
        const database = this.databases.get(databaseName)
        if(objectDir != 'indexes') {
          console.error(`unknown object dir ${objectDir}, something is wrong, exiting...`)
          process.exit(1)
        }
        const indexName = pathParts[2]
        if(!database) {
          if(this.initializingDatabase.name != databaseName) {
            console.error('error in non existing database?!', databaseName)
            process.exit(1)
          }
          console.error('error when initializing database', databaseName)
          this.initializingDatabase.handleUnhandledRejectionInIndex(indexName, reason, promise)
        } else {
          database.handleUnhandledRejectionInIndex(indexName, reason, promise)
        }
      } else break;
    }
  }
}

module.exports = Server
