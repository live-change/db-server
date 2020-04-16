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
      this.replicator.start()
    }
  }
  createDao(session) {
    const scriptContext = new ScriptContext({
      /// TODO: script available routines
    })
    if(this.config.master) {
      return new ReactiveDao(session, {
        remoteUrl: this.config.master,
        database: {
          type: 'local',
          source: new ReactiveDao.SimpleDao({
            methods: {
              ...dbDao.remoteRequests(this)
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
              ...dbDao.localRequests(this, scriptContext)
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
              ...storeDao.localRequests(this)
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
        databases: {
          system: {
            tables: {},
            indexes: {},
            logs: {}
          }
        }
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
    for(const db of this.databaseStores.values()) db.close()
  }
}

module.exports = Server
