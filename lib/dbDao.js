const ReactiveDao = require("@live-change/dao")

function localRequests(server, scriptContext) {
  return {
    createDatabase: async (dbName, options = {}) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      if(server.metadata.databases[dbName]) throw new Error("databaseAlreadyExists")
      server.metadata.databases[dbName] = options
      const database = await server.initDatabase(dbName, options)
      server.databases.set(dbName, database)
      server.databasesListObservable.push(dbName)
      await Promise.all([
        server.databases.get('system').createTable(dbName + "_tables"),
        server.databases.get('system').createTable(dbName + "_logs"),
        server.databases.get('system').createTable(dbName + "_indexes")
      ])
      await server.saveMetadata()
      return 'ok'
    },
    deleteDatabase: async (dbName) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      if(!server.metadata.databases[dbName]) throw new Error("databaseNotFound")
      delete server.metadata.databases[dbName]
      const database = server.databases.get(dbName)
      database.onAutoRemoveIndex = null
      server.databases.delete(dbName)
      const dbStore = server.databaseStores.get(dbName)
      server.databaseStores.delete(dbName)
      server.databasesListObservable.remove(dbName)
      await Promise.all([
        server.databases.get('system').deleteTable(dbName + "_tables"),
        server.databases.get('system').deleteTable(dbName + "_logs"),
        server.databases.get('system').deleteTable(dbName + "_indexes")
      ])
      await server.saveMetadata()
      await dbStore.delete()
      return 'ok'
    },
    clearDatabaseOpLogs: async (dbName, lastTimestamp, limit) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      return db.clearOpLogs(lastTimestamp || Date.now() - 60 * 1000, limit)
    },
    clearTableOpLog: async (dbName, tableName, lastTimestamp, limit) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      return table.clearOpLog(lastTimestamp || Date.now() - 60 * 1000, limit)
    },
    clearIndexOpLog: async (dbName, indexName, lastTimestamp, limit) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const index = db.table(indexName)
      if(!index) throw new Error("indexNotFound")
      return index.clearOpLog(lastTimestamp || Date.now() - 60 * 1000, limit)
    },
    createTable: async (dbName, tableName, options = {}) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = await db.createTable(tableName, options)
      await server.databases.get('system').table(dbName+'_tables').put({
        id: table.configObservable.value.uid,
        name: table.name,
        config: table.configObservable.value
      })
      return 'ok'
    },
    deleteTable: async (dbName, tableName, options) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      if(tableName[tableName.length - 1] == "*") {
        const list = db.tablesListObservable.list.slice()
        for(const foundTableName of list) {
          const base = tableName.slice(0, -1)
          if(foundTableName.slice(0, base.length) != base) continue
          const table = db.table(foundTableName)
          if(!table) throw new Error("tableNotFound")
          const uid = table.configObservable.value.uid
          await db.deleteTable(foundTableName)
          await server.databases.get('system').table(dbName + '_tables').delete(uid)
        }
      } else {
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        const uid = table.configObservable.value.uid
        await db.deleteTable(tableName)
        await server.databases.get('system').table(dbName + '_tables').delete(uid)
      }
      return 'ok'
    },
    renameTable: async (dbName, tableName, newTableName) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      const uid = table.configObservable.value.uid
      await server.databases.get('system').table(dbName+'_tables').update(uid,[
        { op: 'merge', property: 'name', value: newTableName }
      ])
      return db.renameTable(tableName, newTableName)
    },
    createIndex: async (dbName, indexName, code, params, options = {}) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const index = await db.createIndex(indexName, code, params, options)
      await server.databases.get('system').table(dbName+'_indexes').put({
        id: index.configObservable.value.uid,
        name: index.name,
        config: index.configObservable.value
      })
      return 'ok'
    },
    deleteIndex: async (dbName, indexName, options) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      if(indexName[indexName.length - 1] == "*") {
        const base = indexName.slice(0, -1)
        const list = db.indexesListObservable.list.slice()
        for(const foundIndexName of list) {
          if(foundIndexName.slice(0, base.length) != base) continue
          const index = await db.index(foundIndexName)
          if(!index) throw new Error("indexNotFound")
          const uid = index.configObservable.value.uid
          await db.deleteIndex(foundIndexName)
          await server.databases.get('system').table(dbName + '_indexes').delete(uid)
        }
      } else {
        const index = await db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        const uid = index.configObservable.value.uid
        await db.deleteIndex(indexName)
        await server.databases.get('system').table(dbName + '_indexes').delete(uid)
      }
      return 'ok'
    },
    renameIndex: async (dbName, indexName, newIndexName) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw await new Error('databaseNotFound')
      const index = await db.index(indexName)
      if(!index) throw new Error("indexNotFound")
      const uid = index.configObservable.value.uid
      await server.databases.get('system').table(dbName+'_indexes').update(uid,[
        { op: 'merge', property: 'name', value: newIndexName }
      ])
      return db.renameIndex(indexName, newIndexName)
    },
    createLog: async (dbName, logName, options = {}) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = await db.createLog(logName, options)
      await server.databases.get('system').table(dbName+'_logs').put({
        id: log.configObservable.value.uid,
        name: log.name,
        config: log.configObservable.value
      })
      return 'ok'
    },
    deleteLog: async (dbName, logName, options) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = db.log(logName)
      if(!log) throw new Error("logNotFound")
      const uid = log.configObservable.value.uid
      await db.deleteLog(logName)
      await server.databases.get('system').table(dbName+'_logs').delete(uid)
      return 'ok'
    },
    renameLog: async (dbName, logName, newLogName) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = db.log(logName)
      if(!log) throw new Error("logNotFound")
      const uid = log.configObservable.value.uid
      await server.databases.get('system').table(dbName+'_logs').update(uid,[
        { op: 'merge', property: 'name', value: newLogName }
      ])
      return db.renameLog(logName, newLogName)
    },
    put: (dbName, tableName, object) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      return table.put(object)
    },
    delete: (dbName, tableName, id) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      return table.delete(id)
    },
    update: (dbName, tableName, id, operations, options) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      //console.log("DAO UPDATE OPS", operations, "OPTIONS", options)
      return table.update(id, operations, options)
    },
    putLog: (dbName, logName, object) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = db.log(logName)
      if(!log) throw new Error("logNotFound")
      return log.put(object)
    },
    putOldLog: (dbName, logName, object) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = db.log(logName)
      if(!log) throw new Error("logNotFound")
      return log.putOld(object)
    },
    query: (dbName, code, params) => {
      if(dbName == 'system') throw new Error("system database is not writable")
      if(!dbName) throw new Error("databaseNameRequired")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const queryFunction = scriptContext.run(code, 'query')
      return db.queryUpdate((input, output) => queryFunction(input, output, params))
    }
  }
}

function remoteRequests(server) {
  return {
    createDatabase: async (dbName, options = {}) => {
      if(server.metadata.databases[dbName]) throw new Error("databaseAlreadyExists")
      return server.masterDao.request(['database', 'createDatabase'], dbName, options)
    },
    deleteDatabase: async (dbName) => {
      if(!server.metadata.databases[dbName]) throw new Error("databaseNotFound")
      return server.masterDao.request(['database', 'deleteDatabase'], dbName, options)
    },
    clearDatabaseOpLogs: async (dbName, lastTimestamp, limit) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      if(typeof lastTimestamp == 'string') lastTimestamp = new Date(lastTimestamp)
      return server.masterDao.request(['database', 'clearDatabaseOpLogs'], dbName, lastTimestamp, limit)
    },
    clearTableOpLog: async (dbName, tableName, lastTimestamp, limit) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      if(typeof lastTimestamp == 'string') lastTimestamp = new Date(lastTimestamp)
      return server.masterDao.request(['database', 'clearTableOpLog'], dbName, tableName, lastTimestamp, limit)
    },
    clearIndexOpLog: async (dbName, indexName, lastTimestamp, limit) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const index = db.table(indexName)
      if(!index) throw new Error("indexNotFound")
      if(typeof lastTimestamp == 'string') lastTimestamp = new Date(lastTimestamp)
      return server.masterDao.request(['database', 'clearIndexOpLog'], dbName, indexName, lastTimestamp, limit)
    },
    createTable: async (dbName, tableName, options = {}) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      return server.masterDao.request(['database', 'createTable'], dbName, tableName, options)
    },
    deleteTable: async (dbName, tableName, options) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      return server.masterDao.request(['database', 'deleteTable'], dbName, tableName, options)
    },
    renameTable: async (dbName, tableName, newTableName) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      return server.masterDao.request(['database', 'renameTable'], dbName, tableName, newTableName)
    },
    createIndex: async (dbName, indexName, code, params, options = {}) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      return server.masterDao.request(['database', 'createIndex'], dbName, indexName, code, params, options )
    },
    deleteIndex: async (dbName, indexName, options) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      return server.masterDao.request(['database', 'deleteIndex'], dbName, indexName, options )
    },
    renameIndex: async (dbName, indexName, newIndexName) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const index = db.index(indexName)
      if(!index) throw new Error("indexNotFound")
      return server.masterDao.request(['database', 'renameIndex'], dbName, indexName, newIndexName )
    },
    createLog: async (dbName, logName, options = {}) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      return server.masterDao.request(['database', 'createLog'], dbName, logName, options )
    },
    deleteLog: async (dbName, logName, options) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      await db.deleteLog(logName)
      return server.masterDao.request(['database', 'deleteLog'], dbName, logName, options )
    },
    renameLog: async (dbName, logName, newLogName) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = db.log(logName)
      if(!log) throw new Error("logNotFound")
      return server.masterDao.request(['database', 'renameLog'], dbName, logName, newLogName )
    },
    put: (dbName, tableName, object) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      return server.masterDao.request(['database', 'put'], dbName, tableName, object )
    },
    delete: (dbName, tableName, id) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      return server.masterDao.request(['database', 'delete'], dbName, tableName, id )
    },
    update: (dbName, tableName, id, operations, options) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const table = db.table(tableName)
      if(!table) throw new Error("tableNotFound")
      return server.masterDao.request(['database', 'update'], dbName, tableName, id, operations, options )
    },
    putLog: (dbName, logName, object) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = db.log(logName)
      if(!log) throw new Error("logNotFound")
      return server.masterDao.request(['database', 'putLog'], dbName, logName, object )
    },
    putOldLog: (dbName, logName, object) => {
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const log = db.log(logName)
      if(!log) throw new Error("logNotFound")
      return server.masterDao.request(['database', 'putOldLog'], dbName, logName, object )
    },
    query: (dbName, code, params) => {
      if(!dbName) throw new Error("databaseNameRequired")
      const db = server.databases.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      return server.masterDao.request(['database', 'query'], dbName, code, params )
    }
  }
}

function localReads(server, scriptContext) {
  return {
    databasesList: {
      observable: () => server.databasesListObservable,
      get: async () => server.databasesListObservable.list
    },
    databaseConfig: {
      observable: (dbName) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        return db.configObservable
      },
      get: async (dbName) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        return db.configObservable.value
      }
    },
    tablesList: {
      observable: (dbName, tableName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        return db.tablesListObservable
      },
      get: async (dbName, tableName, id) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        return db.tablesListObservable.list
      }
    },
    indexesList: {
      observable: (dbName, indexName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        return db.indexesListObservable
      },
      get: async (dbName, indexName, id) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        return db.indexesListObservable.list
      }
    },
    logsList: {
      observable: (dbName, logName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        return db.logsListObservable
      },
      get: async (dbName, logName, id) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        return db.logsListObservable.list
      }
    },
    tableConfig: {
      observable: (dbName, tableName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const table = db.table(tableName)
        if(!table) return new ReactiveDao.ObservableError('tableNotFound')
        return table.configObservable
      },
      get: async (dbName, tableName, id) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        return table.configObservable.value
      }
    },
    indexConfig: {
      observable: async (dbName, indexName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.configObservable
      },
      get: async (dbName, indexName, id) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.configObservable.value
      }
    },
    indexCode: {
      observable: async (dbName, indexName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.codeObservable
      },
      get: async (dbName, indexName, id) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.codeObservable.value
      }
    },
    logConfig: {
      observable: (dbName, logName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const log = db.log(logName)
        if(!log) return new ReactiveDao.ObservableError('logNotFound')
        return log.configObservable
      },
      get: async (dbName, logName, id) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const log = db.log(logName)
        if(!log) throw new Error("logNotFound")
        return log.configObservable.value
      }
    },
    tableObject: {
      observable: (dbName, tableName, id) => {
        if(!id) return new ReactiveDao.ObservableValue(null)
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const table = db.table(tableName)
        if(!table) return new ReactiveDao.ObservableError('tableNotFound')
        return table.objectObservable(id)
      },
      get: async (dbName, tableName, id) =>{
        if(!id) return null
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        return table.objectGet(id)
      }
    },
    tableRange: {
      observable: (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const table = db.table(tableName)
        if(!table) return new ReactiveDao.ObservableError('tableNotFound')
        return table.rangeObservable(range)
      },
      get: async (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        return table.rangeGet(range)
      }
    },
    tableCount: {
      observable: (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const table = db.table(tableName)
        if(!table) return new ReactiveDao.ObservableError('tableNotFound')
        return table.countObservable(range)
      },
      get: async (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        return table.countGet(range)
      }
    },
    tableOpLogObject: {
      observable: (dbName, tableName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const table = db.table(tableName)
        if(!table) return new ReactiveDao.ObservableError('tableNotFound')
        return table.opLog.objectObservable(id)
      },
      get: async (dbName, tableName, id) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        return table.opLog.objectGet(id)
      }
    },
    tableOpLogRange: {
      observable: (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const table = db.table(tableName)
        if(!table) return new ReactiveDao.ObservableError('tableNotFound')
        return table.opLog.rangeObservable(range)
      },
      get: async (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        return table.opLog.rangeGet(range)
      }
    },
    tableOpLogCount: {
      observable: (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const table = db.table(tableName)
        if(!table) return new ReactiveDao.ObservableError('tableNotFound')
        return table.opLog.countObservable(range)
      },
      get: async (dbName, tableName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const table = db.table(tableName)
        if(!table) throw new Error("tableNotFound")
        return table.opLog.countGet(range)
      }
    },
    indexObject: {
      observable: async (dbName, indexName, id) => {
        if(!id) return new ReactiveDao.ObservableError("id is required")
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.objectObservable(id)
      },
      get: async (dbName, indexName, id) => {
        if(!id) throw new Error("id is required")
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.objectGet(id)
      }
    },
    indexRange: {
      observable: async (dbName, indexName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.rangeObservable(range)
      },
      get: async (dbName, indexName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.rangeGet(range)
      }
    },
    indexCount: {
      observable: async (dbName, indexName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.countObservable(range)
      },
      get: async (dbName, indexName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = await db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.countGet(range)
      }
    },
    indexOpLogObject: {
      observable: (dbName, indexName, id) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.opLog.objectObservable(id)
      },
      get: async (dbName, indexName, id) =>{
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.opLog.objectGet(id)
      }
    },
    indexOpLogRange: {
      observable: (dbName, indexName, range) => {
        if(!id) return new ReactiveDao.ObservableError("id is required")
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.opLog.rangeObservable(range)
      },
      get: async (dbName, indexName, range) => {
        if(!id) throw new Error("id is required")
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.opLog.rangeGet(range)
      }
    },
    indexOpLogCount: {
      observable: (dbName, indexName, range) => {
        if(!id) return new ReactiveDao.ObservableError("id is required")
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const index = db.index(indexName)
        if(!index) return new ReactiveDao.ObservableError('indexNotFound')
        return index.opLog.countObservable(range)
      },
      get: async (dbName, indexName, range) => {
        if(!id) throw new Error("id is required")
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const index = db.index(indexName)
        if(!index) throw new Error("indexNotFound")
        return index.opLog.countGet(range)
      }
    },
    logObject: {
      observable: (dbName, logName, id) => {

        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const log = db.log(logName)
        if(!log) return new ReactiveDao.ObservableError('logNotFound')
        return log.objectObservable(id)
      },
      get: (dbName, logName, id) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const log = db.log(logName)
        if(!log) throw new Error("logNotFound")
        return log.objectGet(id)
      }
    },
    logRange: {
      observable: (dbName, logName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const log = db.log(logName)
        if(!log) return new ReactiveDao.ObservableError('logNotFound')
        return log.rangeObservable(range)
      },
      get: async (dbName, logName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const log = db.log(logName)
        if(!log) throw new Error("logNotFound")
        return log.rangeGet(range)
      }
    },
    logCount: {
      observable: (dbName, logName, range) => {
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const log = db.log(logName)
        if(!log) return new ReactiveDao.ObservableError('logNotFound')
        return log.countObservable(range)
      },
      get: async (dbName, logName, range) => {
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const log = db.log(logName)
        if(!log) throw new Error("logNotFound")
        return log.countGet(range)
      }
    },
    query: {
      observable: (dbName, code, params = {}) => {
        if(!dbName) return new ReactiveDao.ObservableError("databaseNameRequired")
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const queryFunction = scriptContext.run(code, 'userCode:query/query.js') /// TODO: log queries, more info here
        return db.queryObservable(async (input, output) => {
          return queryFunction(input, output, params)
        })
      },
      get: async (dbName, code, params = {}) => {
        if(!dbName) throw new Error("databaseNameRequired")
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const queryFunction = scriptContext.run(code, 'query')
        return db.queryGet((input, output) => queryFunction(input, output, params))
      }
    },
    queryObject: {
      observable: (dbName, code, params = {}) => {
        if(!dbName) return new ReactiveDao.ObservableError("databaseNameRequired")
        const db = server.databases.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const queryFunction = scriptContext.run(code, `userCode:${dbName}/query.js`) /// TODO: log queries, more info here
        return db.queryObjectObservable(async (input, output) => {
          return queryFunction(input, output, params)
        })
      },
      get: async (dbName, code, params = {}) => {
        if(!dbName) throw new Error("databaseNameRequired")
        const db = server.databases.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const queryFunction = scriptContext.run(code, `userCode:${dbName}/query.js`)
        return db.queryObjectGet((input, output) => queryFunction(input, output, params))
      }
    }
  }
}

module.exports = {
  localRequests,
  remoteRequests,
  localReads
}