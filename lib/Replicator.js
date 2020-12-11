const bucketSize = 100

const LOG_NOTSTARTED = 0
const LOG_UPDATING = 2
const LOG_READY = 3
const LOG_CLOSED = 4

class LogReplicator {
  constructor(databaseName, server, logName, destination) {
    this.server = server
    this.databaseName = databaseName
    this.logName = logName
    this.destination = destination

    this.dao = server.masterDao
    
    this.state = LOG_NOTSTARTED
    this.lastUpdateId = null

    this.observable = null
  }
  observeMore() {
    if(this.observable) this.observable.unobserve(this)
    this.observable = this.dao.observable(['database', 'logRange', this.databaseName, this.logName, {
      gt: this.lastUpdateId,
      limit: bucketSize
    }])
    this.observable.observe(this)
  }
  async set(entries) {
    for(let entry of entries) {
      await this.destination.data.put(entry)
      this.lastUpdateId = entry.id
    }
    if(this.observable.list.length == bucketSize) {
      this.observeMore()
    }
  }
  putByField(_fd, id, entry, _reverse, oldObject) {
    this.destination.data.put(entry)
    this.lastUpdateId = entry.id
    if(this.observable.list.length == bucketSize) {
      this.observeMore()
    }
  }
  async updateAll() {
    let entries
    do {
      entries = await this.dao.get(['database', 'logRange', this.databaseName, this.logName, {
        gt: this.lastUpdateId,
        limit: bucketSize
      }])
      for(let entry of entries) {
        await this.destination.data.put(entry)
        this.lastUpdateId = entry.id
      }
    } while(entries.length >= bucketSize && this.state != LOG_CLOSED)
    this.state = LOG_READY
    this.observeMore()
  }
  async start() {
    const lastLogOperations = await this.destination.rangeGet({ reverse: true, limit: 1 })
    const lastLogOperation = lastLogOperations[0]
    if(!lastLogOperation) {
      this.lastUpdateId = ''
    } else {
      this.lastUpdateId = lastLogOperation.id // one second overlap
    }
    this.state = LOG_UPDATING
    this.updateAll()
  }
  close() {
    this.state = LOG_CLOSED
    if(this.observable) this.observable.unobserve(this)
  }
}

const TABLE_NOTSTARTED = 0
const TABLE_COPYING = 1
const TABLE_UPDATING = 2
const TABLE_READY = 3
const TABLE_CLOSED = 4

class TableReplicator {
  constructor(databaseName, server, tableName, destination) {
    this.server = server
    this.databaseName = databaseName
    this.tableName = tableName
    this.destination = destination

    this.dao = server.masterDao

    this.state = TABLE_NOTSTARTED
    this.lastUpdateId = null

    this.observable = null
  }
  observeMore() {
    if(this.observable) this.observable.unobserve(this)
    this.observable = this.dao.observable(['database', 'tableOpLogRange', this.databaseName, this.tableName, {
      gt: this.lastUpdateId,
      limit: bucketSize
    }])
    this.observable.observe(this)
  }
  async set(ops) {
    for(let op of ops) {
      if(op.type == 'put') await this.destination.put(op.object)
      if(op.type == 'delete') await this.destination.delete(op.object)
      if(op.type == 'clearOpLog') await this.destination.clearOpLog(op.to)
      this.lastUpdateId = op.id
    }
    if(this.observable.list.length == bucketSize) {
      this.observeMore()
    }
  }
  async putByField(_fd, id, op, _reverse, oldObject) {
    if(op.type == 'put') this.destination.put(op.object)
    if(op.type == 'delete') this.destination.delete(op.object)
    if(op.type == 'clearOpLog') await this.destination.clearOpLog(op.to)
    this.lastUpdateId = op.id
    if(this.observable.list.length == bucketSize) {
      this.observeMore()
    }
  }
  async applyOp(op) {
    if(op.operation.type == 'put') await this.destination.data.put(op.operation.object)
    if(op.operation.type == 'delete') await this.destination.data.delete(op.operation.object)
    await this.destination.opLog.put(op)
    this.lastUpdateId = op.id
  }
  async updateAll() {
   // console.log("UPDATE TABLE", this.databaseName, this.tableName, "FROM", this.lastUpdateId)
    let ops
    do {
      ops = await this.dao.get(['database', 'tableOpLogRange', this.databaseName, this.tableName, {
        gt: this.lastUpdateId,
        limit: bucketSize
      }])
      //console.log("GOT TABLE OPS", this.databaseName, this.tableName, ":", JSON.stringify(ops, null, "  "))
      for(let op of ops) {
        await this.applyOp(op)
      }
    } while(ops.length >= bucketSize && this.state != TABLE_CLOSED)
    this.state = TABLE_READY
    this.observeMore()
  }
  async copyAll() {
    await Promise.all([
      this.destination.opLog.rangeDelete({}),
      this.destination.data.rangeDelete({}),
    ])
    //console.log("COPY TABLE", this.databaseName, this.tableName)
    let copyPosition = ''
    let data
    do {
      data = await this.dao.get(['database', 'tableRange', this.databaseName, this.tableName, {
        gt: copyPosition,
        limit: bucketSize
      }])
      for(let obj of data) {
        await this.destination.put(obj)
      }
    } while(data.length >= bucketSize && this.state != TABLE_CLOSED)
    this.state = TABLE_READY
    this.observeMore()
  }
  async start() {
    const lastTableOperation = await this.destination.opLog.rangeGet({ reverse: true, limit: 1 })[0]
    const firstSourceOperation = await this.dao.get(['database', 'tableOpLogRange', this.databaseName, this.tableName, {
      limit: 1
    }])
    if(!lastTableOperation // If empty table, or opLog desynchronized(cleared)
        || (firstSourceOperation && firstSourceOperation.id > lastTableOperation.id)) { // Copy data first
      let tableCreateTimestamp = Date.now()
      this.state = TABLE_COPYING
      this.lastUpdateId = (''+(tableCreateTimestamp - 1000)).padStart(16,'0') // one second overlay
      this.copyAll()
    } else {
      this.state = TABLE_UPDATING
      this.lastUpdateId = lastTableOperation.id // one second overlap
      this.updateAll()
    }
  }
  close() {
    this.state = TABLE_CLOSED
    if(this.observable) this.observable.unobserve(this)
  }
}

class ListReplicator {
  constructor(databaseName, server, list, type, copy, factory) {
    this.name = databaseName
    this.database = null
    this.server = server
    this.dao = server.masterDao
    this.list = list
    this.type = type
    this.copy = copy
    this.factory = factory
    this.observable = null
    this.objects = new Map()
  }
  start() {
    this.database = this.server.databases.get(this.name)
    /// Observe system table because it is organized by table uids which is better for rename operations
    this.observable = this.dao.observable(['database', 'tableRange', 'system', this.name+'_'+this.list, {}])
    this.observable.observe(this)
  }
  async createObject(obj) {
    let foundName = null
    const configList = this.database.config[this.list]
    for(let k in configList) if(configList[k].uid == obj.id) foundName = k
    if(foundName) {
      if(foundName != obj.name) {
        await this.database['rename' + this.type](foundName, obj.name)
        await this.server.databases.get('system').table(this.name+'_'+this.list).update(obj.id,[
          { op: 'merge', property: 'name', value: obj.name }
        ])
      }
      /// TODO: compare configurations?
    } else {
      if(configList[obj.name]) { // Object with this name already exists, overwrite?!
        console.error("NAME", obj.name, "EXISTS WITH ANOTHER UID",
            configList[obj.name].uid, "!=", obj.id ,"IN DB", this.name)
      }
      try {
        const object = await this.factory(obj)
        await this.server.databases.get('system').table(this.name + '_' + this.list).put(obj)
      } catch(e) {
        console.log("CREATE OBJ", obj, "IN DB", this.name, "FAILED")
        console.error(e)
      }
    }
    if(this.copy) {
      const objRep = this.objects.get(obj.id)
      if(objRep) {
        objRep.close()
        this.objects.delete(obj.id)
      }
      const objectReplicator = this.copy(obj)
      this.objects.set(obj.id, objectReplicator)
      objectReplicator.start()
    }
  }
  set(objects) {
    for(const obj of objects) {
      this.createObject(obj)
    }
    for(const key in this.database.config[this.list]) {
      const value = this.database.config[this.list][key]
      const obj = objects.find(o => o.id == value.uid)
      if(!obj) {
        this.database['delete' + this.type](key)
        this.server.databases.get('system').table(this.name+'_'+this.list).delete(value.uid)
      }
    }
  }
  putByField(_fd, uid, object, _reverse, oldObject) {
    this.createObject(obj)
  }
  removeByField(_fd, uid, oldObject) {
    this.database['delete' + this.type](oldObject.name)
    this.server.databases.get('system').table(this.name+'_'+this.list).delete(uid)
    if(this.copy) {
      this.objects.get(uid).close()
      this.objects.delete(uid)
    }
  }
}

class DatabaseReplicator {
  constructor(databaseName, server) {
    this.name = databaseName
    this.server = server
    this.dao = server.masterDao
    this.tables = null
    this.indexes = null
    this.logs = null
  }
  async start() {
    this.databaseConfig = this.dao.get(['database', 'databaseConfig', this.name])
    if(!this.server.metadata.databases[this.name]) {
      this.server.metadata.databases[this.name] = this.databaseConfig
      const database = await this.server.initDatabase(this.name, this.databaseConfig)
      this.server.databases.set(this.name, database)
      this.server.databasesListObservable.push(this.name)
      await Promise.all([
        this.server.databases.get('system').createTable(this.name + "_tables"),
        this.server.databases.get('system').createTable(this.name + "_logs"),
        this.server.databases.get('system').createTable(this.name + "_indexes")
      ])
      await this.server.saveMetadata()
    }
    const database = this.server.databases.get(this.name)

    this.tables = new ListReplicator(this.name, this.server, 'tables', 'Table',
        (obj) => new TableReplicator(this.name, this.server, obj.name, database.table(obj.name)),
            (obj) => database.createTable(obj.name, obj.config)
    )
    this.indexes = new ListReplicator(this.name, this.server, 'indexes', 'Index',
        false,
        (obj) => {
          //console.log("CREATE INDEX", obj.id, obj.name, "WITH UID", obj.config.uid)
          return database.createIndex(obj.name, obj.config.code, obj.config.parameters, obj.config)
        })
    this.logs = new ListReplicator(this.name, this.server, 'logs', 'Log',
        (obj) => new LogReplicator(this.name, this.server, obj.name, database.log(obj.name)),
            (obj) => database.createLog(obj.name, obj.config)
    )

    this.tables.start()
    this.indexes.start()
    this.logs.start()
  }
  close() {
    this.tables.close()
    this.indexes.close()
    this.logs.close()
  }
}

class Replicator { // synchronizes database list
  constructor(server) {
    this.server = server
    this.dao = server.masterDao
    this.listObservable = null
    this.databases = new Map()
  }
  start() {
    this.listObservable = this.dao.observable(['database', 'databasesList'])
    this.listObservable.observe(this)
  }
  close() {
    if(this.listObservable) this.listObservable.unobserve(this)
    for(let replicator of this.databases.values()) replicator.close()
  }
  set(dbList) {
    if(!Array.isArray(dbList)) return
    for(let [name, db] of this.server.databases.entries()) {
      if(name!='system' && dbList.indexOf(name) == -1) { // Database removed
        this.remove(name)
      }
    }
    for(let name of dbList) {
      this.push(name)
    }
  }
  push(name) {
    if(!this.databases.get(name)) {
      const replicator = new DatabaseReplicator(name, this.server)
      this.databases.set(name, replicator)
      replicator.start()
    }
  }
  async remove(dbName) {
    const replicator = this.databases.get(dbName)
    if(replicator) {
      replicator.close()
    }
    delete this.server.metadata.databases[dbName]
    this.server.databases.get(dbName).delete()
    this.server.databaseStores.get(dbName).delete()
    this.server.databasesListObservable.remove(dbName)
    await Promise.all([
      this.server.databases.get('system').deleteTable(dbName + "_tables"),
      this.server.databases.get('system').deleteTable(dbName + "_logs"),
      this.server.databases.get('system').deleteTable(dbName + "_indexes")
    ])
    await this.server.saveMetadata()
  }
}

module.exports = Replicator
