function localRequests(server) {
  return {
    put: (dbName, storeName, object) => {
      const db = server.databaseStores.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const store = db.stores.get(storeName)
      if(!store) throw new Error('storeNotFound')
      return store.put(object)
    },
    delete: (dbName, storeName, id) => {
      const db = server.databaseStores.get(dbName)
      if(!db) throw new Error('databaseNotFound')
      const store = db.stores.get(storeName)
      if(!store) throw new Error('storeNotFound')
      return store.delete(id)
    }
  }
}

function localReads(server) {
  return {
    object: {
      observable(dbName, storeName, id) {
        const db = server.databaseStores.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const store = db.stores.get(storeName)
        if(!store) return new ReactiveDao.ObservableError('storeNotFound')
        return store.objectObservable(id)
      },
      get: (dbName, storeName, id) => {
        const db = server.databaseStores.get(dbName)
        if(!db) throw new Error('databaseNotFound')
        const store = db.stores.get(storeName)
        if(!store) throw new Error('storeNotFound')
        return store.objectGet(id)
      }
    },
    range: {
      observable(dbName, storeName, range) {
        const db = server.databaseStores.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const store = db.stores.get(storeName)
        if(!store) return new ReactiveDao.ObservableError('storeNotFound')
        return storeName.rangeObservable(range)
      },
      get: async (dbName, storeName, range) => {
        const db = server.databaseStores.get(dbName)
        if(!db) return new ReactiveDao.ObservableError('databaseNotFound')
        const store = db.stores.get(storeName)
        if(!store) return new ReactiveDao.ObservableError('storeNotFound')
        return storeName.rangeGet(range)
      }
    }
  }
}

module.exports = {
  localRequests,
  localReads
}
