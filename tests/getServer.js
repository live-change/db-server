const rimraf = require("rimraf")
const fs = require('fs')
const Server = require('../lib/Server.js')
const ReactiveDao = require('@live-change/dao')

serverPromise = (async () => {
  rimraf.sync('test.db')
  await fs.promises.mkdir('test.db')
  const server = new Server({
    backend: process.env.DB_BACKEND || 'lmdb',
    dbRoot: 'test.db'
  })
  await server.initialize()
  server.connect = (sessionId, delay = 50) => new Promise((resolve, reject) => {
    const client = new ReactiveDao.LoopbackConnection(sessionId, server.apiServer, {
      onConnect: () => resolve(client),
      delay
    })
  })
  return server
})()

process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason);
  // application specific logging, throwing an error, or other logic here
})

module.exports = serverPromise
