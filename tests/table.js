const test = require('tape')

let users = [
  { id: '1', name: 'david' },
  { id: '2', name: 'thomas' },
  { id: '3', name: 'george' },
  { id: '4', name: 'donald' },
  { id: '5', name: 'david' }
]
let messages = [
  { id: '1', author: 1, text: "Hello!" },
  { id: '2', author: 2, text: "Hi!" },
  { id: '3', author: 1, text: "Bla bla bla" },
  { id: '4', author: 3, text: "IO XAOS" },
  { id: '5', author: 4, text: "Bye" },
]

test("store range observable", t => {
  t.plan(3)

  let server, client

  t.test('connect to server and create database', async t => {
    t.plan(1)
    server = await require('./getServer.js')
    client = await server.connect(1)
    await client.request(['database', 'createDatabase'], 'table.test')
    t.pass('opened')
  })

  t.test("create tables", async t => {
    t.plan(1)
    await client.request(['database', 'createTable'], 'table.test', 'users')
    await client.request(['database', 'createTable'], 'table.test', 'messages')
    t.pass('tables created')
  })

  t.test("insert data", async t => {
    t.plan(1)
    for(let user of users) await client.request(['database', 'put'], 'table.test', 'users', user)
    for(let message of messages) await client.request(['database', 'put'], 'table.test', 'messages', message)
    t.pass("data inserted to database")
  })
})