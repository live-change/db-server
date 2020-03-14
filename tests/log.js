const test = require('tape')

const events = [
  { type: 'add', value: 1 },
  { type: 'sub', value: 2 },
  { type: 'mul', value: 10 },
  { type: 'div', value: 3 },
  { type: 'email', value: 'spam' }
]

test("store range observable", t => {
  t.plan(3)

  let server, client

  t.test('connect to server and create database', async t => {
    t.plan(1)
    server = await require('./getServer.js')
    client = await server.connect(1)
    await client.request(['database', 'createDatabase'], 'log.test')
    t.pass('opened')
  })

  t.test("create log", async t => {
    t.plan(1)
    await client.request(['database', 'createLog'], 'log.test', 'events')
    t.pass('log created')
  })

  t.test("insert data", async t => {
    t.plan(1)
    for(let event of events) await client.request(['database', 'putLog'], 'log.test', 'events', event)
    t.pass("data inserted to database")
  })

})
