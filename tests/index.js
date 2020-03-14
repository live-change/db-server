const test = require('tape')

let users = [
  { id: '1', name: 'david' },
  { id: '2', name: 'thomas' },
  { id: '3', name: 'george' },
  { id: '4', name: 'donald' },
  { id: '5', name: 'david' }
]
let messages = [
  { id: '1', author: '1', text: "Hello!" },
  { id: '2', author: '2', text: "Hi!" },
  { id: '3', author: '1', text: "Bla bla bla" },
  { id: '4', author: '3', text: "IO XAOS" },
  { id: '5', author: '4', text: "Bye" }
]
let events = [
  { type: 'add', value: 1 },
  { type: 'sub', value: 2 },
  { type: 'add', value: 10 },
  { type: 'div', value: 3 },
  { type: 'add', value: 30 }
]

function delay(ms) {
  return new Promise((resolve, reject) => setTimeout(resolve, ms))
}

test("index", t => {
  t.plan(7)

  let server, client

  t.test('connect to server and create database', async t => {
    t.plan(1)
    server = await require('./getServer.js')
    client = await server.connect(1)
    await client.request(['database', 'createDatabase'], 'index.test')
    t.pass('opened')
  })

  t.test("create tables", async t => {
    t.plan(1)
    await client.request(['database', 'createTable'], 'index.test', 'users')
    await client.request(['database', 'createTable'], 'index.test', 'messages')
    await client.request(['database', 'createLog'], 'index.test', 'events')
    t.pass('tables created')
  })

  t.test("insert data", async t => {
    t.plan(1)
    for(let user of users) await client.request(['database', 'put'], 'index.test', 'users', user)
    for(let message of messages) await client.request(['database', 'put'], 'index.test', 'messages', message)
    for(let event of events) await client.request(['database', 'putLog'], 'index.test', 'events', event)
    t.pass("data inserted to database")
  })

  const idSort = (a,b) => a.id > b.id ? 1 : (a.id < b.id ? -1 : 0)

  t.test("create users by name index", async t => {
    t.plan(4)
    const mapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
    await client.request(['database', 'createIndex'], 'index.test', 'userByName', `(${
        async function(input, output) {
          const nameMapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
          await input.table('users').onChange((obj, oldObj) =>
              output.change(obj && nameMapper(obj), oldObj && nameMapper(oldObj)) )
        }
    })`)
    await delay(100)
    let results = await client.get(['database', 'indexRange', 'index.test', 'userByName', {}])
    t.deepEqual(results, users.map(mapper).sort(idSort), 'query result')

    const updatedUser = users.find(u => u.id == "3")
    updatedUser.name = "jack"
    await client.request(['database', 'put'], 'index.test', 'users', updatedUser)
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'userByName', {}])
    t.deepEqual(results, users.map(mapper).sort(idSort))

    users = users.filter(u => u.id != "4")
    await client.request(['database', 'delete'], 'index.test', 'users', "4")
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'userByName', {}])
    t.deepEqual(results, users.map(mapper).sort(idSort))

    const newUser = { id: '7', name: 'henry' }
    users.push(newUser)
    await client.request(['database', 'put'], 'index.test', 'users', newUser)
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'userByName', {}])
    t.deepEqual(results, users.map(mapper).sort(idSort))
  })

  t.test("create messages by user index", async t => {
    t.plan(1)
    await client.request(['database', 'createIndex'], 'index.test', 'messagesByUser', '('+(
        async function(input, output) {
          const authorMapper = (obj) => ({ id: obj.author+'_'+obj.id, to: obj.id })
          await input.table('messages').onChange((obj, oldObj) =>
              output.change(obj && authorMapper(obj), oldObj && authorMapper(oldObj)) )
        }
    ).toString()+')')

    console.log("INDEX CREATED")

    let results = await client.get(['database', 'indexRange', 'index.test', 'messagesByUser', {}])
    t.deepEqual(results, messages.map(m => ({ id: m.author+'_'+m.id, to: m.id })).sort(idSort))
  })

  t.test("create messages with users index", async t => {
    t.plan(5)
    await client.request(['database', 'createIndex'], 'index.test', 'messagesWithUsers', '('+(
        async function(input, output) {
          input.table('messages').onChange((obj, oldObj) => {
            return output.synchronized(obj ? obj.id : oldObj.id, async () => {
              const user = obj && await input.table('users').object(obj.author).get()
              output.change(obj && {user, ...obj}, oldObj)
            })
          }),
          input.table('users').onChange((obj, oldObj) => {
            const userId = obj ? obj.id : oldObj.id
            return output.synchronized('u_' + userId, async () => {
              const messageIds = await input.index('messagesByUser').range({gte: userId, lt: userId + '\xFF'}).get()
              return Promise.all(messageIds.map(async mid => {
                const message = await input.table('messages').object(mid.to).get()
                if (message) await output.synchronized(message.id, async () => {
                  output.change({user: obj, ...message}, {user: oldObj, ...message})
                })
              }))
            })
          })
        }
    ).toString()+')')

    console.log("INDEX CREATED")

    const jsResult = () => messages.map(msg => ({user: users.find(u => u.id == msg.author) || null, ...msg}))

    await delay(100)
    let results = await client.get(['database', 'indexRange', 'index.test', 'messagesWithUsers', {}])
    t.deepEqual(results, jsResult())

    const newMessage = { id: '6', author: '1', text: "test" }
    messages.push(newMessage)
    await client.request(['database', 'put'], 'index.test', 'messages', newMessage)
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'messagesWithUsers', {}])
    t.deepEqual(results, jsResult())

    const newUser = { id: '4', name: 'james' }
    users.push(newUser)
    users.sort(idSort)
    await client.request(['database', 'put'], 'index.test', 'users', newUser)
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'messagesWithUsers', {}])
    t.deepEqual(results, jsResult())

    messages = messages.filter(m => m.id != '3')
    await client.request(['database', 'delete'], 'index.test', 'messages', '3')
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'messagesWithUsers', {}])
    t.deepEqual(results, jsResult())

    users = users.filter(u => u.id != "2")
    await client.request(['database', 'delete'], 'index.test', 'users', '2')
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'messagesWithUsers', {}])
    t.deepEqual(results, jsResult())

  })

  t.test("create events index by type", async t => {
    t.plan(2)
    await client.request(['database', 'createIndex'], 'index.test', 'eventsByType', `(${
        async function(input, output) {
          const mapper = (obj) => ({ id: obj.type+'_'+obj.id, to: obj.id })
          await input.log('events').onChange((obj, oldObj) =>
              output.change(obj && mapper(obj), oldObj && mapper(oldObj)) )
        }
    })`)
    await delay(100)
    let results = await client.get(['database', 'indexRange', 'index.test', 'eventsByType', {}])
    t.equal(results.length, events.length, 'query result')

    const newEvent = { id: '7', name: 'henry' }
    events.push(newEvent)
    await client.request(['database', 'putLog'], 'index.test', 'events', newEvent)
    await delay(100)
    results = await client.get(['database', 'indexRange', 'index.test', 'eventsByType', {}])
    t.deepEqual(results.length, events.length)
  })

})