const test = require('tape')
const ReactiveDao = require('@live-change/dao')

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
  { type: 'mul', value: 10 },
  { type: 'div', value: 3 },
  { type: 'email', value: 'spam' }
]

function delay(ms) {
  return new Promise((resolve, reject) => setTimeout(resolve, ms))
}

test("query observable", t => {
  t.plan(7)

  let server, client

  t.test('connect to server and create database', async t => {
    t.plan(1)
    server = await require('./getServer.js')
    client = await server.connect(1)
    await client.request(['database', 'createDatabase'], 'query-observable.test')
    t.pass('opened')
  })


  t.test("create tables and indexes", async t => {
    t.plan(1)
    await client.request(['database', 'createTable'], 'query-observable.test', 'users')
    await client.request(['database', 'createTable'], 'query-observable.test', 'messages')
    await client.request(['database', 'createLog'], 'query-observable.test', 'events')
    await client.request(['database', 'createIndex'], 'query-observable.test', 'userByName', '('+(
        async function(input, output) {
          const nameMapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
          await input.table('users').onChange((obj, oldObj) =>
              output.change(obj && nameMapper(obj), oldObj && nameMapper(oldObj)) )
        }
    ).toString()+')')
    await client.request(['database', 'createIndex'], 'query-observable.test', 'messagesByUser', '('+(
        async function(input, output) {
          const authorMapper = (obj) => ({ id: obj.author+'_'+obj.id, to: obj.id })
          await input.table('messages').onChange((obj, oldObj) =>
              output.change(obj && authorMapper(obj), oldObj && authorMapper(oldObj)) )
        }
    ).toString()+')')
    t.pass('tables and indexes created')
  })

  t.test("insert data", async t => {
    t.plan(1)
    for(let user of users) await client.request(['database', 'put'], 'query-observable.test', 'users', user)
    for(let message of messages) await client.request(['database', 'put'], 'query-observable.test', 'messages', message)
    for(let event of events) await client.request(['database', 'putLog'], 'query-observable.test', 'events', event)
    t.pass("data inserted to database")
  })

  let nextValueResolve
  let gotNextValue
  const getNextValue = () => {
    if(gotNextValue) {
      gotNextValue = false
      return queryObservable.list
    }
    return new Promise((resolve, reject) => nextValueResolve = resolve)
  }
  let queryObservable
  const queryObserver = (signal, value, ...rest) => {
    console.log("SIGNAL", signal, value, ...rest)
    if(nextValueResolve) {
      nextValueResolve(queryObservable.list)
    } else {
      gotNextValue = true
    }
  }


  t.test("query users", async t => {
    t.plan(4)
    queryObservable = client.observable(['database', 'query', 'query-observable.test', '('+(
        async (input, output) => await input.table('users').onChange((obj, oldObj) => output.change(obj, oldObj) )
    ).toString()+')'], ReactiveDao.ObservableList)
    queryObservable.observe(queryObserver)
    const results = await getNextValue()
    t.deepEqual(results, users, 'query result')

    const newUser = { id: '6', name: 'arnold' }
    users.push(newUser)
    client.request(['database', 'put'], 'query-observable.test', 'users', newUser)
    await delay(100)
    let updated = await getNextValue()
    t.deepEqual(updated, users)

    users = users.filter(u => u.id != "3")
    client.request(['database', 'delete'], 'query-observable.test', 'users', "3")
    await delay(100)
    updated = await getNextValue()
    t.deepEqual(updated, users)

    queryObservable.unobserve(queryObserver)
    t.pass('unobserved')
  })

  const idSort = (a,b) => a.id > b.id ? 1 : (a.id < b.id ? -1 : 0)

  t.test("query for users by name", async t => {
    t.plan(5)
    gotNextValue = false
    const mapper = (obj) => ({ id: obj.name+'_'+obj.id, to: obj.id })
    queryObservable = client.observable(['database', 'query', 'query-observable.test', `(${
        async (input, output) =>
            await input.table('users').onChange((obj, oldObj) =>
                output.change(
                    obj && { id: obj.name + '_' + obj.id, to: obj.id },
                    oldObj && { id: oldObj.name + '_' + oldObj.id, to: oldObj.id }))
    })`], ReactiveDao.ObservableList)
    queryObservable.observe(queryObserver)
    const results = await getNextValue()
    t.deepEqual(results, users.map(mapper).sort(idSort), 'query result')

    const newUser = { id: '3', name: 'jack' }
    users.push(newUser)
    users.sort(idSort)
    client.request(['database', 'put'], 'query-observable.test', 'users', newUser)
    await delay(100)
    let updated = await getNextValue()
    t.deepEqual(updated, users.map(mapper).sort(idSort))

    users = users.filter(u => u.id != "4")
    client.request(['database', 'delete'], 'query-observable.test', 'users', "4")
    await delay(100)
    updated = await getNextValue()
    t.deepEqual(updated, users.map(mapper).sort(idSort))

    const updatedUser = users.find(u => u.id == "6")
    updatedUser.name = "henry"
    client.request(['database', 'put'], 'query-observable.test', 'users', updatedUser)
    await delay(100)
    await getNextValue()
    updated = await getNextValue()
    t.deepEqual(updated, users.map(mapper).sort(idSort), 'user updated')

    queryObservable.unobserve(queryObserver)
    t.pass('unobserved')
  })

  t.test("query messages with users", async t => {
    t.plan(5)
    gotNextValue = false
    queryObservable = client.observable(['database', 'query', 'query-observable.test', `(${
        async (input, output) => {
          await input.table('messages').onChange((obj, oldObj) => {
            return output.synchronized(obj ? obj.id : oldObj.id, async () => {
              const user = obj && await input.table('users').object(obj.author).get()
              output.change(obj && { user, ...obj }, oldObj)
            })
          })
          await input.table('users').onChange((obj, oldObj) => {
            const userId = obj ? obj.id : oldObj.id
            return output.synchronized('u_'+userId, async () => {
              const messageIds = await (await input.index('messagesByUser'))
                  .range({ gte: userId, lt: userId + '\xFF' }).get()
              return Promise.all(messageIds.map(async mid => {
                const message = await input.table('messages').object(mid.to).get()
                if(message) await output.synchronized(message.id, async () => {
                  output.change({ user: obj, ...message }, { user: oldObj, ...message })
                })
              }))
            })
          })
        }})`], ReactiveDao.ObservableList)

    const jsResult = () => messages.map(msg => ({ user: users.find( u => u.id == msg.author ) || null, ...msg }))

    queryObservable.observe(queryObserver)
    let results = await getNextValue()
    t.deepEqual(results, jsResult())

    const newMessage = { id: '6', author: '1', text: "test" }
    messages.push(newMessage)
    client.request(['database', 'put'], 'query-observable.test', 'messages', newMessage)
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())

    const newUser = { id: '4', name: 'james' }
    users.push(newUser)
    users.sort(idSort)
    client.request(['database', 'put'], 'query-observable.test', 'users', newUser)
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())

    messages = messages.filter(m => m.id != '3')
    client.request(['database', 'delete'], 'query-observable.test', 'messages', "3")
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())

    users = users.filter(u => u.id != "2")
    client.request(['database', 'delete'], 'query-observable.test', 'users', "2")
    await delay(100)
    results = await getNextValue()
    t.deepEqual(results, jsResult())
  })

  t.test("query events", async t => {
    t.plan(3)
    queryObservable = client.observable(['database', 'query', 'query-observable.test', '('+(
        async (input, output) => await input.log('events').onChange((obj, oldObj) => output.change(obj, oldObj) )
    ).toString()+')'], ReactiveDao.ObservableList)

    queryObservable.observe(queryObserver)
    const results = await getNextValue()
    t.deepEqual(results.map(r=>({ type: r.type, value: r.value })), events, 'query result')

    const newEvent = { type:"post", value:"lol" }
    events.push(newEvent)
    client.request(['database', 'putLog'], 'query-observable.test', 'events', newEvent)
    await delay(100)
    let updated = await getNextValue()
    t.deepEqual(updated.map(r=>({ type: r.type, value: r.value })), events)

    queryObservable.unobserve(queryObserver)
    t.pass('unobserved')
  })
})