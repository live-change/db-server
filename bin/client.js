#!/usr/bin/env node
const path = require('path')
const fs = require('fs')
const service = require ("os-service")
const SegfaultHandler = require('segfault-handler')
const Server = require('../lib/Server.js')
const { client: WSClient } = require("@live-change/dao-websocket")
const ReactiveDao = require('@live-change/dao')
const lineReader = require('line-reader')

process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
})

process.on('uncaughtException', function (err) {
  console.error(err.stack)
})

SegfaultHandler.registerHandler("crash.log");

function clientOptions(yargs) {
  yargs.option('serverUrl', {
    describe: 'database api url',
    type: 'string',
    default: 'http://localhost:9417/api/ws'
  })
}

function dumpOptions(yargs) {
  yargs.option('table', {
    describe: 'table name',
    type: 'string'
  })
  yargs.option('log', {
    describe: 'log name',
    type: 'string'
  })
  yargs.option('structure', {
    describe: 'dump structure',
    type: 'boolean'
  })
  yargs.option('metadata', {
    describe: 'dump only metadata',
    type: 'boolean'
  })
  yargs.option('targetDb', {
    describe: 'target database name'
  })
}

function parseList(str, sep = '.') {
  if(str[0] == '[' && str[str.length-1] == ']') {
    return eval(`(${str})`)
  } else {
    return str.split(sep).map(p => p.trim()).filter(p => !!p).map(v => {
      try {
        return eval(`(${v})`)
      } catch(err) {
        return v
      }
    })
  }
}

const argv = require('yargs') // eslint-disable-line
    .command('request <method> [args..]', 'request method on server', (yargs) => {
      clientOptions(yargs)
      yargs.positional('method', {
        describe: 'method to request',
        type: 'string'
      })
      yargs.positional('args', {
        describe: 'method arguments',
        type: 'string'
      })
      yargs.array('args')
    }, argv => {
      const method = parseList(argv.method)
      const args = argv.args.length == 1 ? parseList(argv.args[0]) : argv.args.map(v => {
        try {
          return eval(`(${v})`)
        } catch(err) {
          return v
        }
      })
      //console.dir({ method, args })
      request({ serverUrl: argv.serverUrl, method, args, verbose: argv.verbose })
    })
    .command('get <path>', 'gets value', (yargs) => {
      clientOptions(yargs)
      yargs.positional('path', {
        describe: 'value path',
        type: 'string'
      })
    }, argv => {
      const path = parseList(argv.path)
      //console.dir({ method, args })
      get({ serverUrl: argv.serverUrl, path, verbose: argv.verbose })
    })
    .command('observe <path>', 'observes value', (yargs) => {
      clientOptions(yargs)
      yargs.positional('path', {
        describe: 'value path',
        type: 'string'
      })
    }, argv => {
      const path = parseList(argv.path)
      //console.dir({ method, args })
      observe({ serverUrl: argv.serverUrl, path, verbose: argv.verbose })
    })
    .command('dump <db>', 'dump objects as requests json', (yargs) => {
      clientOptions(yargs)
      dumpOptions(yargs)
      yargs.positional('db', {
        describe: 'database name',
        type: 'string'
      })
    }, argv => {
      const db = argv.db
      dump({
        serverUrl: argv.serverUrl, verbose: argv.verbose, db: db, metadata: argv.metadata, structure: argv.structure,
        targetDb: argv.targetDb,
        tables: argv.table && (Array.isArray(argv.table) ? argv.table : [argv.table]),
        logs: argv.log && (Array.isArray(argv.log) ? argv.log : [argv.log])
      })
    })
    .command('exec [file]', 'exec commands from file', (yargs) => {
      clientOptions(yargs)
      yargs.positional('file', {
        describe: 'file to with commands to execute',
        default: '-'
      })
    }, argv => {
      exec({
        serverUrl: argv.serverUrl, verbose: argv.verbose, file: argv.file
      })
    })
    .option('verbose', {
      alias: 'v',
      type: 'boolean',
      description: 'Run with verbose logging'
    }).argv

async function request({ serverUrl, method, args, verbose }) {
  if(verbose) console.info(`requesting ${serverUrl} method ${JSON.stringify(method)} with arguments `
      +`${args.map(a=>JSON.stringify(a)).join(', ')}`)
  let done = false
  let promise
  const client = new WSClient("commandLine", serverUrl, {
    connectionSettings: {
      logLevel: 1
    },
    onConnect: () => {
      if(verbose) console.info("connected to server")
      if(promise) return;
      promise = client.request(method, ...args)
      promise.then(result => {
        if(verbose) console.log("command result:")
        console.log(JSON.stringify(result, null, "  "))
        done = true
        client.dispose()
      }).catch(error => {
        console.error(error)
        done = true
        client.dispose()
      })
    },
    onDisconnect: () => {
      if(verbose) console.info("disconnected from server")
      if(!done) {
        console.error("disconnected before request done")
        process.exit(1)
      } else {
        process.exit(0)
      }
    }
  })
}

async function get({ serverUrl, path, verbose }) {
  if(verbose) console.info(`getting ${serverUrl} value ${JSON.stringify(path)}`)
  let done = false
  let promise
  const client = new WSClient("commandLine", serverUrl, {
    connectionSettings: {
      logLevel: 1
    },
    onConnect: () => {
      if(verbose) console.info("connected to server")
      if(promise) return;
      promise = client.get(path)
      promise.then(result => {
        if(verbose) console.log("get result:")
        console.log(JSON.stringify(result, null, "  "))
        done = true
        client.dispose()
      }).catch(error => {
        console.error(error)
        done = true
        client.dispose()
      })
    },
    onDisconnect: () => {
      if(verbose) console.info("disconnected from server")
      if(!done) {
        console.error("disconnected before request done")
        process.exit(1)
      } else {
        process.exit(0)
      }
    }
  })
}

async function observe({ serverUrl, path, verbose }) {
  if(verbose) console.info(`observing ${serverUrl} value ${JSON.stringify(path)}`)
  let done = false
  const client = new WSClient("commandLine", serverUrl, {
    connectionSettings: {
      logLevel: 1
    },
    onConnect: () => {
      if(verbose) console.info("connected to server")
    },
    onDisconnect: () => {
      if(verbose) console.info("disconnected from server")
      if(!done) {
        console.error("disconnected before request done")
        process.exit(1)
      }
    }
  })
  const observable = client.observable(path, ReactiveDao.ObservableList)
  observable.observe((signal, ...args) => {
    done = true
    console.log(`signal: ${signal}`)
    for(const arg of args) console.log(JSON.stringify(arg, null, "  "))
  })
}

async function dump(options) {
  let { serverUrl, verbose, db, targetDb, tables, logs, metadata, structure } = options
  if(!targetDb) targetDb = db
 // console.log("options", options)
  const dumpAll = !tables && !logs

  let done = false

  const clientPromise = new Promise((resolve, reject) => {
    const client = new WSClient("commandLine", serverUrl, {
      connectionSettings: {
        logLevel: 1
      },
      onConnect: () => {
        if(verbose) console.error("connected to server")
        resolve(client)
      },
      onDisconnect: () => {
        if(verbose) console.error("disconnected from server")
        if(!done) {
          console.error("disconnected before request done")
          process.exit(1)
        } else {
          process.exit(0)
        }
      }
    })
  })

  const client = await clientPromise

  function req(method, ...args) {
    console.log(JSON.stringify({ type: 'request', method, parameters: args }))
  }
  function sync() {
    console.log(JSON.stringify({ type: 'sync' }))
  }

  let tablesList = tables || [], logsList = logs || []
  let databaseConfig
  if(structure || metadata) {
    databaseConfig = await client.get(['database', 'databaseConfig', db])
  }
  if(dumpAll && (structure || metadata)) {
       // console.log("CONFIG", databaseConfig)
    tablesList = Object.keys(databaseConfig.tables)
    logsList = Object.keys(databaseConfig.logs)
  } else if(dumpAll) {
    tablesList = await client.get(['database', 'tablesList', db])
    logsList = await client.get(['database', 'logsList', db])
  }
  if(structure || metadata) {
    if(dumpAll) {
      req(['database', 'createDatabase', targetDb,
        { ...databaseConfig, tables: undefined, logs: undefined, indexes: undefined}])
      sync()
    }
    for(let tableName of tablesList) {
      req(['database', 'createTable'], targetDb, tableName, databaseConfig.tables[tableName])
    }
    for(let logName of logsList) {
      req(['database', 'createLog'], targetDb, logName, databaseConfig.logs[logName])
    }
    if(dumpAll) {
      sync()
      for(let indexName in databaseConfig.indexes) {
        const conf = databaseConfig.indexes[indexName]
        req(['database', 'createIndex'], targetDb, indexName, conf.code, conf.parameters,
            { ...conf, code: undefined, parameters: undefined })
      }
    }
    sync()
  }

  async function stream(path, output) {
    const bucket = 256
    let found = 0
    let position = ''
    do {
      const results = await client.get(path(position, bucket))
      results.forEach(output)
      found = results.length
      if(results.length) position = results[results.length - 1].id
    } while(found == bucket)
  }

  if(!metadata) {
    for(let tableName of tablesList) {
      await stream(
          (from, limit) => ['database', 'tableRange', db, tableName, { gt: from, limit }],
          row => req(['database', 'put'], targetDb, tableName, row)
      )
    }
    for(let logName of logsList) {
      await stream(
          (from, limit) => ['database', 'logRange', db, logName, { gt: from, limit }],
          row => req(['database', 'putLog'], targetDb, logName, row)
      )
    }
  }

  done = true

  client.dispose()
}

async function exec(options) {
  let { serverUrl, verbose, file } = options

  let done = false

  const clientPromise = new Promise((resolve, reject) => {
    const client = new WSClient("commandLine", serverUrl, {
      connectionSettings: {
        logLevel: 1
      },
      onConnect: () => {
        if(verbose) console.error("connected to server")
        resolve(client)
      },
      onDisconnect: () => {
        if(verbose) console.error("disconnected from server")
        if(!done) {
          console.error("disconnected before request done")
          process.exit(1)
        } else {
          process.exit(0)
        }
      }
    })
  })

  lineReader.open(file, async function(err, reader) {
    if (err) throw err

    const client = await clientPromise
    let currentPromises = []
    const maxPromises = 256

    function nextLine() {
      return new Promise(function(resolve, reject) {
        reader.nextLine(function(err, line) {
          if (err) return reject(err)
          resolve(line)
        })
      })
    }
    while (reader.hasNextLine()) {
      const line = await nextLine()
      const command = JSON.parse(line)
      switch(command.type) {
        case 'request' :
          while(currentPromises.length > maxPromises) {
            await currentPromises[0]
            currentPromises.shift()
          }
          currentPromises.push(client.request(command.method, ...command.parameters))
          break;
        case 'sync' :
          await Promise.all(currentPromises)
          currentPromises = []
          break;
      }
    }
    reader.close(function(err) {
      if (err) throw err
    })

    await Promise.all(currentPromises)

    done = true

    client.dispose()

  })
}
