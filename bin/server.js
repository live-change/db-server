#!/usr/bin/env node
const path = require('path')
const fs = require('fs')
const service = require ("os-service")
const SegfaultHandler = require('segfault-handler')
const Server = require('../lib/Server.js')
const { client: WSClient } = require("@live-change/dao-websocket")
const ReactiveDao = require('@live-change/dao')

process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
})

process.on('uncaughtException', function (err) {
  console.error(err.stack)
})

SegfaultHandler.registerHandler("crash.log");

function serverOptions(yargs) {
  yargs.option('port', {
    describe: 'port to bind on',
    type: 'number',
    default: 9417
  })
  yargs.option('host', {
    describe: 'bind host',
    type: 'string',
    default: '0.0.0.0'
  })
  yargs.option('master', {
    describe: 'replicate from master',
    type: 'string',
    default: null
  })
}

function storeOptions(yargs, defaults = {}) {
  yargs.option('dbRoot', {
    describe: 'server root directory',
    type: 'string',
    default: defaults.dbRoot || '.'
  })
  yargs.option('backend', {
    describe: 'database backend engine ( lmdb | leveldb | rocksdb | mem )',
    type: "string",
    default: defaults.backend || 'lmdb'
  })
}

function serviceOptions(yargs) {
  yargs.option('serviceName', {
    describe: 'name of service',
    type: 'string',
    default: 'liveChangeDb'
  })
}

function clientOptions(yargs) {
  yargs.option('serverUrl', {
    describe: 'database api url',
    type: 'string',
    default: 'http://localhost:9417/api/ws'
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
    .command('create', 'create database root', (yargs) => {
      storeOptions(yargs)
    }, (argv) => create(argv))
    .command('serve', 'start server', (yargs) => {
      serverOptions(yargs)
      storeOptions(yargs)
      yargs.option('service',{
        describe: 'run as service',
        type: 'boolean'
      })
    }, (argv) => {
      if(argv.service) {
        if(argv.verbose) {
          console.info('running as service!')
        }
        service.run (function () {
          service.stop (0);
        })
      }
      serve(argv)
    })
    .command('install', 'install system service', (yargs) => {
      serviceOptions(yargs)
      serverOptions(yargs)
      storeOptions(yargs, { dbRoot: '/var/db/liveChangeDb' })
    }, async (argv) => {
      const programArgs = ["serve", '--service',
        '--port', argv.port, '--host', argv.host,
        '--dbRoot', argv.dbRoot, '--backend', argv.backend]
      if(argv.verbose) console.info(`creating system service ${argv.serviceName}`)
      await fs.promises.mkdir(argv.dbRoot, {recursive:true})
      service.add (argv.serviceName, {programArgs}, function(error){
        if(error) console.trace(error);
      })
    })
    .command('uninstall', 'remove system service', (yargs) => {
      serviceOptions(yargs)
    }, (argv) => {
      if(argv.verbose) console.info(`removing system service ${argv.serviceName}`)
      service.remove (argv.serviceName, function(error){
        if(error) console.trace(error);
      })
    })
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
    .option('verbose', {
      alias: 'v',
      type: 'boolean',
      description: 'Run with verbose logging'
    }).argv

async function create({ dbRoot, backend, verbose }) {
  await fs.promises.mkdir(dbRoot, { recursive:true })
  if(verbose) console.info(`creating database in ${path.resolve(dbRoot)}`)
  let server = new Server({ dbRoot, backend })
  await server.initialize({ forceNew: true })
  if(verbose) console.info(`database server root directory created.`)
}

async function serve({ dbRoot, backend, verbose, host, port, master }) {
  if(verbose) console.info(`starting server in ${path.resolve(dbRoot)}`)
  let server = new Server({ dbRoot, backend, master })
  await server.initialize()
  if(verbose) console.info(`database initialized!`)
  if(verbose) console.info(`listening on: ${argv.host}:${argv.port}`)
  server.listen(port, host)
  if(verbose) console.info(`server started!`)
}

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


