#!/usr/bin/env node
const path = require('path')
const fs = require('fs')
const service = require ("os-service")
const SegfaultHandler = require('segfault-handler')
const Server = require('../lib/Server.js')
const { client: WSClient } = require("@live-change/dao-websocket")
const ReactiveDao = require('@live-change/dao')
const db = require("@live-change/db")
const profileOutput = require("../lib/profileOutput.js")
const { performance } = require('perf_hooks')

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
  yargs.option('slowStart', {
    type: 'boolean',
    description: 'start indexes one after another(better for debugging)'
  })
  yargs.option('profileLog', {
    type: 'string',
    description: 'profiling log file path'
  })
}

function storeOptions(yargs, defaults = {}) {
  yargs.option('dbRoot', {
    describe: 'server root directory',
    type: 'string',
    default: defaults.dbRoot || '.'
  })
  yargs.option('backend', {
    describe: 'database backend engine ( lmdb | leveldb | rocksdb | memdown | mem )',
    type: "string",
    default: defaults.backend || 'lmdb'
  })
  yargs.option('backendUrl', {
    describe: 'remote database backend address',
    type: "string"
  })
}

function serviceOptions(yargs) {
  yargs.option('serviceName', {
    describe: 'name of service',
    type: 'string',
    default: 'liveChangeDb'
  })
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

async function serve(argv) {
  const { dbRoot, backend, backendUrl, verbose, host, port, master, slowStart, profileLog } = argv
  if(profileLog) {
    const out = profileOutput(profileLog)
    await db.profileLog.startLog(out, performance)
  }
  const profileOp = await db.profileLog.begin({ operation: "startingDbServer", ...argv })
  if(verbose) console.info(`starting server in ${path.resolve(dbRoot)}`)
  let server = new Server({
    dbRoot, backend, backendUrl, master,
    slowStart
  })

  process.on('unhandledRejection', (reason, promise) => {
    if(reason.stack && reason.stack.match(/\s(userCode:([a-z0-9_.\/-]+):([0-9]+):([0-9]+))\n/i)) {
      server.handleUnhandledRejectionInQuery(reason, promise)
    } else {
      console.error('Unhandled Promise Rejection', (reason && reason.stack) || reason, "Promise:", promise)
      //process.exit(1) // TODO: database should not fail because of it, but it should be logged somewhere
    }
  })

  await server.initialize()
  if(verbose) console.info(`database initialized!`)
  if(verbose) console.info(`listening on: ${argv.host}:${argv.port}`)
  server.listen(port, host)
  if(verbose) console.info(`server started!`)
  await db.profileLog.end(profileOp)
}
