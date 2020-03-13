#!/usr/bin/env node
const path = require('path')

const Server = require('../lib/Server.js')

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
}

function storeOptions(yargs) {
  yargs.option('dbRoot', {
    describe: 'server root directory',
    type: 'string',
    default: '.'
  })
  yargs.option('backend', {
    describe: 'database backend engine ( lmdb | leveldb | rocksdb | mem )',
    type: "string",
    default: 'lmdb'
  })
}

const argv = require('yargs') // eslint-disable-line
    .command('create', 'create database root', (yargs) => {
      storeOptions(yargs)
    }, (argv) => create(argv))
    .command('serve', 'start server', (yargs) => {
      serverOptions(yargs)
      storeOptions(yargs)
    }, (argv) => serve(argv))
    .option('verbose', {
      alias: 'v',
      type: 'boolean',
      description: 'Run with verbose logging'
    }).argv

async function create({ dbRoot, backend, verbose }) {
  if(verbose) console.info(`creating database in ${path.resolve(dbRoot)}`)
  let server = new Server({ dbRoot, backend })
  await server.initialize({ forceNew: true })
  if(verbose) console.info(`database server root directory created.`)
}

async function serve({ dbRoot, backend, verbose, host, port }) {
  if(verbose) console.info(`starting server in ${path.resolve(dbRoot)}`)
  let server = new Server({ dbRoot, backend })
  await server.initialize()
  if(verbose) console.info(`database initialized!`)
  if(verbose) console.info(`listening on: ${argv.host}:${argv.port}`)
  server.listen(port, host)
  if(verbose) console.info(`server started!`)
}