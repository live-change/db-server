const fs = require('fs')
const { once } = require('events')

function profileOutput(path) {
  const profileLogStream = fs.createWriteStream(path)

  let profileLogStreamDrainPromise = null

  async function write(operation) {
    const msg = {
      time: (new Date()).toISOString(),
      ...operation
    }
    if(!profileLogStream.write(JSON.stringify(msg) + '\n')) {
      if(!profileLogStreamDrainPromise) {
        profileLogStreamDrainPromise = once(profileLogStream, 'drain')
      }
      await profileLogStreamDrainPromise
      profileLogStreamDrainPromise = null
    }
  }

  return write
}

module.exports = profileOutput
