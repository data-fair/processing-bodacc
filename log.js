// Very basic logging policy by overriding standard console methods

module.exports = function (name) {
  ['log', 'warn', 'error'].forEach(function (method) {
    const oldMethod = console[method].bind(console)
    console[method] = function () {
      let args = [...arguments]
      const level = method === 'log' ? 'info' : method

      // Prefix with log level
      const prefix = process.env.NODE_ENV === 'production' ? name + ' - ' + level + ' - ' : ''
      if (args[0] !== undefined && typeof args[0] === 'string') args[0] = prefix + args[0]
      else args = [prefix].concat(args)

      oldMethod.apply(console, args)
    }
  })
}

process.on('uncaughtException', e => {
  console.error('Uncaught exception', e.stack)
  process.exit(1)
})
