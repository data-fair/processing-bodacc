const download = require('./src/download')
const processBodacc = require('./src/process')
const upload = require('./src/upload')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await download(pluginConfig, tmpDir, axios, log, [2008])
  await processBodacc(processingConfig, tmpDir, axios, log, patchConfig)
  if (!processingConfig.skipUpload) await upload(processingConfig, tmpDir, axios, log, patchConfig)
}
