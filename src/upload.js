const FormData = require('form-data')
const path = require('path')
const fs = require('fs-extra')
const util = require('util')

function displayBytes(aSize) {
  aSize = Math.abs(parseInt(aSize, 10))
  if (aSize === 0) return '0 octets'
  const def = [[1, 'octets'], [1000, 'ko'], [1000 * 1000, 'Mo'], [1000 * 1000 * 1000, 'Go'], [1000 * 1000 * 1000 * 1000, 'To'], [1000 * 1000 * 1000 * 1000 * 1000, 'Po']]
  for (let i = 0; i < def.length; i++) {
    if (aSize < def[i][0]) return (aSize / def[i - 1][0]).toLocaleString() + ' ' + def[i - 1][1]
  }
}

module.exports = async (processingConfig, tmpDir, axios, log, patchConfig) => {
  let type
  if (processingConfig.typeFile === 'modification') type = 'RCS-B'
  else if (processingConfig.typeFile === 'compte') type = 'BILAN'
  else if (processingConfig.typeFile === 'prevention') type = 'PCL'
  else type = 'RCS-A'

  const datasetSchemaGlobal = require('./schema_global.json')
  const datasetSchemaType = require(`./schema_${type}.json`)
  const datasetSchema = datasetSchemaGlobal.concat(datasetSchemaType)

  // console.log(datasetSchema)
  const formData = new FormData()
  if (processingConfig.datasetMode === 'update') {
    await log.step('Mise à jour du jeu de données')
  } else {
    formData.append('schema', JSON.stringify(datasetSchema))
    formData.append('title', processingConfig.dataset.title)
    await log.step('Création du jeu de données')
  }

  const filePath = path.join(tmpDir, `${processingConfig.typeFile.toUpperCase()}.csv`)
  formData.append('dataset', fs.createReadStream(filePath), { filename: `${processingConfig.typeFile.toUpperCase()}.csv` })
  formData.getLength = util.promisify(formData.getLength)
  const contentLength = await formData.getLength()
  await log.info(`chargement de ${displayBytes(contentLength)}`)

  const dataset = (await axios({
    method: 'post',
    url: (processingConfig.dataset && processingConfig.dataset.id) ? `api/v1/datasets/${processingConfig.dataset.id}` : 'api/v1/datasets',
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })).data

  if (processingConfig.datasetMode === 'update') {
    await log.info(`jeu de données mis à jour, id="${dataset.id}", title="${dataset.title}"`)
  } else {
    await log.info(`jeu de données créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  }
}
