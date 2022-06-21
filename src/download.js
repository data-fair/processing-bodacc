const fs = require('fs-extra')
const path = require('path')
const decompress = require('decompress')
const dayjs = require('dayjs')
const exec = require('child-process-promise').exec

module.exports = async (pluginConfig, tmpDir = 'data', axios, log, years) => {
  for (const year of years) {
    if (dayjs(year.toString()).diff('2016', 'year') > 0) {
      let firstUrl = null
      let urlLine = null
      if (year === dayjs().year()) {
        console.log('Telechargement 2022')
        firstUrl = 'https://echanges.dila.gouv.fr/OPENDATA/BODACC/' + year + '/'
        urlLine = await axios.get(firstUrl)
      } else if (year !== dayjs().year() && dayjs(year.toString()).diff('2017', 'year') >= 0) {
        console.log('Telechargement hstorique taz')
        firstUrl = 'https://echanges.dila.gouv.fr/OPENDATA/BODACC/FluxHistorique/' + year + '/'
        urlLine = await axios.get(firstUrl)
      }
      const linesOld = urlLine.data.split(/\r\n|\r|\n/g)
      while (linesOld.length) {
        const lines = linesOld.splice(0, 50)
        await getFiles(lines, tmpDir, firstUrl, '.taz', axios)
        await sleep(10000)
        extractFiles(tmpDir, '.taz')
      }
    }
    if (dayjs(year.toString()).diff('2008', 'year') >= 0) {
      console.log('Telechargement tar')
      const url = 'https://echanges.dila.gouv.fr/OPENDATA/BODACC/FluxHistorique/'
      getFilesOld(url, year, axios, tmpDir)
      await sleep(3 * 60000)
      console.log('Extraction')
      await exec(`tar -xvzf ${tmpDir + '/' + year.toString()} -C ${'BODACC_' + year + '.tar'}`)
      await exec(`cp -r ${tmpDir + '/' + year.toString()} ${tmpDir}`)
      await exec(`rm -f ${tmpDir + '/' + year.toString()}`)
    }
    extractFiles(tmpDir, '.taz')
  }
  extractFiles(tmpDir, '.taz')
}
/*
function extractFilesOld (tmpDir, filePath, year) {
  fs.readdir(tmpDir, (err, files) => {
    if (err) {
      console.log(err)
    } else {
      files.forEach(async file => {
        if (file.endsWith('.tar') === true) {
          try {
            await decompress(tmpDir + '/' + year + '/' + filePath, tmpDir).then(files => {
              console.log('done!', files)
            })
            await fs.remove(path.join(tmpDir, file))
          } catch (err) {
            console.error('Failure to extract', err)
          }
        }
      }
      )
    }
  })
}
*/
async function getFiles (lines, tmpDir, url, endName, axios) {
  await lines.forEach(async line => {
    if (line.match('href=".+?(?=.taz)') !== null) {
      const idFile = line.match('href=".+?(?=.taz)')[0].replace('href="', '') + endName
      const filePath = `${tmpDir}/${idFile}`
      console.log(filePath)
      const response = await axios({ url: url + idFile, method: 'GET', responseType: 'stream' })
      const writer = fs.createWriteStream(path.join(tmpDir, idFile))
      await response.data.pipe(writer)

      return new Promise((resolve, reject) => {
        writer.on('finish', resolve)
        writer.on('error', reject)
      })
    }
  })
}

function extractFiles (tmpDir, endName) {
  fs.readdir(tmpDir, (err, files) => {
    if (err) {
      console.log(err)
    } else {
      files.forEach(async file => {
        if (file.endsWith('.xml') === false) {
          const filePath = tmpDir + '/' + file
          const xmlFilePath = `${tmpDir}/${file.replace(endName, '')}.xml`
          console.log(`Extract ${file} -> ${xmlFilePath}`)
          try {
            await decompress(filePath, tmpDir).then(files => {
              console.log('done!', files)
            })
            await fs.remove(path.join(tmpDir, file))
          } catch (err) {
            console.error('Failure to extract', err)
          }
        }
      }
      )
    }
  })
}

function sleep (ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

async function getFilesOld (url, year, axios, tmpDir) {
  const idFile = 'BODACC_' + year + '.tar'
  const response = await axios({ url: url + idFile, method: 'GET', responseType: 'stream' })
  const writer = fs.createWriteStream(path.join(tmpDir, idFile))
  await response.data.pipe(writer)
  return new Promise((resolve, reject) => {
    writer.on('finish', resolve)
    writer.on('error', reject)
  })
}
