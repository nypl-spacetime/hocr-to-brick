#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const H = require('highland')
const JSONStream = require('JSONStream')
const brickDb = require('to-brick')

const BASE_DIR = '/Users/bertspaan/data/city-directories/data/'
const BASE_URL = 'http://spacetime-nypl-org.s3.amazonaws.com/city-directories/'
const PNG_DIR = 'bin-pngs'

const ORGANIZATION_ID = 'nypl'
const DATA_DIR = path.join(__dirname, 'data')
const COLLECTIONS = require(path.join(DATA_DIR, 'collections.json'))

const TASKS = [
  {
    id: 'fix-ocr',
    submissionsNeeded: 3
  }
]

const tasks = TASKS
  .map((task) => ({
    id: task.id
  }))

const collections = COLLECTIONS
  .map((collection) => ({
    organization_id: ORGANIZATION_ID,
    tasks: TASKS,
    id: collection.uuid,
    url: collection.url,
    data: {
      dir: collection.dir,
      baseUrl: BASE_URL,
      pngDir: PNG_DIR
    }
  }))

function createBboxStream(collection) {
  var i = 0
  var stream = fs.createReadStream(collection.bboxes, 'utf8')
    .pipe(JSONStream.parse('*'))

  return H(stream)
    .map((page) => page.bboxes.map((bbox) => Object.assign(bbox, {
      page_num: page.page_num,
      file: page.file
    })))
    .flatten()
    .map((bbox) => {
      const item = {
        id: `${collection.uuid}.${bbox.id}`,
        collection_id: collection.uuid,
        organization_id: ORGANIZATION_ID,
        data: {
          bbox: bbox.bbox,
          text: bbox.text,
          page_num: bbox.page_num,
          file: bbox.file.replace(`./${PNG_DIR}/`, '')
        }
      }

      i += 1

      return item
    })
    .filter(() => i % 100 === 0)
}

H(COLLECTIONS)
  .map((collection) => Object.assign(collection, {
    bboxes: path.join(BASE_DIR, collection.dir, 'bboxes.json')
  }))
  .filter((collection) => fs.existsSync(collection.bboxes))
  .map((collection) => createBboxStream(collection))
  .flatten()
  .toArray((items) => {
    brickDb.addAll(tasks, collections, items, true)
  })
