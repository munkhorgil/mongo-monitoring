/**
 * Get the size of db, collection by given size (MB)
 * author: Munkh-Orgil Myagmarsuren
 * email: munkhorgil@live.com
 * created date: 2020-11-16
*/
const connection = new Mongo();
const dbNames = connection.getDBNames();
const data = [];
const sizeData = [];

const excludeDatabases = ['admin', 'test', 'local', 'config'];

// MB
const DB_SIZE = 5;
const COLLECTION_SIZE = 5;
const DOCUMENT_SIZE = 1;

const bytesToMegaBytes = bytes => bytes / (1024*1024);

const sort = (data, key) => {
  return data.sort((a, b) => {
    if (a[key] > b[key]) {
      return -1;
    } else if (a[key] < b[key]) {
      return 1;
    }

    return 0;
  });
}

function main() {
  for (let i = 0; i < dbNames.length; i++) {
    const dbName = dbNames[i];

    if (excludeDatabases.includes(dbName)) {
      continue;
    }

    const selectedDb = connection.getDB(dbName);

    // dataSize as MB
    const dbSize = selectedDb.stats(1024*1024).dataSize;

    if (dbSize < DB_SIZE) {
      continue;
    }

    const collectionNames = selectedDb.getCollectionNames();

    for (collectionName of collectionNames) {
      const collection = selectedDb.getCollection(`${collectionName}`);

      const collectionStat = collection.stats(1024*1024);

      if (collectionStat.storageSize < COLLECTION_SIZE) {
        continue;
      }

      data.push({
        ns: collectionStat.ns,
        count: collectionStat.count,
        storageSize: collectionStat.storageSize,
        index: {
          numberOfIndexes: collectionStat.nindexes,
          totalIndexSize: collectionStat.totalIndexSize,
        },
        cache: {
          inCache: collectionStat.wiredTiger.cache["bytes currently in the cache"],
          readCache: collectionStat.wiredTiger.cache["bytes read into cache"],
          writeCache: collectionStat.wiredTiger.cache["bytes written from cache"]
        }
      });


      collection.find().forEach(item => {
        const size = bytesToMegaBytes(Object.bsonsize(item))

        if (size > DOCUMENT_SIZE) {
          sizeData.push({
            [collectionName]: {
              id: item._id,
              size
            }
          });
        }
      })
    }
  }

  const filteredData = sort(data, 'storageSize');

  for (let j = 0; j < filteredData.length; j++) {
    print(JSON.stringify(filteredData[j]), '\n');
  }

  for (let u = 0; u < sizeData.length; u++) {
    print(JSON.stringify(sizeData[u]), '\n');
  }
}

main();
