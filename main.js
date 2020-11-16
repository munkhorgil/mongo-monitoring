/**
 * Get the size of db, collection by given size (MB)
 * author: Munkh-Orgil Myagmarsuren
 * email: munkhorgil@live.com
 * created date: 2020-11-16
*/

const dbNames = db.getMongo().getDBNames();
const connection = new Mongo();
const data = [];

const excludeDatabases = ['admin', 'test', 'local', 'config'];

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
  for (const dbName of dbNames) {
    if (excludeDatabases.includes(dbName)) {
      continue;
    }

    const selectedDb = connection.getDB(dbName);

    // dataSize as MB
    const dbSize = selectedDb.stats(1024*1024).dataSize;

    if (dbSiz < 50) {
      continue;
    }

    const collectionNames = selectedDb.getCollectionNames();

    for (collectionName of collectionNames) {
      const collection = selectedDb.getCollection(`${collectionName}`);

      const collectionStat = collection.stats(1024*1024);

      if (collectionStat.storageSize < 10) {
        continue;
      }

      data.push({
        ns: collectionStat.ns,
        count: collectionStat.count,
        storageSize: collectionStat.storageSize,
        index: {
          numberOfIndexes: collectionStat.nindexes,
          totalIndexSize: collectionStat.totalIndexSize,
          ...collectionStat.indexSizes
        },
        cache: {
          inCache: collectionStat.wiredTiger.cache["bytes currently in the cache"],
          readCache: collectionStat.wiredTiger.cache["bytes read into cache"],
          writeCache: collectionStat.wiredTiger.cache["bytes written from cache"]
        }
      });
    }
  }

  const filteredData = sort(data, 'storageSize');

  for (const item of filteredData) {
    print(JSON.stringify(item), '\n');
  }
}

main();
