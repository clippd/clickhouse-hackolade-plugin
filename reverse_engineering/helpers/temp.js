const { createClient, ClickHouseClient } = require("@clickhouse/client");

const { testConnection } = require("../api");

const async = require("async");

// const arr = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

const client = createClient({});

async function getRecords(client, databaseName, tableName, size) {
  const resultSet = await client.query({
    query: `SELECT * FROM ${databaseName}.${tableName} LIMIT ${size}`,
    format: "JSONEachRow",
  });
  const dataset = await resultSet.json();
  return dataset;
}

async function temp(client, data) {
  try {
    let includeEmptyCollection = data.includeEmptyCollection;
    let { recordSamplingSettings, fieldInference, documentKinds } = data;
    const databaseNames = data.collectionData.dataBaseNames;
    const databases = data.collectionData.collections;

    const outputItems = [];
    for (const databaseName of databaseNames) {
      const tableNames = databases[databaseName];
      for (const tableName of tableNames) {
        const records = await getRecords(client, databaseName, tableName, 1);
        const outputItem = {
          dbName: databaseName,
          collectionName: tableName,
          documents: records,
          indexes: [],
          bucketIndexes: [],
          views: [],
          emptyBucket: false,
          bucketInfo: {},
          entityLevel: {
            constraint: [],
          },
          validation: {},
          fieldInference: {
            active: "field",
          },
          documentTemplate: records[0],
        };
        outputItems.push(outputItem);
      }
    }

    return outputItems;
  } catch (err) {
    console.log("error", err, "Clickhouse server ping request failed");
  } finally {
    client.close();
  }
}

temp(client, {
  collectionData: {
    collections: {
      clippd_dev_green: ["calculated_scores", "player_dashboard"],
    },
    dataBaseNames: ["clippd_dev_green"],
  },
}).then((res) => console.log(JSON.stringify(res)));

// const temp = async (num) => {
//   return new Promise((resolve) => {
//     setTimeout(() => {
//       resolve(num);
//     }, 2000);
//   });
// };

// async
//   .map(arr, temp)
//   .then((res) => console.log(res))
//   .catch((err) => console.log(err));

// async function temp() {
//   try {
//     const resultSet = await client.query({
//       query: "SELECT * FROM clippd_dev_green.shots_raw limit 1",
//       format: "JSONEachRow",
//     });
//     const dataset = await resultSet.json();
//     return dataset;
//   } catch (err) {
//     console.log(err);
//   } finally {
//     await client.close();
//   }
// // }

// // temp().then((res) =>
// //   // console.log(String(res.stream.read().toString("utf8")).split("\n"))
// //   console.log(res)
// // );

// const getDatabaseList = async (client) => {
//   const databases = [
//     "clippd_dev_green",
//     "clippd_dev_blue",
//     "test_db",
//     "default",
//     "clippd_dev_purple_v1",
//     "clippd_dev_purple",
//     "clippd_lookup",
//   ];
//   for (const database of databases) {
//     const res = await client.exec({
//       query: `show tables in ${database}`,
//     });
//     console.log(database);
//     const stream = res.stream;
//     const string = await streamToString(stream);
//     const tables = string.split("\n");
//     console.log(tables);
//   }
//   return 1;
// };

// // getDatabaseList(client)
// //   .then((res) => console.log(res))
// //   .finally(() => client.close());

// async function test() {
//   return new Promise((resolve) => {
//     setTimeout(() => {
//       // throw "error";
//       resolve("setTimeout");
//     }, 5000);
//   });
// }

// test()
//   .then(
//     (res) => {
//       console.log(res);
//     }
//     // (err) => console.log("second arg error")
//   )
//   .catch((err) => console.log("catch error"));
