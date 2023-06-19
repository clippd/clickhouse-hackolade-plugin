"use strict";

const { createClient } = require("@clickhouse/client");

let _client = null;

const internalDatabases = [
  "INFORMATION_SCHEMA",
  "information_schema",
  "system",
];

module.exports = {
  connect: function (connectionInfo, logger, cb) {
    logger.log(
      "info",
      connectionInfo,
      "Connection information",
      connectionInfo.hiddenKeys
    );
    if (_client !== null) {
      return cb(null, _client);
    }
    const endpoint = `${connectionInfo.protocol}://${connectionInfo.host}:${connectionInfo.port}`;
    const connectionObject = {
      host: endpoint,
      username: connectionInfo.username,
      password: connectionInfo.password,
    };
    _client = createClient(connectionObject);

    cb(null, _client);
  },

  disconnect: function (connectionInfo, logger, cb) {
    _client
      .close()
      .then((res) =>
        logger.log("info", res, "Clickhouse client disconnect success")
      )
      .catch((err) =>
        logger.log("error", err, "Clickhouse client disconnect failed")
      )
      .finally(() => cb());
  },

  testConnection: function (connectionInfo, logger, cb) {
    this.connect(connectionInfo, logger, async (error, client) => {
      try {
        if (!error) {
          const response = await client.ping();
          error = response ? undefined : "failed";
          logger.log("info", response, "Clickhouse server ping response");
        }
      } catch (err) {
        error = err;
        logger.log("error", err, "Clickhouse server ping request failed");
      } finally {
        this.disconnect(connectionInfo, logger, () => {});
        cb(error);
      }
    });
  },

  getDbCollectionsNames: function (connectionInfo, logger, cb) {
    this.connect(connectionInfo, logger, async (error, client) => {
      try {
        if (error) {
          throw error;
        }
        const databaseNames = await getDatabaseList(client);
        logger.log(
          "info",
          databaseNames,
          "List of Database names on the Clickhouse cluster"
        );

        const promiseList = databaseNames.map(async (databaseName) => {
          const response = await client.exec({
            query: `show tables in ${databaseName}`,
          });
          const dataString = await streamToString(response.stream);
          let tableList = dataString.split("\n");
          tableList = tableList.filter((table) => table);
          return {
            dbName: databaseName,
            dbCollections: tableList,
          };
        });

        const data = await Promise.all(promiseList);
        cb(null, data);
      } catch (error) {
        logger.log("error", error, "failed in getDbCollectionNames hook");
        cb(error);
      } finally {
        this.disconnect(connectionInfo, logger, () => {});
      }
    });
  },

  getDbCollectionsData: function (data, logger, cb, app) {
    this.connect(data, logger, async (error, client) => {
      try {
        let includeEmptyCollection = data.includeEmptyCollection;
        let { recordSamplingSettings, fieldInference, documentKinds } = data;
        const databaseNames = data.collectionData.dataBaseNames;
        const databases = data.collectionData.collections;

        const outputItems = [];
        for (const databaseName of databaseNames) {
          const tableNames = databases[databaseName];
          for (const tableName of tableNames) {
            logger.log(
              "info",
              databaseName + "-" + tableName,
              "Getting records"
            );
            const records = await getRecords(
              client,
              databaseName,
              tableName,
              10
            );
            logger.log("info", records.length, "Size of the records received");
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
              documentTemplate: records[0],
            };
            logger.log("info", outputItem, "OutpuItem");
            outputItems.push(outputItem);
          }
        }

        cb(null, outputItems);
      } catch (err) {
        logger.log("error", err, "Clickhouse server ping request failed");
        cb(err);
      } finally {
        this.disconnect(data, logger, () => {});
      }
    });
  },
};

const streamToString = async (stream) => {
  return await new Promise((resolve, reject) => {
    stream.on("readable", function () {
      const buffer = stream.read();
      resolve(buffer ? buffer.toString("utf8") : "");
    });
    stream.on("error", (err) => reject(err));
  });
};

const getDatabaseList = async (client) => {
  const res = await client.exec({
    query: "show databases",
  });
  const str = await streamToString(res.stream);
  const databaseList = str.split("\n");
  return databaseList.filter(
    (database) => database && internalDatabases.indexOf(database) === -1
  );
};

async function getRecords(client, databaseName, tableName, size) {
  const resultSet = await client.query({
    query: `SELECT * FROM ${databaseName}.${tableName} LIMIT ${size}`,
    format: "JSONEachRow",
  });
  const dataset = await resultSet.json();
  return dataset;
}

//   getDocumentKinds: function (connectionInfo, logger, cb, app) {
//     const _ = app.require("lodash");
//     this.connect(connectionInfo, logger, async (error, client) => {
//       try {
//         if (error) {
//           throw error;
//         }
//       } catch (error) {
//         logger.log("error", error, "failed when getting documentKinds");
//         cb(error);
//       } finally {
//         this.disconnect(connectionInfo, logger, () => {});
//       }
//     });
//   },
