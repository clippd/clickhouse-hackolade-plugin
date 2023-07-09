"use strict";

const { createClient } = require("@clickhouse/client");
const async = require("async");

const {
  getDatabaseList,
  getTableList,
  getRecords,
} = require("./helpers/clickhouse");

const { getSchema } = require("./helpers");

let _client = null;

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
        logger.log("info", databaseNames, "List of databases on the cluster");

        const data = await async.map(databaseNames, async (databaseName) => {
          const tableList = await getTableList(client, databaseName);
          return {
            dbName: databaseName,
            dbCollections: tableList,
          };
        });
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

        let tables = [];

        databaseNames.forEach((databaseName) => {
          const tableList = databases[databaseName];
          tables = [
            ...tables,
            ...tableList.map((tableName) => ({
              tableName,
              databaseName,
            })),
          ];
        });

        const collectionItems = await async.mapSeries(tables, async (table) => {
          const { tableName, databaseName } = table;
          logger.log("info", databaseName + "-" + tableName, "Getting records");
          const records = await getRecords(client, databaseName, tableName, 10);
          logger.log("info", records.length, "Size of the records received");

          const schema = await getSchema(client, databaseName, tableName);

          return {
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
            validation: {
              jsonSchema: schema,
            },
            documentTemplate: records[0],
          };
        });

        cb(null, collectionItems);
      } catch (err) {
        logger.log("error", err, "Clickhouse server ping request failed");
        cb(err);
      } finally {
        this.disconnect(data, logger, () => {});
      }
    });
  },
};
