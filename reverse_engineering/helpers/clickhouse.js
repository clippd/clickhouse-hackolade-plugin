const internalDatabases = [
  "INFORMATION_SCHEMA",
  "information_schema",
  "system",
];

module.exports = {
  getDatabaseList: async (client) => {
    const response = await client.exec({
      query: "show databases",
    });
    const databaseList = await streamToList(response.stream);
    return databaseList.filter(
      (database) => database && internalDatabases.indexOf(database) === -1
    );
  },
  getTableList: async (client, databaseName) => {
    const response = await client.exec({
      query: `show tables in ${databaseName}`,
    });
    let tableList = await streamToList(response.stream);
    tableList = tableList.filter((table) => table);
    return tableList;
  },

  getRowCount: async (client, databaseName, tableName) => {
    const resultSet = await client.query({
      query: `SELECT COUNT(0) AS count FROM ${databaseName}.${tableName}`,
      format: "JSONEachRow",
    });
    const dataset = await resultSet.json();
    return dataset;
  },

  getRecords: async (client, databaseName, tableName, size = 10) => {
    const resultSet = await client.query({
      query: `SELECT * FROM ${databaseName}.${tableName} LIMIT ${size}`,
      format: "JSONEachRow",
    });
    const dataset = await resultSet.json();
    return dataset;
  },
};

const streamToList = async (stream) => {
  return await new Promise((resolve, reject) => {
    stream.on("readable", function () {
      const buffer = stream.read();
      resolve(buffer ? buffer.toString("utf8").split("\n") : []);
    });
    stream.on("error", (err) => reject(err));
  });
};
