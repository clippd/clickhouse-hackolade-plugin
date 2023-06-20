const MAX_DOCUMENTS = 10000;

module.exports = {
  getSampleSize: async function (client, databaseName, tableName) {
    const count = await getRowCount(client, databaseName, tableName);
    const per = recordSamplingSettings.relative.value;
    const size =
      recordSamplingSettings.active === "absolute"
        ? recordSamplingSettings.absolute.value
        : Math.round((count / 100) * per);
    return size > MAX_DOCUMENTS ? MAX_DOCUMENTS : size;
  },
};
