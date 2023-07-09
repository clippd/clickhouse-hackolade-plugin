const MAX_DOCUMENTS = 10000;

const { getRowCount, getTableDdl } = require("./clickhouse");

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
  getSchema: async function (client, databaseName, tableName) {
    const ddl = await getTableDdl(client, databaseName, tableName);
    let arr = ddl.split("\\n");
    arr = arr.slice(arr.indexOf("(") + 1, arr.indexOf(")"));
    arr = arr.map((item) => {
      const temp = item
        .replaceAll(/`/g, "")
        .replace("    ", "")
        .replace(",", "")
        .split(" ");
      const attribute = { name: temp[0], subtype: temp[1] };
      return attribute;
    });

    const schema = { properties: {} };
    arr.forEach((attribute) => {
      const { name, subtype } = attribute;
      schema.properties[name] = {
        type: getType(subtype),
        mode: subtype,
      };
    });
    return schema;
  },
};

const getType = (subtype) => {
  switch (subtype) {
    case "String":
    case "FixedString":
      return "String";
    case "UInt8":
    case "UInt16":
    case "UInt32":
    case "UInt64":
    case "UInt128":
    case "UInt256":
    case "Int8":
    case "Int16":
    case "Int32":
    case "Int64":
    case "Int128":
    case "Int256":
    case "Float32":
    case "Float64":
      return "Numeric";
    case "Bool":
      return "Bool";
    case "DateTime":
    case "DateTime64":
    case "Date":
    case "Date32":
      return "Date";
  }
};
