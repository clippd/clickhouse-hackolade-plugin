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
    let columns = ddl.split("\\n");
    columns = columns.slice(columns.indexOf("(") + 1, columns.indexOf(")"));
    columns = columns.map((item) => {
      const temp = item
        .replaceAll(/`/g, "")
        .replace("    ", "")
        .replace(",", "")
        .split(" ");
      const column = { name: temp[0], subtype: temp[1] };
      return column;
    });

    const schema = { properties: {} };
    columns.forEach((column) => {
      const { name, subtype } = column;
      schema.properties[name] = getType(subtype);
      if (subtype.indexOf("Array") !== -1) {
        schema.properties[name] = getTypeForArray(subtype);
      }
    });
    return schema;
  },
};

const getType = (subtype) => {
  switch (subtype) {
    case "String":
    case "FixedString":
      return { type: "String", mode: subtype };
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
      return { type: "Numeric", mode: subtype };
    case "Bool":
      return { type: "Bool" };
    case "DateTime":
    case "DateTime64":
    case "Date":
    case "Date32":
      return { type: "Date", mode: subtype };
    default:
      return { type: subtype };
  }
  //TODO more types to be added
};

const getTypeForArray = (subtype) => {
  const childSubType = subtype.match(/\((.*?)\)/)[1];
  const childType = getType(childSubType).type;
  return { type: "Array", subtype: `Array<${childType}>` };
};
