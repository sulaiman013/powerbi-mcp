// The one module that knows where the data comes from.
//
//   custom visual   app.rows, built from the fields on the data roles
//   measure page    app.data.ITEMS, a payload measure: "name|value;name|value", names
//                   percent-encoded (see generate_payload_measure)

function readItems(app) {
  if (app.rows) {
    return app.rows.map(function (row) {
      return { name: row.text.category, value: row.values.value || 0, row: row.index };
    });
  }
  return String(app.data.ITEMS || "").split(";").filter(Boolean).map(function (record, index) {
    var fields = record.split("|");
    return { name: decodeURIComponent(fields[0]), value: Number(fields[1]) || 0, row: index };
  });
}
