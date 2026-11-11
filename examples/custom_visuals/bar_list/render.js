// Bar list: the whole visual is one function that returns markup.
// Each row carries data-row, which is all the kit needs to make it a data point.

function render(ctx) {
  var bars = ctx.settings.bars, labels = ctx.settings.labels;
  var column = ctx.columns.value[0];
  var units = labels.units === "auto" ? "auto" : undefined;
  var rows = ctx.rows.slice();
  var largest = 0;

  if (bars.order === "value") {
    rows.sort(function (a, b) { return (b.values.value || 0) - (a.values.value || 0); });
  }
  rows.forEach(function (row) { largest = Math.max(largest, Math.abs(row.values.value || 0)); });

  return '<div class="bl" style="font-size:' + labels.fontSize + 'pt">' + rows.map(function (row) {
    var value = row.values.value || 0;
    var length = largest ? Math.abs(value) / largest * 100 : 0;
    // With a selection elsewhere on the page, highlights hold the selected part of each value.
    var part = ctx.hasHighlights ? (row.highlights.value || 0) : value;
    var share = value ? Math.max(0, Math.min(1, part / value)) * 100 : 0;
    var shown = ctx.hasHighlights && row.highlights.value !== null ? part : value;
    return '<div class="bl-row" data-row="' + row.index + '">' +
      '<div class="bl-name">' + ctx.escape(row.text.category) + '</div>' +
      '<div class="bl-track" style="height:' + bars.height + 'px">' +
        '<div class="bl-bar" style="width:' + length.toFixed(2) + '%;background:' + bars.color + '40">' +
          '<div class="bl-fill" style="width:' + share.toFixed(2) + '%;background:' + bars.color + '"></div>' +
        '</div>' +
      '</div>' +
      (labels.show ? '<div class="bl-value">' + ctx.escape(ctx.format(shown, column, { units: units })) + '</div>' : '') +
    '</div>';
  }).join("") + '</div>';
}
