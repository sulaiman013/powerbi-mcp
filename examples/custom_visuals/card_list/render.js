// Card list: markup only. The cards are data points because they carry data-row.

function render(ctx) {
  var cards = ctx.settings.cards, details = ctx.settings.details, heading = ctx.settings.heading.text;
  var headline = ctx.columns.value[0];
  var units = cards.units === "auto" ? "auto" : undefined;

  var items = ctx.rows.map(function (row) {
    var lines = !details.show ? "" : row.cells.details.map(function (cell) {
      return '<div class="cl-detail"><span>' + ctx.escape(cell.column.displayName) + '</span><b>' +
        ctx.escape(cell.text) + '</b></div>';
    }).join("");
    // Cross-highlighted from another visual: show the highlighted part against the total.
    var total = ctx.format(row.values.value, headline, { units: units });
    var figure = ctx.hasHighlights && row.highlights.value !== null
      ? ctx.escape(ctx.format(row.highlights.value, headline, { units: units })) +
        '<small> of ' + ctx.escape(total) + '</small>'
      : ctx.escape(total);
    return '<div class="cl-card" data-row="' + row.index + '">' +
      '<div class="cl-title">' + ctx.escape(row.text.category) + '</div>' +
      '<div class="cl-figure">' + figure + '</div>' +
      (lines ? '<div class="cl-details" style="font-size:' + details.fontSize + 'pt">' + lines + '</div>' : '') +
    '</div>';
  }).join("");

  return (heading ? '<div class="cl-heading">' + ctx.escape(heading) + '</div>' : '') +
    '<div class="cl-grid" style="--cl-accent:' + cards.accent + ';--cl-min:' + cards.minWidth + 'px">' + items + '</div>';
}
