// KPI tiles: measures only, so there is a single row and no data point to select.
// The tiles are not rows, which makes this the place to show the explicit calls: the script
// raises its own tooltips with ctx.showTooltip and wires its own listener with ctx.on.

function render(ctx) {
  var tiles = ctx.settings.tiles;
  var cells = ctx.rows.length ? ctx.rows[0].cells.values : [];
  var units = tiles.units === "auto" ? "auto" : undefined;
  var columns = tiles.columns > 0 ? "repeat(" + Math.round(tiles.columns) + ", 1fr)"
                                  : "repeat(auto-fit, minmax(120px, 1fr))";

  function text(value, cell) { return ctx.format(value, cell.column, { units: units }); }

  // ctx.on keeps one listener per key and swaps the handler, so drawing again never stacks them.
  ctx.on(ctx.root, "mousemove", "tile-tooltip", function (event) {
    var node = event.target, cell;
    while (node && node !== ctx.root && !(node.hasAttribute && node.hasAttribute("data-tile"))) {
      node = node.parentNode;
    }
    cell = node && node !== ctx.root ? cells[Number(node.getAttribute("data-tile"))] : null;
    if (!cell) { ctx.hideTooltip(); return; }
    ctx.showTooltip([{ displayName: cell.column.displayName, value: cell.text }].concat(
      cell.highlight === null ? [] : [{ displayName: "Highlighted",
                                        value: ctx.format(cell.highlight, cell.column) }]), event);
  });

  return '<div class="kt" style="grid-template-columns:' + columns + '">' + cells.map(function (cell, index) {
    var color = tiles.vary ? ctx.color(cell.column.displayName) : tiles.color;
    var highlighted = ctx.hasHighlights && cell.highlight !== null;
    return '<div class="kt-tile" data-tile="' + index + '" tabindex="0" style="border-top-color:' + color + '">' +
      '<div class="kt-label">' + ctx.escape(cell.column.displayName) + '</div>' +
      '<div class="kt-value" style="font-size:' + tiles.fontSize + 'pt;color:' + color + '">' +
        ctx.escape(text(highlighted ? cell.highlight : cell.value, cell)) + '</div>' +
      (highlighted ? '<div class="kt-note">of ' + ctx.escape(text(cell.value, cell)) + '</div>' : '') +
    '</div>';
  }).join("") + '</div>';
}
