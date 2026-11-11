// Drawing, the same on both routes. Each item carries data-row: on the measure page that is
// only an attribute; in the custom visual it makes the item a data point Power BI can select,
// cross-filter by, open a context menu for and show a tooltip on.

function main(app) {
  var items = readItems(app).sort(function (a, b) { return b.value - a.value; });
  var largest = items.length ? Math.abs(items[0].value) || 1 : 1;

  app.root.innerHTML = '<ol class="ra">' + items.map(function (item, place) {
    var picked = !app.select && app.state.picked === item.name ? " ra-picked" : "";
    return '<li class="ra-item' + picked + '" data-row="' + item.row + '" data-name="' + escapeHtml(item.name) + '">' +
      '<span class="ra-place">' + (place + 1) + '</span>' +
      '<span class="ra-name">' + escapeHtml(item.name) + '</span>' +
      '<span class="ra-meter"><i style="width:' + (Math.abs(item.value) / largest * 100).toFixed(1) + '%"></i></span>' +
      '<span class="ra-value">' + compact(item.value) + '</span>' +
    '</li>';
  }).join("") + '</ol>';

  // A measure page cannot hand a selection to Power BI, so it marks the picked item itself.
  // In the custom visual app.select exists and the kit does the real thing from data-row.
  if (!app.select) {
    app.on(app.root, "click", "pick", function (event) {
      var node = event.target;
      while (node && node !== app.root && !node.getAttribute("data-name")) { node = node.parentNode; }
      app.state.picked = node && node !== app.root && app.state.picked !== node.getAttribute("data-name")
        ? node.getAttribute("data-name") : null;
      app.schedule(main);
    });
  }
}
