// How a visual announces itself to the host: an entry on powerbi.visuals.plugins under its
// guid, holding a factory the host calls with {element, host}. The same shape the official
// toolchain writes at the end of its bundle; the quoted placeholders are filled at build time.

var plugin = {
  name: "__VK_GUID__",
  displayName: "__VK_DISPLAY_NAME__",
  "class": "Visual",
  apiVersion: "__VK_API_VERSION__",
  create: function (options) { return new Visual(options); },
  createModalDialog: function () {},
  custom: true
};
var powerbi = window.powerbi;
if (typeof powerbi !== "undefined") {
  powerbi.visuals = powerbi.visuals || {};
  powerbi.visuals.plugins = powerbi.visuals.plugins || {};
  powerbi.visuals.plugins["__VK_GUID__"] = plugin;
}
