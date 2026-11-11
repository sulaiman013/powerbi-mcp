// Shared helpers. Nothing here knows where the page runs.

function escapeHtml(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function compact(value) {
  var size = Math.abs(value);
  if (size >= 1e6) { return (value / 1e6).toFixed(1) + "M"; }
  if (size >= 1e3) { return (value / 1e3).toFixed(1) + "K"; }
  return String(Math.round(value));
}
