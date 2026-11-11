// The visual the host talks to. It owns everything the host expects from a native-feeling
// visual (selection, highlights, context menu, tooltips, keyboard, format pane, rendering
// events, landing page) and calls the author's render(ctx) for the drawing.
//
// Zero wiring: any element the author's markup carries with data-row="<row index>" is
// selectable, opens the context menu, shows a tooltip, takes keyboard focus, and gets the
// classes vk-selected / vk-dimmed / vk-highlighted.

KIT.up = function (node, test, stop) {
  while (node && node !== stop) {
    if (node.nodeType === 1 && test(node)) { return node; }
    node = node.parentNode;
  }
  return null;
};

KIT.sameIdentity = function (a, b) {
  if (!a || !b) { return false; }
  if (a === b) { return true; }
  if (typeof a.equals === "function") { return a.equals(b); }
  return typeof a.getKey === "function" && typeof b.getKey === "function" && a.getKey() === b.getKey();
};

function Visual(options) {
  var self = this;
  this.host = options.host;
  this.element = options.element;
  this.config = KIT.config;
  this.features = KIT.config.features;
  this.listeners = [];
  this.tooltipRow = null;
  this.tooltipAuto = false;
  this.selectedIds = [];
  this.colors = this.readColors();
  this.settings = KIT.readSettings(this.config.format, null, this.colors);
  this.model = KIT.buildModel(null, this.config, this.host);

  // Everything the kit draws sits in one frame of its own: the author's stage (root), the
  // landing / empty / error notice, and the line that says rows were cut at the limit.
  this.frame = document.createElement("div");
  this.frame.className = "vk-frame";
  this.root = document.createElement("div");
  this.root.className = "vk-root vk-" + this.config.name;
  this.notice = document.createElement("div");
  this.notice.className = "vk-notice vk-hidden";
  this.limit = document.createElement("div");
  this.limit.className = "vk-limit vk-hidden";
  this.frame.appendChild(this.root);
  this.frame.appendChild(this.notice);
  this.frame.appendChild(this.limit);
  this.element.appendChild(this.frame);

  this.selectionManager = this.host.createSelectionManager ? this.host.createSelectionManager() : null;
  if (this.selectionManager && this.selectionManager.registerOnSelectCallback) {
    // The host calls back when it sets the selection itself: a bookmark being applied, or
    // another visual taking the selection over.
    this.selectionManager.registerOnSelectCallback(function (ids) { self.afterSelection(ids || []); });
  }

  this.listen(this.element, "click", function (event) { self.onClick(event); });
  this.listen(this.element, "contextmenu", function (event) { self.onContextMenu(event); });
  this.listen(this.element, "mousemove", function (event) { self.onPointer(event); });
  this.listen(this.element, "mouseleave", function () { self.hideTooltip(); });
  this.listen(this.element, "keydown", function (event) { self.onKeyDown(event); });

  this.ctx = this.createContext();
  try {
    this.hooks = KIT.load(this.ctx);
  } catch (error) {
    this.hooks = {};
    this.loadError = error;
  }
}

Visual.prototype.listen = function (target, type, handler, options) {
  target.addEventListener(type, handler, options);
  this.listeners.push([target, type, handler, options]);
};

Visual.prototype.canInteract = function () {
  var capabilities = this.host.hostCapabilities;
  if (capabilities && capabilities.allowInteractions !== undefined) { return !!capabilities.allowInteractions; }
  return this.host.allowInteractions !== false;
};

Visual.prototype.readColors = function () {
  var palette = this.host.colorPalette || {};
  function pick(name, fallback) { return palette[name] && palette[name].value ? palette[name].value : fallback; }
  var colors = {
    isHighContrast: !!palette.isHighContrast,
    foreground: pick("foreground", "#252423"),
    background: pick("background", "#FFFFFF"),
    foregroundSelected: pick("foregroundSelected", pick("foreground", "#252423")),
    hyperlink: pick("hyperlink", "#0078D4"),
    positive: pick("positive", "#1AAB40"),
    negative: pick("negative", "#D64554"),
    neutral: pick("neutral", "#D9B300")
  };
  // The first colour a palette hands out is the theme's first data colour.
  colors.accent = colors.isHighContrast || !palette.getColor ? colors.foreground
    : palette.getColor("vk-accent").value;
  return colors;
};

Visual.prototype.createContext = function () {
  var self = this, handlers = {}, wired = [];
  var ctx = {
    root: this.root,
    rows: [],
    columns: this.model.columns,
    settings: this.settings,
    colors: this.colors,
    viewport: { width: 0, height: 0 },
    locale: this.host.locale || "",
    hasSelection: false,
    hasHighlights: false,
    hasMoreRows: false,
    loadingMore: false,
    truncated: false,
    rowLimit: this.config.rows,
    interactive: this.canInteract(),
    // What a script written for a measure-driven page expects of its APP object.
    state: {},
    data: {},
    runs: 0,
    main: null
  };
  ctx.escape = KIT.escape;
  ctx.format = function (value, format, options) {
    var pattern = format && typeof format === "object" ? format.format : format;
    var merged = { locale: ctx.locale };
    Object.keys(options || {}).forEach(function (key) { merged[key] = options[key]; });
    return KIT.format(value, pattern, merged);
  };
  ctx.color = function (key) {
    var palette = self.host.colorPalette;
    if (self.colors.isHighContrast || !palette || !palette.getColor) { return self.colors.foreground; }
    return palette.getColor(String(key)).value;
  };
  ctx.select = function (rows, multi) { return self.selectRows([].concat(rows || []), multi); };
  ctx.clearSelection = function () { return self.clearSelection(); };
  ctx.contextMenu = function (row, point) { self.showContextMenu(row, point); };
  ctx.showTooltip = function (rowOrItems, point) {
    self.showTooltip(rowOrItems, point);
    self.tooltipAuto = false;
  };
  ctx.hideTooltip = function () { self.hideTooltip(); };
  ctx.openUrl = function (url) { return self.openUrl(url); };
  // One real listener per (target, type, key): a later call only swaps the handler it runs.
  ctx.on = function (target, type, key, handler, options) {
    var index = wired.indexOf(target), id;
    if (index < 0) { index = wired.push(target) - 1; }
    id = index + "|" + type + "|" + key;
    if (!Object.prototype.hasOwnProperty.call(handlers, id)) {
      self.listen(target, type, function (event) { return handlers[id].call(this, event); }, options);
    }
    handlers[id] = handler;
  };
  // At most one repaint per frame; the last function asked for wins.
  ctx.schedule = function (fn) {
    self.pending = fn;
    if (self.frame) { return; }
    self.frame = window.requestAnimationFrame(function () {
      var next = self.pending;
      self.frame = 0;
      self.pending = null;
      if (next) { next(ctx); }
    });
  };
  return ctx;
};

// ---------------------------------------------------------------- update

Visual.prototype.update = function (options) {
  var events = this.host.eventService, dataView, ctx = this.ctx, loading = false;
  if (events && events.renderingStarted) { events.renderingStarted(options); }
  try {
    if (this.loadError) { throw this.loadError; }
    dataView = options && options.dataViews && options.dataViews[0];
    this.colors = this.readColors();
    this.settings = KIT.readSettings(this.config.format,
                                     dataView && dataView.metadata && dataView.metadata.objects, this.colors);
    this.model = KIT.buildModel(dataView, this.config, this.host);
    this.markSelection(this.selectionManager && this.selectionManager.getSelectionIds
      ? this.selectionManager.getSelectionIds() : this.selectedIds);

    ctx.rows = this.model.rows;
    ctx.columns = this.model.columns;
    ctx.settings = this.settings;
    ctx.colors = this.colors;
    ctx.viewport = options && options.viewport
      ? { width: options.viewport.width, height: options.viewport.height } : ctx.viewport;
    // The host serves rows a window at a time. While the definition wants more than it has and
    // the host has more, ask for the next window: the host answers with another update.
    loading = this.features.fetchMore && this.model.more && this.model.rows.length < this.config.rows &&
      !!this.host.fetchMoreData && this.host.fetchMoreData(true) === true;
    ctx.hasHighlights = this.model.hasHighlights;
    ctx.hasMoreRows = this.model.more;
    ctx.loadingMore = loading;
    // True when the model holds more rows than the visual was given: totals drawn from
    // ctx.rows are then totals of the first rows only.
    ctx.truncated = this.model.truncated;
    ctx.interactive = this.canInteract();
    this.applyTheme();
    this.hideTooltip();

    if (!this.model.bound && this.features.landingPage) {
      this.showNotice("vk-landing", this.config.landing.title, this.config.landing.text);
    } else if (!this.model.rows.length) {
      this.showNotice("vk-empty", "", this.config.emptyMessage);
    } else {
      this.render();
    }
    this.showLimit(this.model.truncated && this.features.limitNotice && this.model.rows.length > 0);
    this.decorate();
    // Every update reports its own end, also one that drew a first window while more rows
    // are on their way: a started without a finished would leave the host waiting.
    if (events && events.renderingFinished) { events.renderingFinished(options); }
  } catch (error) {
    this.showNotice("vk-error", "This visual could not be drawn", String(error && error.message ? error.message : error));
    this.showLimit(false);
    if (events && events.renderingFailed) {
      events.renderingFailed(options, String(error && error.message ? error.message : error));
    }
  }
};

Visual.prototype.render = function () {
  var entry = this.hooks.render || this.hooks.main || this.ctx.main;
  var focused = this.focusedIndex(), output, nodes, k;
  if (typeof entry !== "function") {
    throw new Error("The script defines neither render(ctx) nor main(app).");
  }
  this.notice.className = "vk-notice vk-hidden";
  this.root.classList.remove("vk-hidden");
  this.ctx.runs += 1;
  output = entry(this.ctx);
  if (typeof output === "string") {
    this.root.innerHTML = output;
  } else if (output && (output.nodeType === 1 || output.nodeType === 11)) {
    if (output !== this.root) {
      while (this.root.firstChild) { this.root.removeChild(this.root.firstChild); }
      this.root.appendChild(output);
    }
  } else if (output !== undefined && output !== null) {
    // Nothing returned means the code drew into ctx.root itself. Anything else is a mistake
    // that would otherwise leave the visual blank and reported as drawn.
    throw new Error("The render function returned " + (typeof output === "object" ? "an object" : "a " + typeof output) +
                    ". Return the markup as text, an element, or nothing after drawing into ctx.root.");
  }
  if (focused !== null) {
    // Drawing again replaced the element that had the keyboard focus: give it back.
    nodes = this.rowNodes();
    for (k = 0; k < nodes.length; k += 1) {
      if (this.indexOf(nodes[k]) === focused) { nodes[k].focus(); break; }
    }
  }
};

Visual.prototype.showNotice = function (kind, title, text) {
  var heading, body;
  while (this.notice.firstChild) { this.notice.removeChild(this.notice.firstChild); }
  if (title) {
    heading = document.createElement("div");
    heading.className = "vk-notice-title";
    heading.textContent = title;
    this.notice.appendChild(heading);
  }
  if (text) {
    body = document.createElement("div");
    body.className = "vk-notice-text";
    body.textContent = text;
    this.notice.appendChild(body);
  }
  this.notice.className = "vk-notice " + kind;
  this.root.classList.add("vk-hidden");
};

// The kit's own line for data that was cut: a visual that sums its rows would otherwise show a
// wrong total with nothing to say so. A definition can switch it off (features.limit_notice).
Visual.prototype.showLimit = function (show) {
  if (show) {
    this.limit.textContent = "Showing the first " +
      KIT.format(this.model.rows.length, "#,0", { locale: this.ctx.locale }) + " rows";
  }
  this.limit.classList.toggle("vk-hidden", !show);
};

Visual.prototype.applyTheme = function () {
  var colors = this.colors, style = this.element.style;
  if (style && style.setProperty) {
    style.setProperty("--vk-foreground", colors.foreground);
    style.setProperty("--vk-background", colors.background);
    style.setProperty("--vk-foreground-selected", colors.foregroundSelected);
    style.setProperty("--vk-hyperlink", colors.hyperlink);
    style.setProperty("--vk-accent", colors.accent);
  }
  this.root.classList.toggle("vk-high-contrast", colors.isHighContrast);
  this.root.classList.toggle("vk-static", !this.canInteract());
};

// ---------------------------------------------------------------- rows in the DOM

Visual.prototype.rowNodes = function () {
  return this.root.querySelectorAll("[data-row]");
};

// The row an element names: whole numbers only, so "1.9" or "2abc" is not quietly row 1 or 2.
Visual.prototype.indexOf = function (node) {
  var text = String(node.getAttribute("data-row")).trim();
  return /^[0-9]+$/.test(text) ? parseInt(text, 10) : -1;
};

Visual.prototype.rowOf = function (target) {
  var node = KIT.up(target, function (item) { return item.hasAttribute("data-row"); }, this.element);
  return node ? { node: node, row: this.model.rows[this.indexOf(node)] || null } : null;
};

Visual.prototype.focusedIndex = function () {
  var hit = document.activeElement ? this.rowOf(document.activeElement) : null;
  return hit && hit.row ? hit.row.index : null;
};

Visual.prototype.markSelection = function (ids) {
  var rows = this.model.rows, any = false, highlights = this.model.hasHighlights;
  this.selectedIds = ids || [];
  rows.forEach(function (row) {
    row.selected = !!row.identity && this.selectedIds.some(function (id) {
      return KIT.sameIdentity(id, row.identity);
    });
    any = any || row.selected;
  }, this);
  rows.forEach(function (row) {
    row.dimmed = any ? !row.selected : (highlights && !row.highlighted);
  });
  this.ctx.hasSelection = any;
};

// Keep the classes and the accessibility state of every data-row element in step with the rows.
Visual.prototype.decorate = function () {
  var nodes = this.rowNodes(), keyboard = this.features.keyboard && this.canInteract();
  var selectable = this.features.selection, k, node, row;
  for (k = 0; k < nodes.length; k += 1) {
    node = nodes[k];
    row = this.model.rows[this.indexOf(node)];
    if (!row) { continue; }
    node.classList.toggle("vk-selected", row.selected);
    node.classList.toggle("vk-dimmed", row.dimmed);
    node.classList.toggle("vk-highlighted", this.model.hasHighlights && row.highlighted);
    if (keyboard && !node.hasAttribute("tabindex")) { node.setAttribute("tabindex", "0"); }
    if (selectable) {
      if (!node.hasAttribute("role")) { node.setAttribute("role", "option"); }
      node.setAttribute("aria-selected", row.selected ? "true" : "false");
    }
  }
  if (selectable && nodes.length && !this.root.hasAttribute("role")) {
    this.root.setAttribute("role", "listbox");
    this.root.setAttribute("aria-multiselectable", "true");
    this.root.setAttribute("aria-label", this.config.displayName);
  }
  this.root.classList.toggle("vk-has-selection", this.ctx.hasSelection);
  this.root.classList.toggle("vk-has-highlights", this.model.hasHighlights);
};

// ---------------------------------------------------------------- selection

Visual.prototype.afterSelection = function (ids) {
  this.markSelection(ids);
  this.decorate();
  if (this.hooks.selectionChanged) { this.hooks.selectionChanged(this.ctx); }
};

Visual.prototype.selectRows = function (rows, multi) {
  var self = this, manager = this.selectionManager, ids, pending;
  ids = rows.map(function (row) { return row && row.identity; }).filter(Boolean);
  if (!manager || !this.features.selection || !this.canInteract() || !ids.length) { return false; }
  pending = manager.select(ids.length === 1 ? ids[0] : ids, !!multi);
  if (pending && typeof pending.then === "function") {
    pending.then(function (result) {
      self.afterSelection(result || (manager.getSelectionIds ? manager.getSelectionIds() : []));
    });
  } else {
    this.afterSelection(manager.getSelectionIds ? manager.getSelectionIds() : ids);
  }
  return true;
};

Visual.prototype.clearSelection = function () {
  var self = this, manager = this.selectionManager, pending;
  if (!manager || !this.canInteract()) { return false; }
  pending = manager.clear();
  if (pending && typeof pending.then === "function") {
    pending.then(function () { self.afterSelection([]); });
  } else {
    this.afterSelection([]);
  }
  return true;
};

Visual.prototype.showContextMenu = function (row, point) {
  if (!this.selectionManager || !this.features.contextMenu || !this.canInteract()) { return; }
  // An empty object asks for the menu of the visual itself (the empty-space menu).
  this.selectionManager.showContextMenu(row && row.identity ? row.identity : {},
                                        { x: point ? point.clientX || point.x || 0 : 0,
                                          y: point ? point.clientY || point.y || 0 : 0 });
};

// ---------------------------------------------------------------- tooltips

Visual.prototype.tooltipArgs = function (rowOrItems, point) {
  var box = this.element.getBoundingClientRect ? this.element.getBoundingClientRect() : { left: 0, top: 0 };
  var row = Array.isArray(rowOrItems) ? null : rowOrItems, items = row ? null : rowOrItems;
  if (row) {
    items = this.hooks.tooltip ? this.hooks.tooltip(row, this.ctx) : null;
    if (items === null || items === undefined) {
      items = KIT.tooltipItems(row, this.config, this.model.hasHighlights, this.ctx.locale);
    }
  }
  if (!items || !items.length) { return null; }
  return {
    coordinates: [(point && (point.clientX !== undefined ? point.clientX : point.x) || 0) - box.left,
                  (point && (point.clientY !== undefined ? point.clientY : point.y) || 0) - box.top],
    isTouchEvent: false,
    dataItems: items.map(function (item) {
      var out = { displayName: String(item.displayName === undefined ? "" : item.displayName),
                  value: String(item.value === undefined || item.value === null ? "" : item.value) };
      if (item.color) { out.color = item.color; }
      if (item.header) { out.header = item.header; }
      return out;
    }),
    // The identity is what lets the host put a report-page tooltip in place of the default one.
    identities: row && row.identity ? [row.identity] : []
  };
};

Visual.prototype.showTooltip = function (rowOrItems, point) {
  var service = this.host.tooltipService, args;
  if (!this.features.tooltips || !service || (service.enabled && !service.enabled())) { return; }
  args = this.tooltipArgs(rowOrItems, point);
  if (!args) { this.hideTooltip(); return; }
  // The same row again, or the author's own lines again, is a tooltip that follows the pointer.
  if (this.tooltipRow !== null && (this.tooltipRow === rowOrItems ||
                                   (Array.isArray(rowOrItems) && Array.isArray(this.tooltipRow)))) {
    service.move(args);
  } else {
    service.show(args);
  }
  this.tooltipRow = rowOrItems;
};

Visual.prototype.hideTooltip = function () {
  var service = this.host.tooltipService;
  if (this.tooltipRow === null || !service) { return; }
  this.tooltipRow = null;
  this.tooltipAuto = false;
  service.hide({ immediately: true, isTouchEvent: false });
};

Visual.prototype.openUrl = function (url) {
  // The sandbox blocks navigation; the host opens links, and only these schemes.
  if (!/^(https?:|mailto:)/i.test(String(url || "")) || !this.host.launchUrl) { return false; }
  this.host.launchUrl(String(url));
  return true;
};

// ---------------------------------------------------------------- events

Visual.prototype.onClick = function (event) {
  var link = KIT.up(event.target, function (node) {
    return node.hasAttribute("data-url") || (node.tagName === "A" && node.hasAttribute("href"));
  }, this.element);
  var hit;
  if (link) {
    event.preventDefault();
    this.openUrl(link.getAttribute("data-url") || link.getAttribute("href"));
    return;
  }
  // Whether the host allows interactions is asked in one place for each action: selectRows
  // and clearSelection, which the author's own ctx.select and ctx.clearSelection go through too.
  if (!this.features.selection) { return; }
  hit = this.rowOf(event.target);
  if (hit && hit.row) {
    this.selectRows([hit.row], event.ctrlKey || event.metaKey);
    return;
  }
  // The author's own controls must not throw the selection away.
  if (KIT.up(event.target, function (node) {
    return /^(BUTTON|INPUT|SELECT|TEXTAREA|LABEL)$/.test(node.tagName) || node.hasAttribute("data-keep-selection");
  }, this.element)) { return; }
  if (this.selectionManager && this.selectionManager.hasSelection && this.selectionManager.hasSelection()) {
    this.clearSelection();
  }
};

Visual.prototype.onContextMenu = function (event) {
  var hit;
  if (!this.features.contextMenu || !this.canInteract()) { return; }
  hit = this.rowOf(event.target);
  event.preventDefault();
  event.stopPropagation();
  this.showContextMenu(hit ? hit.row : null, event);
};

Visual.prototype.onPointer = function (event) {
  var hit;
  if (!this.features.tooltips) { return; }
  hit = this.rowOf(event.target);
  if (hit && hit.row) {
    this.showTooltip(hit.row, event);
    this.tooltipAuto = true;
  } else if (this.tooltipAuto) {
    // Only a tooltip this handler opened is closed here: one the author's code opened with
    // ctx.showTooltip stays until that code hides it or the pointer leaves the visual.
    this.hideTooltip();
  }
};

Visual.prototype.onKeyDown = function (event) {
  var hit, key = event.key, nodes, at, k, box;
  if (!this.features.keyboard || !this.canInteract()) { return; }
  hit = this.rowOf(event.target);
  if (!hit || !hit.row) { return; }
  if (key === "Enter" || key === " " || key === "Spacebar") {
    event.preventDefault();
    this.selectRows([hit.row], event.ctrlKey || event.metaKey);
  } else if (key === "ContextMenu" || (key === "F10" && event.shiftKey)) {
    event.preventDefault();
    box = hit.node.getBoundingClientRect();
    this.showContextMenu(hit.row, { x: box.left + box.width / 2, y: box.top + box.height / 2 });
  } else if (/^(ArrowDown|ArrowRight|ArrowUp|ArrowLeft|Home|End)$/.test(key)) {
    nodes = this.rowNodes();
    at = -1;
    for (k = 0; k < nodes.length; k += 1) { if (nodes[k] === hit.node) { at = k; } }
    if (key === "Home") { at = 0; }
    else if (key === "End") { at = nodes.length - 1; }
    else { at += key === "ArrowDown" || key === "ArrowRight" ? 1 : -1; }
    if (at >= 0 && at < nodes.length) {
      event.preventDefault();
      nodes[at].focus();
    }
  }
};

// ---------------------------------------------------------------- host contract

Visual.prototype.getFormattingModel = function () {
  return KIT.formattingModel(this.config.format, this.settings);
};

Visual.prototype.destroy = function () {
  this.hideTooltip();
  this.listeners.forEach(function (item) { item[0].removeEventListener(item[1], item[2], item[3]); });
  this.listeners = [];
  if (this.frame && window.cancelAnimationFrame) { window.cancelAnimationFrame(this.frame); }
  this.frame = 0;
  this.pending = null;
};
