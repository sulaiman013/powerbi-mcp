// Runs a packaged visual outside Power BI: a small stand-in for the browser DOM and for the
// visual host, driven by a list of steps, reporting every call the visual made to the host.
//
//     node harness.js <job.json>          prints the result as JSON
//
// It EXECUTES the visual's script in this Node process, with this user's rights. Use it for
// code you wrote or reviewed (the test-suite does); never point it at a package from elsewhere.
//
// The host here follows the documented visuals API (selection manager, selection id builder,
// tooltip service, event service, colour palette). It is a model of the host, not the host:
// what Power BI Desktop really does can only be seen in Power BI Desktop.
"use strict";

const fs = require("fs");
const vm = require("vm");

// ---------------------------------------------------------------- DOM

const VOID_TAGS = new Set(["area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta",
                           "source", "track", "wbr"]);
const ENTITIES = { amp: "&", lt: "<", gt: ">", quot: "\"", apos: "'", nbsp: String.fromCharCode(160) };

function decodeEntities(text) {
  return text.replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/gi, function (whole, name) {
    if (name.charAt(0) === "#") {
      const code = name.charAt(1).toLowerCase() === "x" ? parseInt(name.slice(2), 16) : parseInt(name.slice(1), 10);
      return String.fromCodePoint(code);
    }
    return Object.prototype.hasOwnProperty.call(ENTITIES, name) ? ENTITIES[name] : whole;
  });
}

function encodeText(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

class TextNode {
  constructor(text) {
    this.nodeType = 3;
    this.parentNode = null;
    this.data = String(text);
  }
  get textContent() { return this.data; }
  set textContent(value) { this.data = String(value); }
  get nodeValue() { return this.data; }
}

class Style {
  constructor() { this.properties = {}; }
  setProperty(name, value) { this.properties[name] = String(value); }
  getPropertyValue(name) { return this.properties[name] || ""; }
  removeProperty(name) { delete this.properties[name]; }
}

class ClassList {
  constructor(element) { this.element = element; }
  list() { return (this.element.attributes["class"] || "").split(/\s+/).filter(Boolean); }
  write(names) { this.element.attributes["class"] = names.join(" "); }
  contains(name) { return this.list().indexOf(name) >= 0; }
  add(...names) {
    const now = this.list();
    names.forEach((name) => { if (now.indexOf(name) < 0) { now.push(name); } });
    this.write(now);
  }
  remove(...names) { this.write(this.list().filter((name) => names.indexOf(name) < 0)); }
  toggle(name, force) {
    const on = force === undefined ? !this.contains(name) : !!force;
    if (on) { this.add(name); } else { this.remove(name); }
    return on;
  }
}

class Element {
  constructor(tag, document) {
    this.nodeType = 1;
    this.tagName = String(tag).toUpperCase();
    this.ownerDocument = document;
    this.parentNode = null;
    this.childNodes = [];
    this.attributes = {};
    this.listeners = {};
    this.style = new Style();
    this.classList = new ClassList(this);
    this.rect = { left: 0, top: 0, width: 100, height: 20 };
  }

  // attributes
  get className() { return this.attributes["class"] || ""; }
  set className(value) { this.attributes["class"] = String(value); }
  get id() { return this.attributes.id || ""; }
  set id(value) { this.attributes.id = String(value); }
  get tabIndex() { return this.hasAttribute("tabindex") ? Number(this.attributes.tabindex) : -1; }
  set tabIndex(value) { this.attributes.tabindex = String(value); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  getAttribute(name) { return this.hasAttribute(name) ? this.attributes[name] : null; }
  hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attributes, name); }
  removeAttribute(name) { delete this.attributes[name]; }
  get dataset() {
    const element = this;
    const attr = (key) => "data-" + String(key).replace(/[A-Z]/g, (c) => "-" + c.toLowerCase());
    return new Proxy({}, {
      get: (_, key) => (typeof key === "string" && element.hasAttribute(attr(key)) ? element.attributes[attr(key)] : undefined),
      set: (_, key, value) => { element.setAttribute(attr(key), value); return true; },
    });
  }

  // tree
  get firstChild() { return this.childNodes[0] || null; }
  get lastChild() { return this.childNodes[this.childNodes.length - 1] || null; }
  get children() { return this.childNodes.filter((node) => node.nodeType === 1); }
  get childElementCount() { return this.children.length; }
  appendChild(node) { return this.insertBefore(node, null); }
  insertBefore(node, reference) {
    if (node.nodeType === 11) {
      node.childNodes.slice().forEach((child) => this.insertBefore(child, reference));
      return node;
    }
    if (node.parentNode) { node.parentNode.removeChild(node); }
    const at = reference ? this.childNodes.indexOf(reference) : -1;
    if (at < 0) { this.childNodes.push(node); } else { this.childNodes.splice(at, 0, node); }
    node.parentNode = this;
    return node;
  }
  removeChild(node) {
    const at = this.childNodes.indexOf(node);
    if (at < 0) { throw new Error("removeChild: the node is not a child of this element"); }
    this.childNodes.splice(at, 1);
    node.parentNode = null;
    if (this.ownerDocument && this.ownerDocument.activeElement &&
        (node === this.ownerDocument.activeElement || (node.contains && node.contains(this.ownerDocument.activeElement)))) {
      this.ownerDocument.activeElement = null;      // like a browser: focus falls back to the body
    }
    return node;
  }
  remove() { if (this.parentNode) { this.parentNode.removeChild(this); } }
  contains(node) {
    for (let at = node; at; at = at.parentNode) { if (at === this) { return true; } }
    return false;
  }
  walk(visit) {
    this.childNodes.forEach((node) => {
      if (node.nodeType === 1) { visit(node); node.walk(visit); }
    });
  }

  // content
  get textContent() { return this.childNodes.map((node) => node.textContent).join(""); }
  set textContent(value) {
    this.clear();
    if (value !== "" && value !== null && value !== undefined) { this.appendChild(new TextNode(value)); }
  }
  get innerText() { return this.textContent; }
  set innerText(value) { this.textContent = value; }
  clear() { this.childNodes.slice().forEach((node) => this.removeChild(node)); }
  get innerHTML() { return this.childNodes.map(serialize).join(""); }
  set innerHTML(markup) {
    this.clear();
    parseInto(this, String(markup));
  }
  get outerHTML() { return serialize(this); }

  // selectors: tag, .class, #id, [attr], [attr="value"], compounds of those, descendant
  // combinators and comma lists. Enough for a visual's own lookups.
  matches(selector) { return selector.split(",").some((part) => matchChain(this, part.trim().split(/\s+/))); }
  closest(selector) {
    for (let at = this; at && at.nodeType === 1; at = at.parentNode) { if (at.matches(selector)) { return at; } }
    return null;
  }
  querySelectorAll(selector) {
    const found = [];
    this.walk((node) => { if (node.matches(selector)) { found.push(node); } });
    return found;
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  getElementsByTagName(tag) { return this.querySelectorAll(tag); }
  getElementsByClassName(name) { return this.querySelectorAll("." + name); }

  // events
  addEventListener(type, handler) { (this.listeners[type] = this.listeners[type] || []).push(handler); }
  removeEventListener(type, handler) {
    this.listeners[type] = (this.listeners[type] || []).filter((item) => item !== handler);
  }
  dispatchEvent(event) {
    event.target = event.target || this;
    for (let at = this; at; at = event.bubbles ? at.parentNode : null) {
      event.currentTarget = at;
      (at.listeners && at.listeners[event.type] || []).slice().forEach((handler) => handler.call(at, event));
      if (event.stopped) { break; }
    }
    return !event.defaultPrevented;
  }

  // layout and focus
  getBoundingClientRect() {
    const r = this.rect;
    return { left: r.left, top: r.top, width: r.width, height: r.height, x: r.left, y: r.top,
             right: r.left + r.width, bottom: r.top + r.height };
  }
  get clientWidth() { return this.rect.width; }
  get clientHeight() { return this.rect.height; }
  get offsetWidth() { return this.rect.width; }
  get offsetHeight() { return this.rect.height; }
  focus() { this.ownerDocument.activeElement = this; }
  blur() { if (this.ownerDocument.activeElement === this) { this.ownerDocument.activeElement = null; } }
  // A canvas context that accepts any drawing call and draws nothing.
  getContext() {
    const sink = new Proxy(function () {}, {
      get: (_, key) => (key === "canvas" ? this : key === Symbol.toPrimitive ? () => 0 : sink),
      set: () => true,
      apply: () => sink,
    });
    return sink;
  }
}

function matchCompound(element, compound) {
  const parts = compound.match(/[#.]?[\w-]+|\[[^\]]+\]|\*/g) || [];
  return parts.every((part) => {
    if (part === "*") { return true; }
    if (part.charAt(0) === ".") { return element.classList.contains(part.slice(1)); }
    if (part.charAt(0) === "#") { return element.id === part.slice(1); }
    if (part.charAt(0) === "[") {
      const body = part.slice(1, -1);
      const eq = body.indexOf("=");
      if (eq < 0) { return element.hasAttribute(body.trim()); }
      const wanted = body.slice(eq + 1).trim().replace(/^["']|["']$/g, "");
      return element.getAttribute(body.slice(0, eq).trim()) === wanted;
    }
    return element.tagName === part.toUpperCase();
  });
}

function matchChain(element, chain) {
  if (!matchCompound(element, chain[chain.length - 1])) { return false; }
  if (chain.length === 1) { return true; }
  for (let at = element.parentNode; at && at.nodeType === 1; at = at.parentNode) {
    if (matchChain(at, chain.slice(0, -1))) { return true; }
  }
  return false;
}

function serialize(node) {
  if (node.nodeType === 3) { return encodeText(node.data); }
  const tag = node.tagName.toLowerCase();
  const attrs = Object.keys(node.attributes).map((name) =>
    " " + name + "=\"" + encodeText(node.attributes[name]).replace(/"/g, "&quot;") + "\"").join("");
  if (VOID_TAGS.has(tag)) { return "<" + tag + attrs + ">"; }
  return "<" + tag + attrs + ">" + node.childNodes.map(serialize).join("") + "</" + tag + ">";
}

// A forgiving HTML reader: elements, attributes, text and comments; no implied tags.
function parseInto(root, markup) {
  const token = /<!--[\s\S]*?-->|<\/([A-Za-z][\w:-]*)\s*>|<([A-Za-z][\w:-]*)((?:\s+[^\s=>\/]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+))?)*)\s*(\/?)>|([^<]+)|</g;
  const attribute = /([^\s=>\/]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g;
  let open = root, found;
  while ((found = token.exec(markup)) !== null) {
    if (found[1]) {
      const closing = found[1].toUpperCase();
      for (let at = open; at && at !== root.parentNode; at = at.parentNode) {
        if (at.tagName === closing && at !== root) { open = at.parentNode; break; }
      }
    } else if (found[2]) {
      const element = new Element(found[2], root.ownerDocument);
      let pair;
      attribute.lastIndex = 0;
      while ((pair = attribute.exec(found[3] || "")) !== null) {
        const value = pair[2] !== undefined ? pair[2] : pair[3] !== undefined ? pair[3] : pair[4] || "";
        element.attributes[pair[1]] = decodeEntities(value);
      }
      open.appendChild(element);
      if (!found[4] && !VOID_TAGS.has(found[2].toLowerCase())) { open = element; }
    } else if (found[5]) {
      open.appendChild(new TextNode(decodeEntities(found[5])));
    } else if (found[0] === "<") {
      open.appendChild(new TextNode("<"));
    }
  }
}

class Document {
  constructor() {
    this.nodeType = 9;
    this.activeElement = null;
    this.documentElement = new Element("html", this);
    this.head = this.documentElement.appendChild(new Element("head", this));
    this.body = this.documentElement.appendChild(new Element("body", this));
    this.listeners = {};
  }
  createElement(tag) { return new Element(tag, this); }
  createElementNS(namespace, tag) { return new Element(tag, this); }
  createTextNode(text) { return new TextNode(text); }
  createDocumentFragment() {
    const fragment = new Element("#fragment", this);
    fragment.nodeType = 11;
    return fragment;
  }
  getElementById(id) { return this.documentElement.querySelector("#" + id); }
  querySelector(selector) { return this.documentElement.querySelector(selector); }
  querySelectorAll(selector) { return this.documentElement.querySelectorAll(selector); }
  addEventListener(type, handler) { (this.listeners[type] = this.listeners[type] || []).push(handler); }
  removeEventListener(type, handler) {
    this.listeners[type] = (this.listeners[type] || []).filter((item) => item !== handler);
  }
}

function makeEvent(type, init) {
  const event = Object.assign({
    type: type, bubbles: type !== "mouseleave" && type !== "mouseenter", target: null, currentTarget: null,
    clientX: 0, clientY: 0, ctrlKey: false, metaKey: false, shiftKey: false, altKey: false, key: "", button: 0,
    defaultPrevented: false, stopped: false,
  }, init || {});
  event.preventDefault = function () { event.defaultPrevented = true; };
  event.stopPropagation = function () { event.stopped = true; };
  return event;
}

// ---------------------------------------------------------------- host

const PALETTE = ["#118DFF", "#12239E", "#E66C37", "#6B007B", "#E044A7", "#744EC2", "#D9B300", "#D64550"];

function makeHost(settings, log, current) {
  const state = { selected: [], onSelect: null, colors: {} };

  function keys(ids) { return [].concat(ids || []).map((id) => (id && id.getKey ? id.getKey() : "{}")); }
  function contains(id) { return state.selected.some((other) => other.equals(id)); }

  const selectionManager = {
    // Same rules as the host's manager: a plain click on the only selected point clears it,
    // a multi-select click toggles the point.
    select(idOrIds, multiSelect) {
      const ids = [].concat(idOrIds);
      log({ call: "select", ids: keys(ids), multiSelect: !!multiSelect });
      if (ids.length === 1 && contains(ids[0])) {
        state.selected = multiSelect ? state.selected.filter((other) => !other.equals(ids[0]))
          : (state.selected.length > 1 ? [ids[0]] : []);
      } else if (multiSelect) {
        ids.forEach((id) => { if (!contains(id)) { state.selected.push(id); } });
      } else {
        state.selected = ids.slice();
      }
      return Promise.resolve(state.selected.slice());
    },
    clear() {
      log({ call: "clear" });
      state.selected = [];
      return Promise.resolve({});
    },
    hasSelection() { return state.selected.length > 0; },
    getSelectionIds() { return state.selected.slice(); },
    registerOnSelectCallback(callback) {
      log({ call: "registerOnSelectCallback" });
      state.onSelect = callback;
    },
    showContextMenu(id, position, dataRoles) {
      // The API takes a selection id, or an empty object for the menu of the visual itself.
      // Anything else (null, undefined) is recorded as such: the real host may not take it.
      const isIdentity = !!id && typeof id.getKey === "function";
      const isEmptyObject = !!id && typeof id === "object" && !isIdentity && Object.keys(id).length === 0;
      const entry = { call: "showContextMenu", id: isIdentity ? id.getKey() : null,
                      argument: isIdentity ? "identity" : isEmptyObject ? "empty object" : String(id),
                      emptySpace: isEmptyObject, x: position.x, y: position.y };
      if (dataRoles !== undefined) { entry.dataRoles = dataRoles; }
      log(entry);
      return Promise.resolve({});
    },
    toggleExpandCollapse() { return Promise.resolve({}); },
  };

  // Read from the settings each time, so a step can switch high contrast on and off.
  const palette = {
    get isHighContrast() { return !!settings.highContrast; },
    get foreground() { return { value: settings.highContrast ? "#FFFFFF" : "#252423" }; },
    get background() { return { value: settings.highContrast ? "#000000" : "#FFFFFF" }; },
    get foregroundSelected() { return { value: settings.highContrast ? "#00FFFF" : "#252423" }; },
    get hyperlink() { return { value: settings.highContrast ? "#FFFF00" : "#0078D4" }; },
    getColor(key) {
      if (!Object.prototype.hasOwnProperty.call(state.colors, key)) {
        state.colors[key] = PALETTE[Object.keys(state.colors).length % PALETTE.length];
      }
      return { value: state.colors[key] };
    },
  };

  const host = {
    locale: settings.locale || "en-US",
    hostCapabilities: { allowInteractions: settings.allowInteractions !== false },
    colorPalette: palette,
    createSelectionManager() {
      log({ call: "createSelectionManager" });
      return selectionManager;
    },
    createSelectionIdBuilder() {
      const parts = [];
      const builder = {
        withCategory(column, index) {
          parts.push(column.source.queryName + "=" + JSON.stringify(column.values[index]));
          return builder;
        },
        withMeasure(name) { parts.push("measure=" + name); return builder; },
        withSeries() { return builder; },
        withTable(table, index) { parts.push("row=" + index); return builder; },
        withMatrixNode() { return builder; },
        createSelectionId() {
          const key = parts.join("|");
          return {
            getKey() { return key; },
            equals(other) { return !!other && typeof other.getKey === "function" && other.getKey() === key; },
            includes(other) { return this.equals(other); },
            hasIdentity() { return parts.length > 0; },
            getSelector() { return {}; },
          };
        },
      };
      return builder;
    },
    tooltipService: {
      enabled() { return settings.tooltipsEnabled !== false; },
      show(options) { log(tooltipEntry("tooltip.show", options)); },
      move(options) { log(tooltipEntry("tooltip.move", options)); },
      hide(options) { log({ call: "tooltip.hide", immediately: options.immediately, isTouchEvent: options.isTouchEvent }); },
    },
    eventService: {
      renderingStarted(options) { log({ call: "renderingStarted", sameOptions: options === current.options }); },
      renderingFinished(options) { log({ call: "renderingFinished", sameOptions: options === current.options }); },
      renderingFailed(options, reason) { log({ call: "renderingFailed", reason: String(reason) }); },
    },
    launchUrl(url) { log({ call: "launchUrl", url: url }); },
    fetchMoreData(aggregate) {
      log({ call: "fetchMoreData", aggregateSegments: aggregate });
      return true;
    },
    persistProperties(changes) { log({ call: "persistProperties", changes: changes }); },
    displayWarningIcon(title, detail) { log({ call: "displayWarningIcon", title: title, detail: detail }); },
    drill(args) { log({ call: "drill", args: args }); },
    applyJsonFilter() { log({ call: "applyJsonFilter" }); },
  };

  function tooltipEntry(call, options) {
    return { call: call, coordinates: options.coordinates, isTouchEvent: options.isTouchEvent,
             dataItems: options.dataItems, identities: keys(options.identities) };
  }

  return { host: host, state: state };
}

// A categorical data view: grouping columns as categories, measures as values.
function makeDataView(data, host) {
  if (!data) { return null; }
  const categories = [], values = [], columns = [];
  data.columns.forEach((column, at) => {
    const measure = column.kind === "measure";
    const roles = {};
    [].concat(column.role).forEach((name) => { roles[name] = true; });
    const type = column.type === "date" ? { dateTime: true } : column.type === "text" || (!measure && !column.type)
      ? { text: true } : { numeric: true };
    const source = { displayName: column.name, queryName: column.queryName || ("t." + column.name), roles: roles,
                     type: type, format: column.format, isMeasure: measure, index: at };
    const cells = data.rows.map((row) =>
      (column.type === "date" && row[at] !== null ? new Date(row[at]) : row[at]));
    columns.push(source);
    if (measure) {
      const entry = { source: source, values: cells };
      if (data.highlights) { entry.highlights = data.highlights.map((row) => row[at]); }
      values.push(entry);
    } else {
      categories.push({ source: source, values: cells, identity: cells.map((_, index) => ({ identityIndex: index })) });
    }
  });
  values.grouped = () => [{ values: values }];
  const view = { metadata: { columns: columns }, categorical: {} };
  if (categories.length) { view.categorical.categories = categories; }
  if (values.length) { view.categorical.values = values; }
  if (data.objects) { view.metadata.objects = data.objects; }
  if (data.segment) { view.metadata.segment = {}; }
  return view;
}

// ---------------------------------------------------------------- run

function snapshot(element, document) {
  const root = element.querySelector(".vk-root");
  const notice = element.querySelector(".vk-notice");
  const limit = element.querySelector(".vk-limit");
  const rows = (root ? root.querySelectorAll("[data-row]") : []).map((node) => ({
    row: node.getAttribute("data-row"), classes: node.classList.list().sort(),
    tabindex: node.getAttribute("tabindex"), role: node.getAttribute("role"),
    ariaSelected: node.getAttribute("aria-selected"), text: node.textContent,
  }));
  const active = document.activeElement && document.activeElement.closest
    ? document.activeElement.closest("[data-row]") : null;
  return {
    rows: rows,
    rootClasses: root ? root.classList.list().sort() : [],
    rootRole: root ? root.getAttribute("role") : null,
    rootAttributes: root ? Object.assign({}, root.attributes) : {},
    noticeClasses: notice ? notice.classList.list().sort() : [],
    noticeText: notice ? notice.textContent : "",
    limitText: limit && !limit.classList.contains("vk-hidden") ? limit.textContent : "",
    html: root ? root.innerHTML : "",
    style: Object.assign({}, element.style.properties),
    focusedRow: active ? active.getAttribute("data-row") : null,
  };
}

async function run(job) {
  const document = new Document();
  const frames = [];
  const consoleLines = [];
  const log = [];
  const current = { step: -1, options: null, view: null };
  const record = (entry) => { entry.step = current.step; log.push(entry); };

  global.window = global;
  global.self = global;
  global.document = document;
  global.powerbi = {};
  global.requestAnimationFrame = (fn) => frames.push(fn);
  global.cancelAnimationFrame = (id) => { frames[id - 1] = null; };
  global.getComputedStyle = (element) => element.style;
  global.innerWidth = 800;
  global.innerHeight = 600;
  global.devicePixelRatio = 1;
  const capture = (level) => (...args) => consoleLines.push(level + ": " + args.map(String).join(" "));
  global.console = { log: capture("log"), info: capture("info"), warn: capture("warn"), error: capture("error"),
                     debug: capture("debug") };

  let script = job.script;
  if (job.pbiviz_json) { script = JSON.parse(fs.readFileSync(job.pbiviz_json, "utf8")).content.js; }
  vm.runInThisContext(script, { filename: "visual.js" });

  const plugin = global.powerbi.visuals && global.powerbi.visuals.plugins && global.powerbi.visuals.plugins[job.guid];
  if (!plugin) { throw new Error("the script did not register powerbi.visuals.plugins." + job.guid); }
  const exported = global[job.guid];

  const settings = Object.assign({}, job.host || {});
  const made = makeHost(settings, record, current);
  // The visual's element does not sit at the corner of the window: a tooltip position has to be
  // worked out relative to it, and only an offset shows whether it is.
  const element = document.body.appendChild(document.createElement("div"));
  element.rect = { left: 100, top: 40, width: 400, height: 300 };
  const visual = plugin.create({ element: element, host: made.host });

  const nodeFor = (step) => {
    if (step.selector) { return element.querySelector(step.selector); }
    if (step.row === undefined || step.row === null) { return element; }
    return element.querySelector("[data-row=\"" + step.row + "\"]");
  };
  const fire = (step, type, extra) => {
    const node = nodeFor(step);
    if (!node) { throw new Error("step " + current.step + ": no element for " + JSON.stringify(step)); }
    const event = makeEvent(type, Object.assign({
      clientX: step.x || 0, clientY: step.y || 0, ctrlKey: !!step.ctrl, metaKey: !!step.meta,
      shiftKey: !!step.shift, key: step.key || "",
    }, extra || {}));
    node.dispatchEvent(event);
    return { defaultPrevented: event.defaultPrevented };
  };

  const states = [];
  for (let at = 0; at < (job.steps || []).length; at += 1) {
    const step = job.steps[at];
    let result = null;
    current.step = at;
    switch (step.op) {
      case "update": {
        if (step.host) {
          Object.assign(settings, step.host);
          made.host.hostCapabilities.allowInteractions = settings.allowInteractions !== false;
        }
        if (step.data !== undefined) { current.view = makeDataView(step.data, made.host); }
        const viewport = step.viewport || { width: 400, height: 300 };
        current.options = { dataViews: current.view ? [current.view] : [], viewport: viewport, type: step.type || 2,
                            operationKind: step.operationKind || 0 };
        visual.update(current.options);
        break;
      }
      case "click": result = fire(step, "click"); break;
      case "contextmenu": result = fire(step, "contextmenu", { button: 2 }); break;
      case "hover": result = fire(step, "mousemove"); break;
      case "leave": result = fire({}, "mouseleave"); break;
      case "key": result = fire(step, "keydown"); break;
      case "event": result = fire(step, step.type); break;
      case "focus": nodeFor(step).focus(); break;
      case "frame": frames.splice(0).forEach((fn) => { if (fn) { fn(0); } }); break;
      case "host_select": {
        // The host sets the selection itself (a bookmark, or another visual taking it over).
        const ids = (step.rows || []).map((index) => {
          let builder = made.host.createSelectionIdBuilder();
          (current.view.categorical.categories || []).forEach((column) => { builder = builder.withCategory(column, index); });
          return builder.createSelectionId();
        });
        made.state.selected = ids;
        if (made.state.onSelect) { made.state.onSelect(ids.slice()); }
        break;
      }
      case "formatting_model": result = visual.getFormattingModel(); break;
      case "destroy": visual.destroy(); break;
      default: throw new Error("unknown step: " + step.op);
    }
    await new Promise((resolve) => setImmediate(resolve));    // let selection promises settle
    const state = snapshot(element, document);
    state.result = result;
    states.push(state);
  }

  return {
    ok: true,
    plugin: { name: plugin.name, displayName: plugin.displayName, "class": plugin["class"],
              apiVersion: plugin.apiVersion, custom: plugin.custom,
              exported: !!(exported && exported["default"] === plugin) },
    methods: ["update", "getFormattingModel", "destroy"].filter((name) => typeof visual[name] === "function"),
    states: states, log: log, console: consoleLines,
  };
}

const output = process.stdout;
run(JSON.parse(fs.readFileSync(process.argv[2], "utf8"))).then(
  (result) => { output.write(JSON.stringify(result)); },
  (error) => { output.write(JSON.stringify({ ok: false, error: String(error && error.stack || error) })); process.exitCode = 1; });
