// From the host's data view to rows, and from its objects to settings.
//
// The package maps every grouping role to categories and every measure role to values of one
// categorical data view, because that is the mapping that carries highlights. A row is one
// category index (one combination of the grouping fields); with no grouping field there is
// one row holding the measures.

KIT.columnOf = function (source, isMeasure) {
  var type = source.type || {};
  return {
    displayName: source.displayName,
    queryName: source.queryName,
    format: source.format || "",
    isMeasure: !!isMeasure,
    type: type.dateTime ? "date" : (type.numeric || type.integer) ? "number" : type.bool ? "bool" : "text"
  };
};

KIT.BLANK = "(Blank)";

KIT.buildModel = function (dataView, config, host) {
  var model = { rows: [], columns: {}, hasHighlights: false, bound: false, more: false, truncated: false };
  var locale = host && host.locale;
  var categorical = dataView && dataView.categorical;
  var categories = (categorical && categorical.categories) || [];
  var values = (categorical && categorical.values) || [];
  var sources = [], count, i, k;

  config.roles.forEach(function (role) { model.columns[role.name] = []; });
  if (!categorical) { return model; }

  function add(data, isMeasure) {
    var column = KIT.columnOf(data.source, isMeasure);
    var roles = Object.keys(data.source.roles || {}).filter(function (name) {
      return data.source.roles[name] && Object.prototype.hasOwnProperty.call(model.columns, name);
    });
    roles.forEach(function (name) { model.columns[name].push(column); });
    sources.push({ data: data, column: column, roles: roles });
    if (isMeasure && data.highlights) { model.hasHighlights = true; }
  }
  for (k = 0; k < categories.length; k += 1) { add(categories[k], false); }
  for (k = 0; k < values.length; k += 1) { add(values[k], true); }

  model.bound = sources.length > 0;
  model.more = !!(dataView.metadata && dataView.metadata.segment);
  count = categories.length ? categories[0].values.length : (values.length ? values[0].values.length : 0);
  // The host cuts the rows to the limit and says nothing. Below one window the package asks for
  // one row more than the limit, so a row beyond it proves the cut; in windows the host's own
  // "more" flag does, once the visual has all it wants or does not fetch at all.
  if (categories.length) {
    model.truncated = count > config.rows ||
      (model.more && (count >= config.rows || !config.features.fetchMore));
    count = Math.min(count, config.rows);
  }

  function cell(source, index) {
    var value = source.data.values[index];
    var highlight = source.column.isMeasure && source.data.highlights ? source.data.highlights[index] : null;
    var empty = value === null || value === undefined || value === "";
    if (source.column.type === "date" && !empty && !(value instanceof Date)) {
      value = new Date(value);
    }
    return {
      value: value === undefined ? null : value,
      // A category without a value reads "(Blank)", as it does in the built-in visuals.
      text: empty && !source.column.isMeasure ? KIT.BLANK : KIT.format(value, source.column.format, { locale: locale }),
      highlight: highlight === undefined ? null : highlight,
      column: source.column
    };
  }

  // The identity of a row is built from every category column, so it still names one data
  // point when a drilled hierarchy shows several levels at once.
  function identity(index) {
    var builder, c;
    if (!categories.length || !host || !host.createSelectionIdBuilder) { return null; }
    builder = host.createSelectionIdBuilder();
    for (c = 0; c < categories.length; c += 1) { builder = builder.withCategory(categories[c], index); }
    return builder.createSelectionId();
  }

  for (i = 0; i < count; i += 1) {
    model.rows.push((function (index) {
      var row = { index: index, values: {}, text: {}, highlights: {}, cells: {},
                  selected: false, highlighted: false, dimmed: false, identity: identity(index) };
      config.roles.forEach(function (role) { row.cells[role.name] = []; });
      sources.forEach(function (source) {
        var item = cell(source, index);
        source.roles.forEach(function (name) { row.cells[name].push(item); });
        if (item.highlight !== null) { row.highlighted = true; }
      });
      config.roles.forEach(function (role) {
        // Power BI Desktop hands a drill role as ONE column, whatever the drill state: the level
        // shown, or the expanded levels joined into one value ("Mumbai Freelancers"). Should a
        // host hand one column per level instead, the last one is the level shown.
        var cells = row.cells[role.name];
        var pick = cells.length ? cells[role.drill ? cells.length - 1 : 0] : null;
        row.values[role.name] = pick ? pick.value : null;
        row.text[role.name] = pick ? pick.text : "";
        row.highlights[role.name] = pick ? pick.highlight : null;
      });
      return row;
    }(i)));
  }
  return model;
};

// The lines a default tooltip shows for a row: every bound field, in role order.
KIT.tooltipItems = function (row, config, hasHighlights, locale) {
  var items = [];
  config.roles.forEach(function (role) {
    row.cells[role.name].forEach(function (cell) {
      if (items.some(function (item) { return item.source === cell; })) { return; }
      items.push({ displayName: cell.column.displayName, value: cell.text, source: cell });
      if (hasHighlights && cell.column.isMeasure && cell.highlight !== null) {
        items.push({ displayName: "Highlighted",
                     value: KIT.format(cell.highlight, cell.column.format, { locale: locale }), source: null });
      }
    });
  });
  return items.map(function (item) { return { displayName: item.displayName, value: item.value }; });
};

// ---------------------------------------------------------------- format pane

// A colour as CSS takes it and nothing else: settings are written into markup by render code.
KIT.safeColor = function (value) {
  var text = typeof value === "string" ? value.trim() : "";
  return /^#[0-9a-f]{3,8}$/i.test(text) || /^(rgb|hsl)a?\([0-9.,%\s\/]+\)$/i.test(text) ||
    /^[a-z]{3,24}$/i.test(text) ? text : undefined;
};

// The value of every format-pane property: what the report stores, or the default. What comes
// out is what the pane shows, so a stored value the property cannot hold falls back as well.
KIT.readSettings = function (cards, objects, colors) {
  var settings = {};
  cards.forEach(function (card) {
    var stored = (objects && objects[card.name]) || {};
    var group = settings[card.name] = {};
    card.properties.forEach(function (property) {
      var value = stored[property.name];
      if (property.type === "color") {
        value = KIT.safeColor(value && value.solid ? value.solid.color : undefined);
        if (colors && colors.isHighContrast) {
          value = colors.foreground;
        } else if (value === undefined) {
          value = property.value === null && colors ? colors.accent : property.value;
        }
      } else if (value === undefined || value === null) {
        value = property.value;
      } else if (property.type === "number" || property.type === "font_size") {
        value = typeof value === "boolean" ? NaN : Number(value);
        if (isNaN(value)) { value = property.value; }
        if (property.min !== undefined && value < property.min) { value = property.min; }
        if (property.max !== undefined && value > property.max) { value = property.max; }
      } else if (property.type === "toggle") {
        value = value === "false" ? false : !!value;
      } else if (property.type === "dropdown") {
        value = String(value);
        if (!property.items.some(function (item) { return item.value === value; })) { value = property.value; }
      } else {
        value = String(value);
      }
      group[property.name] = value;
    });
  });
  return settings;
};

// The model the format pane is drawn from (FormattingModel of the visuals API, 5.1 and later):
// one card per object, one slice per property, a toggle named "show" as the card's own switch.
KIT.formattingModel = function (cards, settings) {
  var CONTROL = { toggle: "ToggleSwitch", color: "ColorPicker", number: "NumUpDown",
                  font_size: "NumUpDown", text: "TextInput", dropdown: "Dropdown" };
  return {
    cards: cards.map(function (card) {
      var out = { displayName: card.displayName, uid: card.name + "-card",
                  groups: [{ displayName: undefined, uid: card.name + "-group", slices: [] }],
                  revertToDefaultDescriptors: [] };
      card.properties.forEach(function (property) {
        var value = settings[card.name][property.name];
        var properties = { descriptor: { objectName: card.name, propertyName: property.name }, value: value };
        var slice = { uid: card.name + "-" + property.name, displayName: property.displayName,
                      control: { type: CONTROL[property.type], properties: properties } };
        if (property.description) { slice.description = property.description; }
        if (property.type === "color") {
          properties.value = { value: value };
        } else if (property.type === "number" || property.type === "font_size") {
          properties.options = {};
          if (property.min !== undefined) { properties.options.minValue = { type: 0, value: property.min }; }
          if (property.max !== undefined) { properties.options.maxValue = { type: 1, value: property.max }; }
        } else if (property.type === "text") {
          properties.placeholder = property.placeholder || "";
        } else if (property.type === "dropdown") {
          properties.items = property.items;
          properties.value = property.items.filter(function (item) { return item.value === value; })[0] ||
            property.items[0];
        }
        out.revertToDefaultDescriptors.push({ objectName: card.name, propertyName: property.name });
        if (property.type === "toggle" && property.name === "show") {
          slice.suppressDisplayName = true;
          out.topLevelToggle = slice;
        } else {
          out.groups[0].slices.push(slice);
        }
      });
      return out;
    })
  };
};
