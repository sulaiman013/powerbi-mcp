// Values as text, the way a Power BI format string describes them.
//
// The host hands a visual raw values and each column's format string; it formats nothing.
//
// Numbers, supported:
//   custom patterns   0 and # digit placeholders, the decimal point, a comma between digits
//                     (thousands grouping) or right before the decimal point or the end of the
//                     digits (divide by 1000 each), % and the per-mille sign, E+0 / E-0 / e+0
//                     exponents, text in quotes or after a backslash, any other character as
//                     written, and the positive;negative;zero sections
//   standard formats  N, F, P, D, E and G with an optional number of digits ("N2", "P1")
//   named formats     General Number, Fixed, Standard, Percent, Scientific, Currency,
//                     Yes/No, True/False, On/Off
//   no format string  the General format: no grouping, the digits the number has (up to 15
//                     significant ones), an exponent only for very large or very small numbers
// Numbers, not supported:
//   a currency symbol for C and Currency (the model's currency is not known here: they print
//   grouped with two decimals and no symbol); the X, B and R standard formats
// Dates: the standard letters d D t T M Y G g, the named date and time formats, and the tokens
//   yyyy yy MMMM MMM MM M dddd ddd dd d HH H hh h mm m ss s tt with quoted or escaped text.
//   Fractions of a second, time zones, eras and quarters are left as written.
// A pattern that holds no digit placeholder, or a letter format this does not know, can not show
// the number: the number is then printed in the General format, never the pattern's own text.

KIT.escape = function (text) {
  return String(text === null || text === undefined ? "" : text)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
};

KIT.format = (function () {
  var UNITS = [[1e12, "T"], [1e9, "bn"], [1e6, "M"], [1e3, "K"]];
  var UNIT_NAMES = { thousands: 1e3, millions: 1e6, billions: 1e9, trillions: 1e12 };
  var NAMED = { "general number": "G", "general": "G", "fixed": "0.00", "standard": "#,0.00",
                "percent": "0.00%", "scientific": "0.00E+00", "currency": "#,0.00" };
  var TRUTH = { "yes/no": ["Yes", "No"], "true/false": ["True", "False"], "on/off": ["On", "Off"] };
  var DATE_NAMES = {
    "general date": "G", "short date": "d", "long date": "D", "medium date": "D",
    "short time": "t", "long time": "T", "medium time": "t"
  };
  var PER_MILLE = String.fromCharCode(0x2030);
  var parsed = {};

  function localized(value, method, options, locale) {
    try {
      return value[method](locale || undefined, options);
    } catch (error) {
      return value[method]("en-US", options);
    }
  }

  function pad(number, width) {
    var text = String(number);
    while (text.length < width) { text = "0" + text; }
    return text;
  }

  function splitSections(format) {
    var parts = [], current = "", quote = "", i = 0, ch;
    while (i < format.length) {
      ch = format.charAt(i);
      if (quote) {
        current += ch;
        if (ch === quote) { quote = ""; }
        i += 1;
      } else if (ch === "\\") {
        current += format.substr(i, 2);
        i += 2;
      } else if (ch === ";") {
        parts.push(current);
        current = "";
        i += 1;
      } else {
        if (ch === "\"" || ch === "'") { quote = ch; }
        current += ch;
        i += 1;
      }
    }
    parts.push(current);
    return parts;
  }

  function blank(text) {
    return { prefix: "", suffix: "", minInt: 0, minFrac: 0, maxFrac: 0, group: false, percent: false,
             permille: false, scale: 1, digits: false, exponent: null, general: false, empty: text === "" };
  }

  function parseSection(text) {
    var out = blank(text);
    var i = 0, commas = 0, point = false, ch, end, sign, zeros;
    function literal(piece) {
      if (out.digits) { out.suffix += piece; } else { out.prefix += piece; }
    }
    while (i < text.length) {
      ch = text.charAt(i);
      if (ch === "\\") {
        literal(text.charAt(i + 1));
        i += 2;
      } else if (ch === "\"" || ch === "'") {
        end = text.indexOf(ch, i + 1);
        if (end < 0) { end = text.length; }
        literal(text.slice(i + 1, end));
        i = end + 1;
      } else if (ch === "0" || ch === "#") {
        if (commas) { out.group = true; commas = 0; }
        out.digits = true;
        if (point) {
          out.maxFrac += 1;
          if (ch === "0") { out.minFrac = out.maxFrac; }
        } else if (ch === "0") {
          out.minInt += 1;
        }
        i += 1;
      } else if (ch === "." && !point) {
        if (commas) { out.scale = Math.pow(1000, commas); commas = 0; }
        point = true;
        out.digits = true;
        i += 1;
      } else if (ch === ",") {
        // A comma is grouping or scaling, never text: after the decimal point it means nothing.
        if (out.digits && !point) { commas += 1; }
        i += 1;
      } else if ((ch === "E" || ch === "e") && out.digits && !out.exponent &&
                 /^[+-]?0/.test(text.slice(i + 1))) {
        sign = text.charAt(i + 1);
        end = i + (sign === "+" || sign === "-" ? 2 : 1);
        zeros = 0;
        while (text.charAt(end + zeros) === "0") { zeros += 1; }
        out.exponent = { letter: ch, plus: sign === "+", digits: zeros };
        i = end + zeros;
      } else {
        if (ch === "%") { out.percent = true; }
        if (ch === PER_MILLE) { out.permille = true; }
        literal(ch);
        i += 1;
      }
    }
    if (commas) { out.scale = Math.pow(1000, commas); }
    return out;
  }

  // "N2", "P1", "F0", "D3", "E2", "G": the one-letter formats. A letter not known here is General.
  function standard(format) {
    var kind = format.charAt(0).toUpperCase();
    var digits = format.length > 1 ? parseInt(format.slice(1), 10) : null;
    var out = blank(format);
    out.digits = true;
    out.minInt = 1;
    if (kind === "N" || kind === "C" || kind === "F" || kind === "P") {
      out.minFrac = out.maxFrac = digits === null ? 2 : digits;
      out.group = kind !== "F";
      if (kind === "P") { out.percent = true; out.suffix = "%"; }
    } else if (kind === "D") {
      out.minInt = digits || 1;
    } else if (kind === "E") {
      out.minFrac = out.maxFrac = digits === null ? 6 : digits;
      out.exponent = { letter: format.charAt(0), plus: true, digits: 3 };
    } else {
      out.general = true;
      out.maxFrac = 15;
    }
    return out;
  }

  function sectionsFor(format) {
    var pattern = format, sections;
    if (!Object.prototype.hasOwnProperty.call(parsed, format)) {
      if (Object.prototype.hasOwnProperty.call(NAMED, format.toLowerCase())) { pattern = NAMED[format.toLowerCase()]; }
      if (/^[A-Za-z][0-9]{0,2}$/.test(pattern)) {
        sections = [standard(pattern)];
      } else {
        sections = splitSections(pattern).map(parseSection);
        // No digit placeholder: the pattern cannot show a number, so its text is not printed.
        if (!sections[0].digits) { sections = [standard("G")]; }
      }
      parsed[format] = sections;
    }
    return parsed[format];
  }

  function unitFor(magnitude, wanted) {
    var k;
    if (Object.prototype.hasOwnProperty.call(UNIT_NAMES, wanted)) {
      for (k = 0; k < UNITS.length; k += 1) {
        if (UNITS[k][0] === UNIT_NAMES[wanted]) { return UNITS[k]; }
      }
    }
    if (wanted === "auto") {
      for (k = 0; k < UNITS.length; k += 1) {
        if (magnitude >= UNITS[k][0]) { return UNITS[k]; }
      }
    }
    return null;
  }

  // The General format: the number as it is. An exponent only where .NET uses one (15 digits
  // or more before the point, or more than four zeros after it).
  function general(value, locale) {
    var magnitude = Math.abs(value), parts, exponent;
    if (magnitude !== 0 && (magnitude >= 1e15 || magnitude < 1e-4)) {
      parts = value.toExponential(14).split("e");
      exponent = parseInt(parts[1], 10);
      return localized(Number(parts[0]), "toLocaleString", { useGrouping: false, maximumSignificantDigits: 15 }, locale) +
        "E" + (exponent < 0 ? "-" : "+") + pad(Math.abs(exponent), 2);
    }
    return localized(value, "toLocaleString", { useGrouping: false, maximumSignificantDigits: 15 }, locale);
  }

  function scientific(magnitude, section, locale) {
    var digits = Math.max(1, section.minInt), exponent = 0, mantissa = 0;
    if (magnitude !== 0) {
      exponent = parseInt(magnitude.toExponential().split("e")[1], 10) - (digits - 1);
      mantissa = magnitude / Math.pow(10, exponent);
      if (Number(mantissa.toFixed(Math.min(section.maxFrac, 20))) >= Math.pow(10, digits)) {
        mantissa /= 10;                  // rounding carried into one more digit: 9.999 -> 10.00
        exponent += 1;
      }
    }
    return localized(mantissa, "toLocaleString", {
      minimumIntegerDigits: Math.min(digits, 21),
      minimumFractionDigits: Math.min(section.minFrac, 20),
      maximumFractionDigits: Math.min(Math.max(section.minFrac, section.maxFrac), 20),
      useGrouping: false
    }, locale) + section.exponent.letter + (exponent < 0 ? "-" : section.exponent.plus ? "+" : "") +
      pad(Math.abs(exponent), section.exponent.digits);
  }

  function formatNumber(value, format, options, locale) {
    var sections = sectionsFor(format || "G");
    var section = sections[0], negative = value < 0, magnitude = Math.abs(value);
    var wantsUnits = !!(options && options.units);
    var decimals = options && options.decimals !== undefined && options.decimals !== null ? options.decimals : null;
    var minFrac, maxFrac, unit = null, shown, body;
    if (section.general && !wantsUnits && decimals === null) { return general(value, locale); }
    if (negative && sections.length > 1 && !sections[1].empty) { section = sections[1]; }
    if (section.percent) { magnitude *= 100; }
    if (section.permille) { magnitude *= 1000; }
    magnitude /= section.scale;
    minFrac = section.minFrac;
    maxFrac = section.general ? 2 : section.maxFrac;
    if (wantsUnits && !section.percent && !section.permille && !section.exponent) {
      unit = unitFor(magnitude, options.units);
    }
    if (unit) {
      magnitude /= unit[0];
      minFrac = 0;
      maxFrac = decimals === null ? 1 : decimals;
    } else if (decimals !== null) {
      minFrac = maxFrac = decimals;
    }
    shown = Number(magnitude.toFixed(Math.min(maxFrac, 20)));
    if (shown === 0) {
      // A value that rounds to zero takes the zero section, or else the positive one.
      negative = false;
      section = sections.length > 2 && !sections[2].empty ? sections[2] : sections[0];
    }
    if (!section.digits) { return section.prefix + section.suffix; }
    if (section.exponent && !unit) {
      body = scientific(magnitude, section, locale);
    } else {
      body = localized(magnitude, "toLocaleString", {
        minimumIntegerDigits: Math.max(1, Math.min(section.minInt, 21)),
        minimumFractionDigits: Math.min(minFrac, 20),
        maximumFractionDigits: Math.min(Math.max(minFrac, maxFrac), 20),
        useGrouping: section.group
      }, locale);
    }
    // A dedicated negative section carries its own sign (or brackets).
    return (negative && section === sections[0] ? "-" : "") + section.prefix + body +
      (unit ? unit[1] : "") + section.suffix;
  }

  function formatDate(value, format, locale) {
    var named = DATE_NAMES[String(format || "").toLowerCase()] || format || "G";
    var midnight = value.getHours() + value.getMinutes() + value.getSeconds() === 0;
    if (named.charAt(0) === "%") { named = named.slice(1); }
    if (named.length === 1) {
      switch (named) {
        case "d": return localized(value, "toLocaleDateString", undefined, locale);
        case "D": return localized(value, "toLocaleDateString",
                                   { year: "numeric", month: "long", day: "numeric" }, locale);
        case "t": return localized(value, "toLocaleTimeString", { hour: "numeric", minute: "2-digit" }, locale);
        case "T": return localized(value, "toLocaleTimeString", undefined, locale);
        case "M": case "m": return localized(value, "toLocaleDateString", { month: "long", day: "numeric" }, locale);
        case "Y": case "y": return localized(value, "toLocaleDateString", { year: "numeric", month: "long" }, locale);
        default: return midnight ? localized(value, "toLocaleDateString", undefined, locale)
                                 : localized(value, "toLocaleString", undefined, locale);
      }
    }
    return named.replace(/yyyy|yy|MMMM|MMM|MM|M|dddd|ddd|dd|d|HH|H|hh|h|mm|m|ss|s|tt|\\.|"[^"]*"|'[^']*'/g,
      function (token) {
        var hours = value.getHours();
        switch (token) {
          case "yyyy": return String(value.getFullYear());
          case "yy": return pad(value.getFullYear() % 100, 2);
          case "MMMM": return localized(value, "toLocaleDateString", { month: "long" }, locale);
          case "MMM": return localized(value, "toLocaleDateString", { month: "short" }, locale);
          case "MM": return pad(value.getMonth() + 1, 2);
          case "M": return String(value.getMonth() + 1);
          case "dddd": return localized(value, "toLocaleDateString", { weekday: "long" }, locale);
          case "ddd": return localized(value, "toLocaleDateString", { weekday: "short" }, locale);
          case "dd": return pad(value.getDate(), 2);
          case "d": return String(value.getDate());
          case "HH": return pad(hours, 2);
          case "H": return String(hours);
          case "hh": return pad(hours % 12 || 12, 2);
          case "h": return String(hours % 12 || 12);
          case "mm": return pad(value.getMinutes(), 2);
          case "m": return String(value.getMinutes());
          case "ss": return pad(value.getSeconds(), 2);
          case "s": return String(value.getSeconds());
          case "tt": return hours < 12 ? "AM" : "PM";
          default: return token.charAt(0) === "\\" ? token.charAt(1) : token.slice(1, -1);
        }
      });
  }

  // format(value, formatString, {units, decimals, blank, locale})
  //   units: "auto" | "thousands" | "millions" | "billions" | "trillions" (omit for none)
  return function (value, format, options) {
    var locale = options && options.locale;
    var truth = TRUTH[String(format || "").toLowerCase()];
    if (value === null || value === undefined || value === "") {
      return options && options.blank !== undefined ? options.blank : "";
    }
    if (value instanceof Date) { return isNaN(value.getTime()) ? "" : formatDate(value, format, locale); }
    if (typeof value === "boolean") { return truth ? truth[value ? 0 : 1] : (value ? "True" : "False"); }
    if (typeof value !== "number") { return String(value); }
    if (!isFinite(value)) { return isNaN(value) ? "NaN" : (value > 0 ? "Infinity" : "-Infinity"); }
    if (truth) { return truth[value !== 0 ? 0 : 1]; }
    return formatNumber(value, String(format || ""), options, locale);
  };
}());
