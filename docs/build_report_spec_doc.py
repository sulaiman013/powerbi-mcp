"""Write docs/REPORT_SPEC.md from the report_spec module docstring, so the reference cannot
drift from the code that implements it."""
import ast
import io
import sys

repo = sys.argv[1] if len(sys.argv) > 1 else __import__("os").path.dirname(
    __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
doc = ast.get_docstring(ast.parse(io.open(repo + "/src/report_spec.py", encoding="utf-8").read()))
head, rest = doc.split("THE SPEC", 1)
spec_part, rest = rest.split("EXAMPLE", 1)
example_part, applying_part = rest.split("APPLYING", 1)


def block(text, lang=""):
    lines = text.strip("\n").split("\n")
    indent = min((len(ln) - len(ln.lstrip()) for ln in lines if ln.strip()), default=0)
    return "```" + lang + "\n" + "\n".join(ln[indent:] for ln in lines) + "\n```\n"


out = []
out.append("# Report as code: the report spec\n")
out.append(
    "`pbir_apply_spec` builds or updates a PBIP report from one declarative JSON document. It is the\n"
    "same idea as building a report with a script, without writing the script: the spec says what\n"
    "the report contains, and the server writes the PBIR files Power BI Desktop reads.\n")
out.append(
    "This page is generated from the docstring of [`src/report_spec.py`](../src/report_spec.py)\n"
    "(`python docs/build_report_spec_doc.py`), so it describes exactly what the code accepts.\n")
out.append("## What applying a spec guarantees\n")
out.append(
    "- **Idempotent.** Every page, element and bookmark has a stable `name`. Applying the same spec\n"
    "  twice rewrites nothing; changing one element rewrites that element's file.\n"
    "- **All or nothing.** The whole spec is validated first: names, layout (elements inside the\n"
    "  page), navigation targets, and every field against the model. If anything is wrong, no file\n"
    "  is written. Valid changes land in one transaction.\n"
    "- **Non-destructive.** Pages, visuals and bookmarks the spec did not create are never rewritten\n"
    "  or removed. `prune` removes only what an earlier apply created. Inside a file the spec writes,\n"
    "  a filter, an interaction or a phone position made in Power BI Desktop is kept; the spec's own\n"
    "  are told apart by the names recorded in the manifest.\n"
    "- **Faithful files.** New files are UTF-8 without a byte order mark and use LF; existing files\n"
    "  keep their own encoding and line endings.\n")
out.append("## Workflow\n")
out.append(block(
    "pbip_load_project        load the .pbip (the report must be in PBIR format)\n"
    "pbir_apply_spec          dry_run=true first: returns the files it would write, or the errors\n"
    "pbir_apply_spec          apply\n"
    "audit_report_design      structure, design and governance findings for the result\n"
    "bridge_reload            hot-reload the open Desktop (never over unsaved changes)\n"
    "bridge_screenshot        look at the rendered pages"))
out.append("## The spec\n")
out.append(block(spec_part))
out.append("## Example\n")
out.append(block(example_part, "json"))
out.append("## Applying\n")
out.append(block(applying_part))
out.append("## Measures that drive HTML, SVG and script elements\n")
out.append(
    "The `html`, `image` and `deneb` elements display what a measure returns. Generate those measures\n"
    "with the matching tools, save them with `pbip_add_measures` (use `mode: \"upsert\"` so a rebuild\n"
    "replaces them in place), then reference them from the spec:\n\n"
    "| Element | Measure from |\n|---|---|\n"
    "| `html` (static HTML: KPI cards, headline sentences, bar lists, tables; either edition) | "
    "`generate_html_measure` |\n"
    "| `html` (a JavaScript app; needs `\"edition\": \"regular\"`, the default) | `build_js_app_measure`, "
    "fed by `generate_payload_measure` |\n"
    "| `image` with `measure` (an SVG data URI) | `generate_svg_measure` |\n")
out.append("## Limits\n")
out.append(
    "- The JSON shapes were taken from report files that open in Power BI Desktop, and the tests\n"
    "  check the emitted structure against them. Generated reports have not been opened in Desktop\n"
    "  by the test suite: reload and look (`bridge_reload`, `bridge_screenshot`) after a build.\n"
    "- Filters, drillthrough and tooltip pages, interactions, the phone layout and bookmark groups\n"
    "  follow the public PBIR schemas and were loaded into Power BI Desktop. Filters were seen to\n"
    "  change the numbers, and the filter pane showed their names, locks and hidden marks. A tooltip\n"
    "  page was seen to appear, filtered to the row, for a visual that names it. What a click does\n"
    "  otherwise (drilling through, one visual filtering another, the phone view) was not observed:\n"
    "  Desktop accepted the files and drew the pages.\n"
    "- Not covered by the spec: data state inside bookmarks (a bookmark shows and hides elements; it\n"
    "  does not set slicers or filters), drillthrough on a measure or a summarized column, relative\n"
    "  time filters and calendar-aligned relative dates, phone-specific formatting, and the look of\n"
    "  the filter pane (set that in the theme).\n"
    "- A filter pins values into the report files. Anyone who can read the report definition can\n"
    "  read them, and a filter is not a security boundary: use row-level security for that.\n"
    "- Images are registered, not created: the file must already be in\n"
    "  `StaticResources/RegisteredResources`.\n"
    "- Certification differs by visual. Deneb's AppSource edition and HTML Content Secure are\n"
    "  certified. HTML Content Regular (the edition that runs scripts, which a JavaScript app in a\n"
    "  measure needs) and private `.pbiviz` visuals, built here or not, are not: a tenant that only\n"
    "  allows certified visuals will not render those. `audit_report_design` lists them.\n")
io.open(repo + "/docs/REPORT_SPEC.md", "w", encoding="utf-8", newline="\n").write("\n".join(out))
print("docs/REPORT_SPEC.md written:", sum(len(x) for x in out), "chars")
