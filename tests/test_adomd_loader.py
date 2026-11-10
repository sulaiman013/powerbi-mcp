"""
Tests for the shared ADOMD.NET / TOM discovery and loading (src/adomd_loader.py).

Issue #12: newer Power BI Desktop no longer ships the client DLL, so discovery honours
ADOMD_DLL_PATH and searches several locations. On top of that, a library exists once per .NET
flavour, and a build for the wrong runtime only fails when its types are used. These tests
cover: the override, recursive discovery, target-framework detection, runtime-aware ranking,
falling through to the next candidate when a load fails, and reporting which DLL was loaded.

No .NET is started: DLLs are stub files and the load step is replaced by fakes.
Run: python tests/test_adomd_loader.py
"""
import os
import sys
import tempfile
import types
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import adomd_loader as L  # noqa: E402
from adomd_loader import DLL_NAME, find_adomd_dll, ensure_adomd_on_path  # noqa: E402

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


def _clear_env():
    os.environ.pop("ADOMD_DLL_PATH", None)
    os.environ.pop("TOM_DLL_PATH", None)


NETFX_BYTES = b"MZ\x00stub\x00.NETFramework,Version=v4.5\x00more"
NETCORE_BYTES = b"MZ\x00stub\x00.NETCoreApp,Version=v3.0\x00more"
NETSTANDARD_BYTES = b"MZ\x00stub\x00.NETStandard,Version=v2.0\x00more"


def stub(folder, content=b"stub", name=DLL_NAME):
    """Create a fake DLL file and return its path."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(content)
    return path


def test_env_override_directory():
    print("\n== ADOMD_DLL_PATH points to a folder ==")
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / DLL_NAME).write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = d
        try:
            check("returns the folder", find_adomd_dll() == Path(d))
        finally:
            _clear_env()


def test_env_override_file():
    print("\n== ADOMD_DLL_PATH points to the DLL file ==")
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / DLL_NAME
        f.write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = str(f)
        try:
            check("returns the parent folder", find_adomd_dll() == Path(d))
        finally:
            _clear_env()


def test_env_override_recursive():
    print("\n== ADOMD_DLL_PATH folder with DLL in a subfolder ==")
    with tempfile.TemporaryDirectory() as d:
        sub = Path(d) / "lib" / "net6.0"
        sub.mkdir(parents=True)
        (sub / DLL_NAME).write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = d
        try:
            check("finds DLL in subfolder", find_adomd_dll() == sub)
        finally:
            _clear_env()


def test_env_missing_does_not_crash():
    print("\n== bad ADOMD_DLL_PATH falls through cleanly ==")
    with tempfile.TemporaryDirectory() as d:  # empty, no DLL
        os.environ["ADOMD_DLL_PATH"] = d
        try:
            r = find_adomd_dll()  # should fall through to system search, never crash
            check("returns Path or None", r is None or isinstance(r, Path))
            check("the empty override is not returned", r != Path(d))
        finally:
            _clear_env()


def test_ensure_on_path():
    print("\n== ensure_adomd_on_path adds the dir to sys.path ==")
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / DLL_NAME).write_bytes(b"stub")
        os.environ["ADOMD_DLL_PATH"] = d
        saved_path = os.environ.get("PATH", "")
        try:
            got = ensure_adomd_on_path()
            check("returns the dir", got == Path(d))
            check("added to sys.path", d in sys.path)
            check("added to PATH", d in os.environ.get("PATH", "").split(os.pathsep))
        finally:
            _clear_env()
            os.environ["PATH"] = saved_path
            if d in sys.path:
                sys.path.remove(d)


def test_target_framework_detection():
    print("\n== which .NET flavour a DLL was built for (read from the file, not by loading it) ==")
    with tempfile.TemporaryDirectory() as d:
        eq("marker: .NET Framework", L.dll_target_framework(stub(Path(d) / "a", NETFX_BYTES)), L.FW_NETFX)
        eq("marker: .NET Core", L.dll_target_framework(stub(Path(d) / "b", NETCORE_BYTES)), L.FW_NETCORE)
        eq("marker: .NET Standard", L.dll_target_framework(stub(Path(d) / "c", NETSTANDARD_BYTES)),
           L.FW_NETSTANDARD)
        eq("the marker beats a misleading folder name",
           L.dll_target_framework(stub(Path(d) / "lib" / "net45" / "x", NETCORE_BYTES)), L.FW_NETCORE)
        for folder, expected in (("net45", L.FW_NETFX), ("net472", L.FW_NETFX),
                                 ("netcoreapp3.0", L.FW_NETCORE), ("net6.0", L.FW_NETCORE),
                                 ("net8.0-windows", L.FW_NETCORE), ("net10.0", L.FW_NETCORE),
                                 ("netstandard2.0", L.FW_NETSTANDARD), ("Common7", L.FW_UNKNOWN)):
            eq(f"no marker, folder {folder}", L.dll_target_framework(stub(Path(d) / "pkg" / folder)),
               expected)
        eq("a missing file is 'unknown', not an exception",
           L.dll_target_framework(Path(d) / "nope" / DLL_NAME), L.FW_UNKNOWN)

    for framework, runtime, expected in (
            (L.FW_NETFX, L.NETFX, True), (L.FW_NETCORE, L.NETFX, False),
            (L.FW_NETCORE, L.CORECLR, True), (L.FW_NETFX, L.CORECLR, False),
            (L.FW_NETSTANDARD, L.NETFX, True), (L.FW_NETSTANDARD, L.CORECLR, True),
            (L.FW_UNKNOWN, L.NETFX, True), (L.FW_UNKNOWN, L.CORECLR, True),
            (L.FW_NETCORE, L.UNKNOWN_RUNTIME, True)):
        eq(f"compatible({framework} on {runtime})", L.is_compatible(framework, runtime), expected)


def test_active_runtime():
    print("\n== the runtime pythonnet will use, without loading it ==")
    saved = os.environ.pop("PYTHONNET_RUNTIME", None)
    try:
        check(".NET is not loaded by this test process", "clr" not in sys.modules)
        eq("default", L.active_runtime(), L.NETFX if os.name == "nt" else L.MONO)
        os.environ["PYTHONNET_RUNTIME"] = "coreclr"
        eq("PYTHONNET_RUNTIME=coreclr", L.active_runtime(), L.CORECLR)
        os.environ["PYTHONNET_RUNTIME"] = "netfx"
        eq("PYTHONNET_RUNTIME=netfx", L.active_runtime(), L.NETFX)
        os.environ["PYTHONNET_RUNTIME"] = "mono"
        eq("PYTHONNET_RUNTIME=mono", L.active_runtime(), L.MONO)
        check("asking did not load .NET", "clr" not in sys.modules)
    finally:
        os.environ.pop("PYTHONNET_RUNTIME", None)
        if saved is not None:
            os.environ["PYTHONNET_RUNTIME"] = saved


def test_ranking_prefers_a_loadable_build():
    print("\n== candidates are ranked for the active runtime ==")
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        core = stub(root / "pkg" / "19.84.1" / "lib" / "netcoreapp3.0", NETCORE_BYTES)
        fx = stub(root / "pkg" / "19.84.1" / "lib" / "net45", NETFX_BYTES)
        unknown = stub(root / "ssms" / "Common7" / "IDE")
        eq(".NET Framework runtime: net45 first, the netcore build last",
           L.rank_candidates([core, unknown, fx], L.NETFX), [unknown, fx, core])
        eq("CoreCLR runtime: netcore first, the net45 build last",
           L.rank_candidates([fx, unknown, core], L.CORECLR), [unknown, core, fx])
        eq("an incompatible build is kept (last), so a precise error can name it",
           L.rank_candidates([core], L.NETFX), [core])

        # A package folder holding both builds: the override picks the right one per runtime.
        os.environ["ADOMD_DLL_PATH"] = str(root / "pkg")
        try:
            eq("ADOMD_DLL_PATH at a package: net45 under .NET Framework", find_adomd_dll(L.NETFX), fx.parent)
            eq("ADOMD_DLL_PATH at a package: netcore under CoreCLR", find_adomd_dll(L.CORECLR), core.parent)
            ordered = [p for p in L.adomd_candidates(L.NETFX) if root in p.parents]
            eq("both stay candidates, compatible first", ordered, [fx, core])
        finally:
            _clear_env()

        # Newest version first within a location.
        old = stub(root / "nuget" / "19.9.0" / "lib" / "net45", NETFX_BYTES)
        new = stub(root / "nuget" / "19.84.1" / "lib" / "net45", NETFX_BYTES)
        eq("19.84.1 is newer than 19.9.0 (numeric, not alphabetical)",
           L._dlls_under(root / "nuget", DLL_NAME), [new, old])


def test_explicit_override_stays_first():
    print("\n== an explicit path is tried before anything found on the system ==")
    if os.name != "nt":
        # The system locations are Windows paths (%USERPROFILE%\.nuget, Program Files, the GAC).
        check("skipped off Windows: there is no system location to compare with", True)
        return
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        explicit = stub(root / "mine" / "lib" / "netcoreapp3.0", NETCORE_BYTES)
        profile = root / "profile"
        packages = profile / ".nuget" / "packages"
        system = stub(packages / "microsoft.analysisservices.adomdclient.retail.amd64"
                      / "19.84.1" / "lib" / "net45", NETFX_BYTES)
        saved = {k: os.environ.get(k) for k in ("USERPROFILE", "ADOMD_DLL_PATH", "TOM_DLL_PATH")}
        os.environ["USERPROFILE"] = str(profile)
        os.environ["ADOMD_DLL_PATH"] = str(root / "mine")
        try:
            ours = [p for p in L.adomd_candidates(L.NETFX) if root in p.parents]
            eq("override first even though it is the 'wrong' flavour; the system copy is the fallback",
               ours, [explicit, system])

            tom_explicit = stub(root / "mine", NETFX_BYTES, L.TOM_DLL_NAME)
            tom_system = stub(packages / "microsoft.analysisservices.retail.amd64"
                              / "19.84.1" / "lib" / "net45", NETFX_BYTES, L.TOM_DLL_NAME)
            tom_core = stub(packages / "microsoft.analysisservices.netcore.retail.amd64"
                            / "19.84.1" / "lib" / "netcoreapp3.0", NETCORE_BYTES, L.TOM_DLL_NAME)
            ours = [p for p in L.tom_candidates(L.NETFX) if root in p.parents]
            eq("TOM honours ADOMD_DLL_PATH too, then prefers the net45 package over the netcore one",
               ours, [tom_explicit, tom_system, tom_core])
            ours = [p for p in L.tom_candidates(L.CORECLR) if root in p.parents]
            eq("  and the netcore package under CoreCLR", ours, [tom_explicit, tom_core, tom_system])
            os.environ["TOM_DLL_PATH"] = str(tom_core.parent)
            ours = [p for p in L.tom_candidates(L.NETFX) if root in p.parents]
            eq("TOM_DLL_PATH is tried before ADOMD_DLL_PATH", ours[:2], [tom_core, tom_explicit])
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def test_load_falls_through():
    print("\n== a candidate that fails to load falls through to the next ==")
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        broken = stub(root / "a" / "netcoreapp3.0", NETCORE_BYTES)
        wrong_bits = stub(root / "b")
        good = stub(root / "c" / "net45", NETFX_BYTES)
        never = stub(root / "d" / "net45", NETFX_BYTES)
        tried = []

        def probe(dll):
            tried.append(dll)
            if dll == broken:
                raise ModuleNotFoundError("No module named 'Microsoft.AnalysisServices'")
            if dll == wrong_bits:
                raise Exception(
                    "BadImageFormatException: wrong architecture\r\n   at System.Reflection.Load()")
            return f"loaded:{dll.parent.name}"

        result = L.load_first("ADOMD.NET", [broken, wrong_bits, good, never], probe, L.NETFX)
        check("the first loadable candidate wins", result.available and result.dll_path == good
              and result.value == "loaded:net45", repr(result))
        eq("candidates after the winner are not touched", tried, [broken, wrong_bits, good])
        eq("each rejected candidate is recorded with its reason, without a stack trace", result.failures,
           [(broken, "ModuleNotFoundError: No module named 'Microsoft.AnalysisServices'"),
            (wrong_bits, "Exception: BadImageFormatException: wrong architecture")])
        text = result.describe()
        check("describe() names the DLL that was loaded, its flavour and the runtime",
              str(good) in text and "netfx build" in text and "netfx runtime" in text
              and "2 earlier candidate(s) failed" in text, text)
        eq("directory of the loaded DLL", result.directory, good.parent)

        def always_fails(dll):
            raise ModuleNotFoundError("No module named 'Microsoft.AnalysisServices'")
        result = L.load_first("ADOMD.NET", [broken], always_fails, L.NETFX)
        text = result.describe()
        check("nothing loadable: unavailable, every candidate and reason listed",
              not result.available and result.value is None and str(broken) in text
              and "No module named" in text, text)
        check("  with a hint that fits the cause (a .NET Core build on .NET Framework)",
              "PYTHONNET_RUNTIME=coreclr" in text and "lib/net45" in text, text)
        result = L.load_first("TOM", [good], always_fails, L.CORECLR)
        check("  and the mirror-image hint under CoreCLR", "unset PYTHONNET_RUNTIME" in result.describe())
        result = L.load_first("ADOMD.NET", [], probe, L.NETFX)
        check("no candidates at all",
              not result.available and "no copy of the library was found" in result.describe())


def test_load_and_probe_with_a_fake_clr():
    print("\n== loading by full path: search path hygiene and the 'already loaded elsewhere' trap ==")
    saved_clr = sys.modules.get("clr")
    saved_path, saved_env = list(sys.path), os.environ.get("PATH", "")
    with tempfile.TemporaryDirectory() as d:
        dll = stub(Path(d) / "lib" / "net45", NETFX_BYTES)
        other = stub(Path(d) / "elsewhere")
        state = {"location": str(dll), "added": []}

        def add_reference(path):
            state["added"].append(path)
            return types.SimpleNamespace(Location=state["location"])
        sys.modules["clr"] = types.SimpleNamespace(AddReference=add_reference)
        sys.modules["fake_adomd_namespace"] = types.SimpleNamespace(marker="ns")
        try:
            got = L._load_and_probe(dll, "fake_adomd_namespace", lambda module: module.marker)
            eq("probe receives the imported namespace", got, "ns")
            eq("the assembly is loaded by full path", state["added"], [str(dll)])
            check("its folder is on sys.path and PATH for dependency resolution",
                  str(dll.parent) in sys.path and str(dll.parent) in os.environ["PATH"].split(os.pathsep))

            # The runtime hands back an already-loaded copy from somewhere else.
            sys.path[:] = saved_path
            os.environ["PATH"] = saved_env
            state["location"] = str(other)
            try:
                L._load_and_probe(dll, "fake_adomd_namespace", lambda module: module.marker)
                check("a different loaded copy is detected", False, "no error")
            except RuntimeError as e:
                check("a different loaded copy is detected", "already loaded from" in str(e), str(e))
            check("a failed load takes its folder off sys.path and PATH again",
                  str(dll.parent) not in sys.path
                  and str(dll.parent) not in os.environ["PATH"].split(os.pathsep))

            # A folder that was already on the path (shared with a library loaded earlier) stays.
            sys.path.insert(0, str(dll.parent))
            state["location"] = str(dll)

            def failing_probe(module):
                raise TypeError("Could not load type 'System.ComponentModel.Component'")
            try:
                L._load_and_probe(dll, "fake_adomd_namespace", failing_probe)
                check("a probe failure propagates", False, "no error")
            except TypeError:
                check("a probe failure propagates", True)
            check("  without removing a path entry this call did not add", str(dll.parent) in sys.path)
        finally:
            sys.path[:] = saved_path
            os.environ["PATH"] = saved_env
            sys.modules.pop("fake_adomd_namespace", None)
            if saved_clr is None:
                sys.modules.pop("clr", None)
            else:
                sys.modules["clr"] = saved_clr


def test_load_adomd_is_cached_and_reports():
    print("\n== load_adomd / load_tom: one attempt per process, result reported ==")
    saved = (L.adomd_candidates, L._probe_adomd, L.tom_candidates, L._probe_tom, dict(L._LOADED))
    calls = []
    with tempfile.TemporaryDirectory() as d:
        bad = stub(Path(d) / "netcoreapp3.0", NETCORE_BYTES)
        good = stub(Path(d) / "net45", NETFX_BYTES)

        def probe(dll):
            calls.append(dll)
            if dll == bad:
                raise ModuleNotFoundError("No module named 'Microsoft.AnalysisServices'")
            return L.AdomdLibrary(connection="Conn", command="Cmd", schema_guid="Guid")
        try:
            L._LOADED.clear()
            L.adomd_candidates = lambda runtime=None: [bad, good]
            L._probe_adomd = probe
            first = L.load_adomd()
            check("falls through to the loadable build", first.available and first.dll_path == good
                  and first.value.connection == "Conn", first.describe())
            second = L.load_adomd()
            check("cached: the second call does not load again", second is first and calls == [bad, good])
            L.load_adomd(force=True)
            eq("force=True tries again", len(calls), 4)

            L.tom_candidates = lambda runtime=None: []
            L._probe_tom = probe
            tom = L.load_tom()
            check("TOM not found is a result, not an exception", not tom.available and tom.searched == 0)
        finally:
            L.adomd_candidates, L._probe_adomd, L.tom_candidates, L._probe_tom = saved[:4]
            L._LOADED.clear()
            L._LOADED.update(saved[4])


if __name__ == "__main__":
    print("=" * 70)
    print("  ADOMD LOADER TESTS (issue #12, runtime-aware loading)")
    print("=" * 70)
    test_env_override_directory()
    test_env_override_file()
    test_env_override_recursive()
    test_env_missing_does_not_crash()
    test_ensure_on_path()
    test_target_framework_detection()
    test_active_runtime()
    test_ranking_prefers_a_loadable_build()
    test_explicit_override_stays_first()
    test_load_falls_through()
    test_load_and_probe_with_a_fake_clr()
    test_load_adomd_is_cached_and_reports()
    print("\n" + "=" * 70)
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED: {', '.join(_failures)}")
        sys.exit(1)
    print("  ALL ADOMD LOADER CHECKS PASSED")
    print("=" * 70)
