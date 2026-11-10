"""
Discovery and loading of the Analysis Services client libraries (ADOMD.NET and TOM/AMO).

Two things go wrong when this is done naively:

* The same library ships once per .NET flavour. A NuGet cache commonly holds a ``netcoreapp3.0``
  build next to (or instead of) the ``net45`` one, and pythonnet runs .NET Framework on Windows
  unless told otherwise. A build for the other runtime fails only when its types are first used
  (ADOMD: the namespace cannot be imported; TOM: the namespace imports, ``Server()`` raises
  ``TypeLoadException``), so "the DLL exists" says nothing about "the DLL works".
* The first folder that contains the file is not necessarily the right one.

So candidates are ranked for the active runtime (builds it can load first), each one is actually
loaded and probed, and a failed candidate falls through to the next. The result records which
DLL was loaded and why the others were rejected.

``ADOMD_DLL_PATH`` / ``TOM_DLL_PATH`` (a DLL path or a folder) are always tried first.
Importing this module loads no .NET; ``load_adomd()`` / ``load_tom()`` do.
"""
import importlib
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

DLL_NAME = "Microsoft.AnalysisServices.AdomdClient.dll"
TOM_DLL_NAME = "Microsoft.AnalysisServices.Tabular.dll"

# pythonnet runtime kinds
NETFX, CORECLR, MONO, UNKNOWN_RUNTIME = "netfx", "coreclr", "mono", "unknown"
# target frameworks an assembly can be built for
FW_NETFX, FW_NETCORE, FW_NETSTANDARD, FW_UNKNOWN = "netfx", "netcore", "netstandard", "unknown"

NOT_FOUND_HELP = (
    "ADOMD.NET client library (Microsoft.AnalysisServices.AdomdClient.dll) was not found. "
    "Newer Power BI Desktop (MSI) builds no longer ship it. Fix by either: installing SQL "
    "Server Management Studio (SSMS); installing the Microsoft.AnalysisServices.AdomdClient "
    "NuGet package; or setting the ADOMD_DLL_PATH environment variable to the folder (or full "
    "path) containing the DLL."
)


# ---------------------------------------------------------------- runtime and framework

def active_runtime() -> str:
    """The .NET runtime pythonnet is using, or will use on first ``import clr``.

    Does not load .NET: an already loaded runtime is asked for its kind, otherwise the answer
    follows pythonnet's own default (``PYTHONNET_RUNTIME``, else .NET Framework on Windows).
    """
    kind = ""
    try:
        import pythonnet
        info = pythonnet.get_runtime_info()
        kind = str(getattr(info, "kind", "") or "") if info is not None else ""
    except Exception:
        # pythonnet is absent in the offline/Docker install; nothing can be loaded there anyway.
        kind = ""
    if not kind:
        kind = os.environ.get("PYTHONNET_RUNTIME", "") or ("netfx" if os.name == "nt" else "mono")
    kind = kind.lower()
    if "core" in kind:
        return CORECLR
    if "framework" in kind or kind == "netfx":
        return NETFX
    if "mono" in kind:
        return MONO
    return UNKNOWN_RUNTIME


_FRAMEWORK_MARKER = re.compile(rb"\.NET(Framework|CoreApp|Standard),Version=v")
_MARKER_KIND = {b"Framework": FW_NETFX, b"CoreApp": FW_NETCORE, b"Standard": FW_NETSTANDARD}
_FRAMEWORK_CACHE: Dict[str, str] = {}


def dll_target_framework(dll: Path) -> str:
    """Which .NET flavour an assembly was built for, without loading it.

    Compilers embed a TargetFrameworkAttribute whose value (".NETFramework,Version=v4.5",
    ".NETCoreApp,Version=v3.0") sits in the file as plain bytes. When it is absent (older or
    framework-owned assemblies) the NuGet folder name decides; otherwise the answer is unknown.
    """
    key = str(dll)
    if key in _FRAMEWORK_CACHE:
        return _FRAMEWORK_CACHE[key]
    result = FW_UNKNOWN
    try:
        match = _FRAMEWORK_MARKER.search(Path(dll).read_bytes())
    except OSError:
        match = None
    if match:
        result = _MARKER_KIND[match.group(1)]
    else:
        for part in reversed([p.lower() for p in Path(dll).parts[:-1]]):
            if part.startswith("netstandard"):
                result = FW_NETSTANDARD
            elif part.startswith("netcoreapp") or re.fullmatch(r"net(?:[5-9]|\d\d)\.\d+(-.+)?", part):
                result = FW_NETCORE
            elif re.fullmatch(r"net[1-4]\d{1,2}", part):
                result = FW_NETFX
            else:
                continue
            break
    _FRAMEWORK_CACHE[key] = result
    return result


def is_compatible(framework: str, runtime: str) -> bool:
    """Can ``runtime`` be expected to load an assembly built for ``framework``?"""
    if framework in (FW_UNKNOWN, FW_NETSTANDARD) or runtime not in (NETFX, CORECLR):
        return True
    return (framework == FW_NETFX) == (runtime == NETFX)


def rank_candidates(dlls: Sequence[Path], runtime: Optional[str] = None) -> List[Path]:
    """``dlls`` reordered so builds the runtime can load come first; order is otherwise kept.

    Incompatible builds stay in the list, last: the probe has the final word, and a machine that
    only has the "wrong" build should still get a precise error naming it.
    """
    rt = runtime or active_runtime()
    return sorted(dlls, key=lambda d: 0 if is_compatible(dll_target_framework(d), rt) else 1)


# ---------------------------------------------------------------- discovery

def _natural_key(path: Path) -> List[Any]:
    return [int(tok) if tok.isdigit() else tok.lower() for tok in re.split(r"(\d+)", str(path))]


def _dlls_under(d: Optional[Path], dll_name: str) -> List[Path]:
    """Every copy of ``dll_name`` in ``d`` or below it, newest version folder first."""
    try:
        if not d or not d.exists():
            return []
        if d.is_file():
            return [d] if d.name.lower() == dll_name.lower() else []
        direct = d / dll_name
        found = [direct] if direct.exists() else []
        nested = [p for p in d.glob("**/" + dll_name) if p != direct]
    except OSError:
        return []
    return found + sorted(nested, key=_natural_key, reverse=True)


def _dir_with_dll(d: Optional[Path], dll_name: str = DLL_NAME) -> Optional[Path]:
    """Return d (or a subfolder) that contains the DLL, else None."""
    hits = _dlls_under(d, dll_name)
    return hits[0].parent if hits else None


def _program_files_roots() -> List[str]:
    return [
        os.environ.get("ProgramFiles", r"C:\Program Files"),
        os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"),
    ]


def _expand(path: str) -> Path:
    return Path(os.path.expandvars(path))


def _system_roots(gac_assembly: str, nuget_packages: Sequence[str]) -> List[Path]:
    """Folders worth searching, most specific first. ``nuget_packages`` are glob patterns."""
    roots: List[Path] = [
        # Power BI Desktop (MSI, per-user, Microsoft Store). Desktop ships its own
        # Microsoft.PowerBI.AdomdClient.dll, not the standard assembly, so this rarely hits.
        Path(r"C:\Program Files\Microsoft Power BI Desktop\bin"),
        _expand(r"%LOCALAPPDATA%\Microsoft\Power BI Desktop\bin"),
        _expand(r"%LOCALAPPDATA%\Microsoft\WindowsApps"),
        # The Global Assembly Cache: where SSMS and older Desktop builds register the assembly.
        _expand(r"%WINDIR%\Microsoft.NET\assembly\GAC_MSIL") / gac_assembly,
    ]
    for pf in _program_files_roots():
        roots.append(Path(pf) / "Microsoft.NET" / "ADOMD.NET")  # legacy standalone ADOMD.NET MSI
    for pf in _program_files_roots():
        root = Path(pf)
        try:
            if not root.exists():
                continue
            # SSMS, any version (SSMS 20 and earlier are x86; SSMS 21+ are x64)
            ssms = sorted(root.glob("Microsoft SQL Server Management Studio*"))
            roots.extend(d / "Common7" / "IDE" for d in ssms)
            sql_root = root / "Microsoft SQL Server"
            if sql_root.exists():
                roots.extend(sorted(sql_root.glob("*/SDK/Assemblies"), key=_natural_key, reverse=True))
                for cache in sql_root.glob("*/Setup Bootstrap/Update Cache"):
                    gdr = sorted(cache.glob("*/GDR/x64"), key=_natural_key)
                    if gdr:
                        roots.append(gdr[-1])
                    roots.append(cache)
        except OSError:
            continue
    nuget = _expand(r"%USERPROFILE%\.nuget\packages")
    try:
        if nuget.exists():
            for pattern in nuget_packages:
                roots.extend(sorted(nuget.glob(pattern)))
    except OSError:
        pass
    return roots


def find_candidates(dll_name: str, env_vars: Sequence[str], gac_assembly: str,
                    nuget_packages: Sequence[str], runtime: Optional[str] = None) -> List[Path]:
    """Every copy of ``dll_name`` worth trying, in the order it should be tried.

    Explicit overrides come first and are never demoted below system locations; within each
    group, builds the active runtime can load come first.
    """
    rt = runtime or active_runtime()
    explicit: List[Path] = []
    for position, var in enumerate(env_vars):
        value = os.environ.get(var)
        if not value:
            continue
        hits = _dlls_under(Path(value), dll_name)
        if not hits and Path(value).is_file():
            # A full path to some other DLL of the same package: look beside it.
            hits = _dlls_under(Path(value).parent, dll_name)
        if hits:
            # Ranked within this variable only (a package folder holds several builds); one
            # variable never overtakes an earlier one.
            explicit.extend(rank_candidates(hits, rt))
        elif position == 0:
            # Secondary variables are a convenience (ADOMD_DLL_PATH for TOM); only the library's
            # own variable is worth a warning when it points at nothing.
            logger.warning(f"{var} is set but {dll_name} was not found at: {value}")
    discovered: List[Path] = []
    for root in _system_roots(gac_assembly, nuget_packages):
        discovered.extend(_dlls_under(root, dll_name))

    ordered: List[Path] = []
    seen = set()
    for dll in explicit + rank_candidates(discovered, rt):
        key = os.path.normcase(str(dll))
        if key not in seen:
            seen.add(key)
            ordered.append(dll)
    return ordered


_ADOMD_NUGET = (
    "microsoft.analysisservices.adomdclient.retail.amd64",
    "microsoft.analysisservices.adomdclient.netcore.retail.amd64",
    "microsoft.analysisservices.adomdclient",
)
_TOM_NUGET = ("microsoft.analysisservices*",)


def adomd_candidates(runtime: Optional[str] = None) -> List[Path]:
    return find_candidates(DLL_NAME, ("ADOMD_DLL_PATH",), "Microsoft.AnalysisServices.AdomdClient",
                           _ADOMD_NUGET, runtime)


def tom_candidates(runtime: Optional[str] = None) -> List[Path]:
    # ADOMD_DLL_PATH is honoured too: the AMO package is commonly extracted beside ADOMD.
    return find_candidates(TOM_DLL_NAME, ("TOM_DLL_PATH", "ADOMD_DLL_PATH"),
                           "Microsoft.AnalysisServices.Tabular", _TOM_NUGET, runtime)


def find_adomd_dll(runtime: Optional[str] = None) -> Optional[Path]:
    """The directory of the best ADOMD.NET candidate for the active runtime, or None.

    Order: ADOMD_DLL_PATH -> Power BI Desktop -> GAC -> legacy ADOMD.NET MSI -> SSMS (any
    version) -> SQL Server SDK assemblies -> SQL Server Update Cache -> NuGet packages, with
    builds the runtime cannot load moved to the end. This only locates a file; ``load_adomd``
    is what proves a candidate works.
    """
    candidates = adomd_candidates(runtime)
    return candidates[0].parent if candidates else None


def _prepend_search_path(directory: Path) -> Tuple[bool, bool]:
    """Make the folder of a DLL visible to pythonnet (sys.path) and to native loaders (PATH).

    Returns which of the two this call actually added, so a failed load can take back exactly
    its own additions and nothing a previously loaded library still relies on.
    """
    path_str = str(directory)
    added_sys = path_str not in sys.path
    if added_sys:
        sys.path.insert(0, path_str)
    added_env = path_str not in os.environ.get("PATH", "").split(os.pathsep)
    if added_env:
        os.environ["PATH"] = path_str + os.pathsep + os.environ.get("PATH", "")
    return added_sys, added_env


def _remove_search_path(directory: Path, added: Tuple[bool, bool]) -> None:
    path_str = str(directory)
    if added[0] and path_str in sys.path:
        sys.path.remove(path_str)
    if added[1]:
        parts = os.environ.get("PATH", "").split(os.pathsep)
        if path_str in parts:
            parts.remove(path_str)
            os.environ["PATH"] = os.pathsep.join(parts)


def ensure_adomd_on_path() -> Optional[Path]:
    """Find the ADOMD.NET DLL and prepend its directory to sys.path and PATH.

    Returns the directory, or None if not found. Kept for callers that load the assembly
    themselves; the connectors use ``load_adomd`` instead.
    """
    d = find_adomd_dll()
    if not d:
        return None
    _prepend_search_path(d)
    return d


# ---------------------------------------------------------------- loading

@dataclass
class LoadResult:
    """Outcome of trying the candidates for one library."""
    name: str
    runtime: str
    value: Any = None                      # whatever the probe returned for the loaded DLL
    dll_path: Optional[Path] = None        # the DLL that was loaded
    failures: List[Tuple[Path, str]] = field(default_factory=list)
    searched: int = 0

    @property
    def available(self) -> bool:
        return self.dll_path is not None

    @property
    def directory(self) -> Optional[Path]:
        return self.dll_path.parent if self.dll_path else None

    def describe(self) -> str:
        """One paragraph for a log line or an 'unavailable' tool response."""
        if self.available:
            fw = dll_target_framework(self.dll_path)
            text = f"{self.name} loaded from {self.dll_path} ({fw} build, {self.runtime} runtime)."
            if self.failures:
                text += f" {len(self.failures)} earlier candidate(s) failed to load."
            return text
        if not self.searched:
            return f"{self.name}: no copy of the library was found."
        lines = [f"{self.name}: found {self.searched} candidate(s) but none could be loaded on the "
                 f"{self.runtime} runtime:"]
        lines += [f"  - {path}: {reason}" for path, reason in self.failures]
        if self.runtime == NETFX and any(
                dll_target_framework(p) == FW_NETCORE for p, _ in self.failures):
            lines.append("  Hint: a .NET (Core) build cannot load on .NET Framework. Either point "
                         "ADOMD_DLL_PATH / TOM_DLL_PATH at the lib/net45 build, or start the "
                         "server with PYTHONNET_RUNTIME=coreclr.")
        elif self.runtime == CORECLR and any(
                dll_target_framework(p) == FW_NETFX for p, _ in self.failures):
            lines.append("  Hint: a .NET Framework build was rejected while pythonnet runs CoreCLR. "
                         "Install the .netcore NuGet package or unset PYTHONNET_RUNTIME.")
        return "\n".join(lines)


def _first_line(exc: BaseException) -> str:
    text = str(exc).replace("\r", "")
    text = text.split("\n   at ")[0].strip()
    return f"{type(exc).__name__}: {text.splitlines()[0] if text else ''}".rstrip(": ")


def load_first(name: str, candidates: Sequence[Path], probe: Callable[[Path], Any],
               runtime: Optional[str] = None) -> LoadResult:
    """Try ``probe(dll)`` on each candidate in order; the first that returns wins.

    ``probe`` must raise when the DLL cannot be used. Its failures are recorded, not hidden, so
    an unavailable library can say exactly which files were tried and why each was rejected.
    """
    result = LoadResult(name=name, runtime=runtime or active_runtime(), searched=len(candidates))
    for dll in candidates:
        try:
            result.value = probe(dll)
        except Exception as exc:  # a failed candidate is the expected way to reach the next one
            result.failures.append((dll, _first_line(exc)))
            logger.debug(f"{name}: candidate rejected: {dll}: {_first_line(exc)}")
            continue
        result.dll_path = dll
        return result
    return result


def _same_file(a: str, b: str) -> bool:
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def _load_and_probe(dll: Path, namespace: str, probe: Callable[[Any], Any]) -> Any:
    """Load ``dll`` by full path, import ``namespace`` from it and run ``probe`` on the module.

    Once a broken copy of an assembly is loaded, asking for another copy with the same identity
    hands back the broken one. Comparing locations turns that into an explicit failure here
    instead of a confusing type error later.
    """
    import clr  # loads the .NET runtime on first use
    added = _prepend_search_path(dll.parent)
    try:
        assembly = clr.AddReference(str(dll))
        location = str(getattr(assembly, "Location", "") or "")
        if location and not _same_file(location, str(dll)):
            raise RuntimeError(
                f"another copy of this assembly is already loaded from {location}; restart the "
                "server with the DLL path set explicitly to use this one")
        return probe(importlib.import_module(namespace))
    except Exception:
        _remove_search_path(dll.parent, added)
        raise


@dataclass
class AdomdLibrary:
    """The ADOMD.NET classes the connectors need."""
    connection: Any
    command: Any
    schema_guid: Any


def _probe_adomd(dll: Path) -> AdomdLibrary:
    def probe(module: Any) -> AdomdLibrary:
        library = AdomdLibrary(module.AdomdConnection, module.AdomdCommand,
                               getattr(module, "AdomdSchemaGuid", None))
        library.connection()  # a wrong-runtime build fails here (or at import), never later
        return library
    return _load_and_probe(dll, "Microsoft.AnalysisServices.AdomdClient", probe)


def _probe_tom(dll: Path) -> Any:
    def probe(module: Any) -> Any:
        module.Server()  # the wrong-runtime build imports fine and only fails to construct this
        return module
    return _load_and_probe(dll, "Microsoft.AnalysisServices.Tabular", probe)


_LOADED: Dict[str, LoadResult] = {}


def load_adomd(force: bool = False) -> LoadResult:
    """Load ADOMD.NET once per process. ``result.value`` is an ``AdomdLibrary`` when available."""
    if force or "adomd" not in _LOADED:
        runtime = active_runtime()
        result = load_first("ADOMD.NET", adomd_candidates(runtime), _probe_adomd, runtime)
        _LOADED["adomd"] = result
        (logger.info if result.available else logger.warning)(result.describe())
    return _LOADED["adomd"]


def load_tom(force: bool = False) -> LoadResult:
    """Load TOM (Microsoft.AnalysisServices.Tabular) once per process. ``result.value`` is the
    imported namespace module when available."""
    if force or "tom" not in _LOADED:
        runtime = active_runtime()
        result = load_first("TOM", tom_candidates(runtime), _probe_tom, runtime)
        _LOADED["tom"] = result
        (logger.info if result.available else logger.warning)(result.describe())
    return _LOADED["tom"]
