"""
Shared ADOMD.NET client-library discovery for the Desktop and XMLA connectors.

Newer Power BI Desktop (MSI) builds no longer ship
``Microsoft.AnalysisServices.AdomdClient.dll`` in their ``bin`` folder, so this looks in
SSMS, the SQL Server SDK, the SQL Server Update Cache, and NuGet too. Versions are matched
with globs (not hard-coded numbers), and an explicit ``ADOMD_DLL_PATH`` environment variable
always wins.

Returns the *directory* that contains the DLL, so callers can add it to ``sys.path`` / ``PATH``.
"""
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

DLL_NAME = "Microsoft.AnalysisServices.AdomdClient.dll"

NOT_FOUND_HELP = (
    "ADOMD.NET client library (Microsoft.AnalysisServices.AdomdClient.dll) was not found. "
    "Newer Power BI Desktop (MSI) builds no longer ship it. Fix by either: installing SQL "
    "Server Management Studio (SSMS); installing the Microsoft.AnalysisServices.AdomdClient "
    "NuGet package; or setting the ADOMD_DLL_PATH environment variable to the folder (or full "
    "path) containing the DLL."
)


def _dir_with_dll(d: Optional[Path]) -> Optional[Path]:
    """Return d (or a subfolder) that contains the ADOMD DLL, else None."""
    try:
        if not d or not d.exists():
            return None
        if (d / DLL_NAME).exists():
            return d
        for found in d.glob("**/" + DLL_NAME):
            return found.parent
    except Exception:
        return None
    return None


def _program_files_roots() -> List[str]:
    return [
        os.environ.get("ProgramFiles", r"C:\Program Files"),
        os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"),
    ]


def find_adomd_dll() -> Optional[Path]:
    """Find the directory containing the ADOMD.NET client DLL, or None.

    Order: ADOMD_DLL_PATH -> Power BI Desktop (MSI + per-user + Store) -> SSMS (any version)
    -> SQL Server SDK assemblies (any version) -> SQL Server Update Cache (newest GDR/x64)
    -> ADOMD.NET NuGet packages.
    """
    # 1. Explicit override (a full path to the DLL, or a folder containing it)
    env = os.environ.get("ADOMD_DLL_PATH")
    if env:
        p = Path(env)
        if p.is_file() and p.name.lower() == DLL_NAME.lower():
            return p.parent
        hit = _dir_with_dll(p)
        if hit:
            return hit
        logger.warning(f"ADOMD_DLL_PATH is set but {DLL_NAME} was not found at: {env}")

    candidates: List[Path] = []

    # 2. Power BI Desktop (MSI, per-user, and Microsoft Store)
    candidates.append(Path(r"C:\Program Files\Microsoft Power BI Desktop\bin"))
    candidates.append(Path(os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Power BI Desktop\bin")))
    candidates.append(Path(os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\WindowsApps")))

    for pf in _program_files_roots():
        root = Path(pf)
        if not root.exists():
            continue
        # 3. SSMS, any version (SSMS 20 and earlier are x86; SSMS 21+ are x64)
        for d in root.glob("Microsoft SQL Server Management Studio*"):
            candidates.append(d / "Common7" / "IDE")
        # 4 + 5. SQL Server SDK assemblies and Update Cache, any version
        sql_root = root / "Microsoft SQL Server"
        if sql_root.exists():
            candidates.extend(sorted(sql_root.glob("*/SDK/Assemblies")))
            for cache in sql_root.glob("*/Setup Bootstrap/Update Cache"):
                gdr = sorted(cache.glob("*/GDR/x64"))
                if gdr:
                    candidates.append(gdr[-1])
                candidates.append(cache)

    # 6. ADOMD.NET NuGet packages under the user profile
    nuget = Path(os.path.expandvars(r"%USERPROFILE%\.nuget\packages"))
    for pkg in (
        "microsoft.analysisservices.adomdclient.netcore.retail.amd64",
        "microsoft.analysisservices.adomdclient.retail.amd64",
        "microsoft.analysisservices.adomdclient",
    ):
        pkgdir = nuget / pkg
        if pkgdir.exists():
            candidates.extend(sorted(pkgdir.glob("*")))  # version folders (recursed by _dir_with_dll)

    for d in candidates:
        hit = _dir_with_dll(d)
        if hit:
            return hit
    return None


def ensure_adomd_on_path() -> Optional[Path]:
    """Find the ADOMD.NET DLL and prepend its directory to sys.path and PATH.

    Returns the directory, or None if not found.
    """
    d = find_adomd_dll()
    if not d:
        return None
    path_str = str(d)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
    if path_str not in os.environ.get("PATH", ""):
        os.environ["PATH"] = path_str + os.pathsep + os.environ.get("PATH", "")
    return d
