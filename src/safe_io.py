"""
Faithful, atomic text file I/O for PBIP projects.

Power BI Desktop is strict about the files it opens (a byte order mark in the wrong TMDL file
stops a model loading) and source control is noisy about the rest (a rewrite that flips LF to
CRLF touches every line). So every writer in this server goes through here:

  * a file keeps the encoding (UTF-8 with or without BOM) and the newline style it had;
  * each write lands in a temp file beside the target and is moved into place with
    os.replace, so a crash never leaves a half-written file;
  * FileTransaction stages many files and commits them together, restoring every file it
    already replaced if a later one fails.
"""
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

PathLike = Union[str, os.PathLike]
_BOM = b"\xef\xbb\xbf"


@dataclass(frozen=True)
class TextFormat:
    """How a text file is stored on disk.

    ``newline`` is "\\n" or "\\r\\n" for a file that uses one style throughout, and "" for a
    file that mixes both (a mostly-LF file with a pasted CRLF block). A mixed file is never
    normalized: its text keeps each line's own ending and is written back as it stands, so an
    edit to two lines does not rewrite the endings of the other nine hundred.
    """
    bom: bool = False
    newline: str = "\n"

    @property
    def mixed(self) -> bool:
        return self.newline == ""

    @property
    def encoding(self) -> str:
        return "utf-8-sig" if self.bom else "utf-8"


DEFAULT_FORMAT = TextFormat()


def detect_format(raw: bytes) -> TextFormat:
    """Format of existing bytes: BOM presence and newline style (LF, CRLF, or "" when mixed)."""
    crlf = raw.count(b"\r\n")
    bare_lf = raw.count(b"\n") - crlf
    newline = "" if (crlf and bare_lf) else ("\r\n" if crlf else "\n")
    return TextFormat(bom=raw.startswith(_BOM), newline=newline)


def decode(raw: bytes) -> Tuple[str, TextFormat]:
    """Bytes -> (text without BOM, the format they were stored in).

    For a file with one newline style the text uses \\n throughout. For a mixed file the text
    is returned as stored, \\r\\n included, so that writing it back cannot change an ending.
    """
    fmt = detect_format(raw)
    text = raw[len(_BOM):].decode("utf-8") if fmt.bom else raw.decode("utf-8")
    return (text if fmt.mixed else text.replace("\r\n", "\n")), fmt


def encode(text: str, fmt: TextFormat = DEFAULT_FORMAT) -> bytes:
    """Text -> bytes in ``fmt``. For a single-style format, \\n and \\r\\n in ``text`` are both
    normalized to that style; for a mixed format the text is written exactly as given."""
    if fmt.mixed:
        body = text
    else:
        body = text.replace("\r\n", "\n")
        if fmt.newline != "\n":
            body = body.replace("\n", fmt.newline)
    return (_BOM if fmt.bom else b"") + body.encode("utf-8")


def read_text(path: PathLike) -> Tuple[str, TextFormat]:
    """Read a text file. Returns (text, its on-disk format). The text uses \\n newlines unless
    the file mixes styles (``fmt.mixed``), in which case it is returned as stored."""
    with open(path, "rb") as f:
        return decode(f.read())


def on_disk_spelling(path: PathLike) -> str:
    """``path`` with its last component spelled the way the folder already spells it.

    A case-insensitive file system keeps whatever name a replace is given, so writing
    "diagramlayout.json" over "diagramLayout.json" would silently rename the file. On a
    case-sensitive file system, or when the file does not exist yet, the path is returned as is.
    """
    text = os.fspath(path)
    parent, leaf = os.path.split(text)
    folded = os.path.normcase(leaf)
    if os.path.normcase("A") == "A":
        return text                      # case-sensitive file system: names are already exact
    try:
        for entry in os.listdir(parent or "."):
            if entry != leaf and os.path.normcase(entry) == folded:
                return os.path.join(parent, entry)
    except OSError:
        pass
    return text


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path = Path(on_disk_spelling(path))
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def write_text(path: PathLike, text: str, fmt: Optional[TextFormat] = None) -> TextFormat:
    """Atomically write ``text``. With no ``fmt``, an existing file keeps its own format and a
    new file is written as UTF-8 without BOM with LF newlines. Returns the format used."""
    p = Path(path)
    if fmt is None:
        fmt = DEFAULT_FORMAT
        if p.exists():
            with open(p, "rb") as f:
                fmt = detect_format(f.read())
    p.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_bytes(p, encode(text, fmt))
    return fmt


def dumps_json(obj: Any, indent: int = 2) -> str:
    """JSON text the way PBIR files are stored: 2-space indent, non-ASCII kept readable."""
    return json.dumps(obj, indent=indent, ensure_ascii=False)


def read_json(path: PathLike) -> Any:
    text, _ = read_text(path)
    return json.loads(text)


def write_json(path: PathLike, obj: Any, fmt: Optional[TextFormat] = None, indent: int = 2) -> TextFormat:
    return write_text(path, dumps_json(obj, indent), fmt)


class FileTransaction:
    """Stage edits to many files in memory, then commit all or none.

        tx = FileTransaction()
        text = tx.read(path)          # sees an earlier staged edit to the same file
        tx.write(path, new_text)      # nothing touches disk yet
        tx.commit()                   # every file is replaced, or every file is restored

    Reads remember each file's format so a staged write goes back out the same way. Commit
    first writes every staged file to a temp file (so a full disk or a locked folder fails
    before anything changes), then moves them into place; if a move fails, the files already
    moved are restored from the bytes read before the commit and the error is re-raised.
    """

    def __init__(self) -> None:
        self._staged: Dict[str, Optional[str]] = {}       # key -> new text, or None to delete
        self._formats: Dict[str, TextFormat] = {}
        self._paths: Dict[str, str] = {}                  # key -> the path as first given
        self.committed = False

    def _key(self, path: PathLike) -> str:
        """Identity of a file inside the transaction. The case-folded form is only a
        dictionary key (two spellings of one file must meet); every file operation uses the
        spelling the caller gave first, so a commit never changes a file name's case."""
        real = os.path.abspath(os.fspath(path))
        key = os.path.normcase(real)
        self._paths.setdefault(key, real)
        return key

    def exists(self, path: PathLike) -> bool:
        k = self._key(path)
        if k in self._staged:
            return self._staged[k] is not None
        return os.path.isfile(self._paths[k])

    def read(self, path: PathLike) -> str:
        """Current text of ``path``: the staged version if there is one, else the file."""
        k = self._key(path)
        if k in self._staged:
            if self._staged[k] is None:
                raise FileNotFoundError(f"{path} is staged for deletion")
            return self._staged[k]
        text, fmt = read_text(self._paths[k])
        self._formats.setdefault(k, fmt)
        return text

    def read_json(self, path: PathLike) -> Any:
        return json.loads(self.read(path))

    def write(self, path: PathLike, text: str, fmt: Optional[TextFormat] = None) -> None:
        """Stage ``text`` for ``path``. An existing file keeps its format unless ``fmt`` is given."""
        k = self._key(path)
        if fmt is not None:
            self._formats[k] = fmt
        elif k not in self._formats and os.path.isfile(self._paths[k]):
            with open(self._paths[k], "rb") as f:
                self._formats[k] = detect_format(f.read())
        self._staged[k] = text

    def write_json(self, path: PathLike, obj: Any, fmt: Optional[TextFormat] = None, indent: int = 2) -> None:
        self.write(path, dumps_json(obj, indent), fmt)

    def delete(self, path: PathLike) -> None:
        self._staged[self._key(path)] = None

    def discard(self) -> None:
        """Drop every staged edit without touching disk."""
        self._staged.clear()

    def _changed_keys(self) -> List[str]:
        changed = []
        for k, text in self._staged.items():
            real = self._paths[k]
            if text is None:
                if os.path.isfile(real):
                    changed.append(k)
                continue
            new_bytes = encode(text, self._formats.get(k, DEFAULT_FORMAT))
            try:
                with open(real, "rb") as f:
                    if f.read() == new_bytes:
                        continue
            except OSError:
                pass
            changed.append(k)
        return changed

    @property
    def paths(self) -> List[str]:
        """Paths whose staged content differs from what is on disk (deletes included), spelled
        as on disk for existing files and as given for new ones."""
        return [on_disk_spelling(self._paths[k]) for k in self._changed_keys()]

    def commit(self) -> List[str]:
        """Apply every staged edit atomically. Returns the paths that changed on disk."""
        if self.committed:
            raise RuntimeError("transaction already committed")
        todo = self._changed_keys()
        real = {k: on_disk_spelling(self._paths[k]) for k in todo}
        originals: Dict[str, Optional[bytes]] = {}
        temps: Dict[str, Path] = {}
        try:
            for k in todo:                                   # phase 1: prepare
                try:
                    with open(real[k], "rb") as f:
                        originals[k] = f.read()
                except FileNotFoundError:
                    originals[k] = None
                text = self._staged[k]
                if text is None:
                    continue
                target = Path(real[k])
                target.parent.mkdir(parents=True, exist_ok=True)
                tmp = target.with_name(f"{target.name}.{uuid.uuid4().hex[:8]}.tmp")
                with open(tmp, "wb") as f:
                    f.write(encode(text, self._formats.get(k, DEFAULT_FORMAT)))
                    f.flush()
                    os.fsync(f.fileno())
                temps[k] = tmp
        except BaseException:
            _remove_all(temps.values())
            raise

        done: List[str] = []
        try:
            for k in todo:                                   # phase 2: move into place
                if self._staged[k] is None:
                    os.remove(real[k])
                else:
                    os.replace(temps[k], real[k])
                done.append(k)
        except BaseException:
            for k in done:                                   # restore what already moved
                try:
                    if originals[k] is None:
                        os.remove(real[k])
                    else:
                        _atomic_write_bytes(Path(real[k]), originals[k])
                except OSError:
                    pass
            _remove_all(tmp for k, tmp in temps.items() if k not in done)
            raise
        self.committed = True
        self._staged.clear()
        return [real[k] for k in todo]


def _remove_all(paths) -> None:
    for p in paths:
        try:
            Path(p).unlink()
        except OSError:
            pass
