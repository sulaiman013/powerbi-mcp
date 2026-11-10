"""
Power BI TOM (Tabular Object Model) Connector
Provides write operations: rename, update, create, delete for tables, columns, measures
Uses Microsoft.AnalysisServices.Tabular for model modifications

How writes work, because every guarantee below depends on it:

* TOM keeps a local copy of the model. An edit changes that copy; ``Model.SaveChanges()`` sends
  all pending edits to the engine as ONE batch, which the engine applies entirely or not at all.
* Outside a transaction the mutating methods only stage an edit; the caller follows up with
  ``save_changes()`` (the batch methods do it themselves when ``auto_save`` is true). If that
  save fails, the staged edits are undone, so a rejected edit can never ride along with the
  next, unrelated save.
* A transaction (``begin_transaction`` / ``commit_transaction`` / ``rollback_transaction``) is
  owned by this connector, not by its caller: while it is open nothing saves implicitly, not
  even an explicit ``save_changes()`` or ``auto_save=True``. Commit is the single SaveChanges,
  rollback is ``UndoLocalChanges()``. A failed commit leaves the transaction open with its
  edits intact, so the caller can fix the cause or roll back. "Reversible" therefore means
  until commit; a committed transaction is not undone by this class.
* Every operation is atomic on the local copy: if it fails halfway (a rename that rewrote
  three expressions before the engine rejected the new name) its own edits are reverted, in or
  out of a transaction, without touching anything else that is staged.
* The local copy is re-synchronised from the engine before a write when nothing is staged, so
  objects created in Power BI Desktop after ``connect`` are seen.

Renames rewrite DAX with the shared tokenizer (dax_tokens), never with regular expressions over
raw text: strings, comments and same-named objects of other kinds are left alone.
"""
import logging
import threading
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import adomd_loader
import dax_tokens as dt
from adomd_query import clean_error
from connection_strings import ConnectionStringError, local_data_source

logger = logging.getLogger(__name__)

# Load TOM (runtime-aware discovery with fall-through; see adomd_loader). TOM_DLL_PATH, then
# ADOMD_DLL_PATH, are tried before any system location.
_tom = adomd_loader.load_tom()
_tom_available = _tom.available
_tom_path = _tom.directory
TOM = _tom.value if _tom.available else None


def _find_tom_dll() -> Optional[Path]:
    """Directory of the TOM build preferred for the active runtime (kept for older callers)."""
    candidates = adomd_loader.tom_candidates()
    return candidates[0].parent if candidates else None


@dataclass
class RenameOperation:
    """Represents a rename operation"""
    old_name: str
    new_name: str
    table_name: Optional[str] = None  # For columns/measures


@dataclass
class OperationResult:
    """Result of a write operation"""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None


TABLE_RENAME_WARNING = (
    "IMPORTANT: Renaming tables will update model references (measures, relationships) but CANNOT "
    "update report visuals. Visuals using this table will break and need manual fixing in Power BI Desktop."
)

# expression kind -> key of the list in a result's details
_KIND_KEYS = {
    "measure": "updated_measures",
    "calculated_column": "updated_calculated_columns",
    "calculated_table": "updated_calculated_tables",
    "calculation_item": "updated_calculation_items",
    "rls_filter": "updated_rls_filters",
    "kpi": "updated_kpis",
}
_OTHER_KEY = "updated_other"
_DETAIL_KEYS = tuple(_KIND_KEYS.values()) + (_OTHER_KEY,)
_PAST = {"rename": "Renamed", "update": "Updated"}


def _opt(obj: Any, attr: str) -> Any:
    """An optional member of a TOM object, or None.

    Newer members (FormatStringDefinition, CalculationGroup, Functions, ...) are missing on
    older TOM builds, and some getters refuse below a compatibility level. Either way the
    object simply has nothing there, which is what None says.
    """
    try:
        return getattr(obj, attr, None)
    except Exception:
        return None


def _enum_name(value: Any) -> str:
    """'Calculated' for PartitionSourceType.Calculated, however the runtime renders the enum."""
    return "" if value is None else str(value).rsplit(".", 1)[-1]


def _locked(method: Callable) -> Callable:
    """Serialise a public method: TOM objects are not thread-safe and handlers run in a pool."""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapper


class _Journal:
    """Undo log for one operation on the local model."""

    def __init__(self) -> None:
        self._undo: List[Callable[[], None]] = []

    def __len__(self) -> int:
        return len(self._undo)

    def set(self, obj: Any, attr: str, value: Any) -> None:
        old = getattr(obj, attr)
        setattr(obj, attr, value)
        self._undo.append(lambda: setattr(obj, attr, old))

    def add(self, collection: Any, item: Any) -> None:
        collection.Add(item)
        self._undo.append(lambda: collection.Remove(item))

    def rollback(self) -> List[str]:
        """Revert everything recorded, newest first. Returns the problems met while doing so."""
        problems: List[str] = []
        while self._undo:
            undo = self._undo.pop()
            try:
                undo()
            except Exception as e:
                problems.append(clean_error(e))
        return problems


@dataclass
class _Slot:
    """One DAX expression stored in the model."""
    obj: Any
    attr: str
    kind: str
    label: str                   # how the expression is named in results, e.g. "Sales[Total]"
    home: Optional[str] = None   # the table that owns the object
    name: Optional[str] = None   # the object's own name, where it has one
    table: Optional[str] = None  # the table whose row context the expression runs in, if any

    @property
    def text(self) -> str:
        value = _opt(self.obj, self.attr)
        return value if isinstance(value, str) else ""


def _has_string_literal(expr: str, name: str) -> bool:
    """True when ``expr`` contains the string literal "name": the usual sign of an extension
    column of that name (ADDCOLUMNS(..., "name", ...)), which an unqualified [name] may mean."""
    for token in dt.tokenize(expr):
        if token.kind == dt.STRING:
            body = token.text[1:-1] if len(token.text) >= 2 and token.text.endswith('"') else token.text[1:]
            if dt.names_equal(body.replace('""', '"'), name):
                return True
    return False


def _rename_measure_qualified_only(expr: str, old: str, new: str, is_column: Callable[[str, str], bool],
                                   measure_table: str) -> Tuple[str, int, int]:
    """Like dax_tokens.rename_measure, but unqualified [old] is left alone and counted.

    Used for expressions evaluated in the row context of a table that has a COLUMN called
    ``old``: there an unqualified [old] is far more likely that column than the measure.
    Returns (new_expr, replacements, unqualified_left).
    """
    tokens = dt.tokenize(expr)
    replacements: Dict[int, str] = {}
    left = 0
    for ref in dt.iter_references(tokens):
        if not dt.names_equal(ref.name, old):
            continue
        if ref.kind == "bare":
            left += 1
        elif (ref.kind == "column" and dt.names_equal(ref.table, measure_table)
              and not is_column(ref.table, ref.name)):
            replacements[ref.index] = dt.bracket(new)
    if not replacements:
        return expr, 0, left
    return "".join(replacements.get(i, t.text) for i, t in enumerate(tokens)), len(replacements), left


class PowerBITOMConnector:
    """
    TOM Connector for Power BI Desktop write operations

    Provides:
    - Rename tables, columns, measures
    - Update measure expressions
    - Batch operations for bulk changes
    - Connector-owned transactions (see the module docstring)
    """

    def __init__(self):
        """Initialize the TOM connector"""
        self.server = None
        self.database = None
        self.model = None
        self.current_port: Optional[int] = None
        self.connection_string: Optional[str] = None
        self.last_error: Optional[str] = None  # why the last connect() returned False
        self._changes_pending = False
        self._txn_open = False
        self._lock = threading.RLock()

    @staticmethod
    def is_available() -> bool:
        """Check if TOM is available"""
        return _tom_available

    @staticmethod
    def availability_details() -> str:
        """Which TOM DLL was loaded, or exactly why none could be."""
        return _tom.describe()

    # ==================== CONNECTION ====================

    def _drop_connection(self) -> None:
        """Forget the local model (and with it anything staged) and release the session."""
        server = self.server
        self.server = None
        self.database = None
        self.model = None
        self._changes_pending = False
        if server is not None:
            try:
                server.Disconnect()
            except Exception as e:
                # The session is gone either way; a failed goodbye changes nothing.
                logger.debug(f"TOM disconnect failed: {clean_error(e)}")

    def _connect_fresh(self, port: int) -> None:
        """Open a new session and take its (single) database. Raises on any problem."""
        server = TOM.Server()
        server.Connect(local_data_source(port))
        try:
            count = int(server.Databases.Count)
            if count != 1:
                # Power BI Desktop hosts exactly one database. Anything else is a full Analysis
                # Services server, and "the first database" there is not ours to edit.
                raise RuntimeError(
                    f"expected the single database of a Power BI Desktop instance, found {count}; "
                    "refusing to pick one to write to")
            database = server.Databases[0]
            model = database.Model
            if model is None:
                raise RuntimeError("the database has no tabular model")
        except Exception:
            try:
                server.Disconnect()
            except Exception as e:
                logger.debug(f"TOM disconnect failed: {clean_error(e)}")
            raise
        self.server, self.database, self.model = server, database, model
        self.current_port = port
        self.connection_string = local_data_source(port)
        self._changes_pending = False

    @_locked
    def connect(self, port: int) -> bool:
        """
        Connect to Power BI Desktop instance via TOM

        Args:
            port: Port number of the Power BI Desktop instance

        Returns:
            True if connection successful; on False, ``last_error`` says why.

        Connecting to a different port is refused while a transaction is open or edits are
        staged: the staged edits belong to the current model and would be lost silently.
        Reconnecting to the same port in that situation is a no-op that keeps them.
        """
        self.last_error = None
        if not _tom_available:
            self.last_error = self.availability_details()
            logger.error("TOM not available - cannot connect for write operations")
            return False
        try:
            local_data_source(port)
        except ConnectionStringError as e:
            self.last_error = str(e)
            logger.error(f"TOM connection failed: {self.last_error}")
            return False
        port = int(port)

        if self.model is not None and (self._txn_open or self._has_local_changes()):
            if port == self.current_port:
                return True
            what = "A transaction is open" if self._txn_open else "There are unsaved edits"
            self.last_error = (f"{what} on port {self.current_port}; commit or roll back "
                               f"(save or discard) before connecting to port {port}.")
            logger.error(f"TOM connection refused: {self.last_error}")
            return False

        self._drop_connection()
        self._txn_open = False
        try:
            self._connect_fresh(port)
        except Exception as e:
            self.last_error = clean_error(e)
            logger.error(f"TOM connection failed: {self.last_error}")
            return False
        logger.info(f"TOM connected to: {self.database.Name}")
        return True

    @_locked
    def disconnect(self):
        """Disconnect from the model. Staged edits and an open transaction are discarded;
        nothing that was not saved reaches the model."""
        self._drop_connection()
        self.current_port = None
        self._txn_open = False

    def _ensure_connected(self) -> bool:
        """Ensure we have a valid connection"""
        if not self.model:
            logger.error("Not connected - call connect() first")
            return False
        return True

    def _has_local_changes(self) -> bool:
        return bool(self._changes_pending or _opt(self.model, "HasLocalChanges"))

    def _sync(self) -> Optional[str]:
        """Bring the local model up to date with the engine. Returns an error text or None.

        ``Model.Sync()`` is the cheap way. If the session behind it is gone, a new connection
        yields the same fresh copy; only when that fails too is there an error to report.
        """
        try:
            self.model.Sync()
            return None
        except Exception as e:
            first = clean_error(e)
        port = self.current_port
        self._drop_connection()
        try:
            self._connect_fresh(port)
        except Exception as e:
            return f"{first}; reconnecting also failed: {clean_error(e)}"
        return None

    def _begin_write(self) -> Optional[OperationResult]:
        """Common entry of every mutating method. Returns a failure result, or None to go on.

        With nothing staged the local model is re-synchronised first, so the edit is checked
        against the model as it is now, not as it was at connect time. With edits staged (an
        open transaction, or a stage-then-save in progress) the local model IS the working
        state and must not be replaced.
        """
        if not self._ensure_connected():
            return OperationResult(False, "Not connected")
        if self._txn_open or self._has_local_changes():
            return None
        error = self._sync()
        if error:
            return OperationResult(
                False, f"Could not refresh the model from Power BI Desktop before writing: {error}")
        return None

    # ==================== SAVE / DISCARD / TRANSACTIONS ====================

    def _discard_local(self) -> Tuple[bool, str]:
        """Throw away everything staged on the local model. Returns (ok, what happened)."""
        try:
            self.model.UndoLocalChanges()
            self._changes_pending = False
            return True, "Local edits were undone; the model is unchanged."
        except Exception as e:
            first = clean_error(e)
        # UndoLocalChanges needs a live session. Dropping the local model discards the edits
        # just as surely; a fresh connection then gives a clean copy to work on.
        port = self.current_port
        self._drop_connection()
        try:
            self._connect_fresh(port)
        except Exception as e:
            return False, (f"Local edits were dropped by disconnecting (undo failed: {first}; "
                           f"reconnect failed: {clean_error(e)}). Nothing was saved; connect again.")
        return True, "Local edits were discarded by reconnecting; the model is unchanged."

    def _save(self, undo_on_failure: bool) -> OperationResult:
        try:
            self.model.SaveChanges()
        except Exception as e:
            error = clean_error(e)
            logger.error(f"Failed to save changes: {error}")
            if not undo_on_failure:
                return OperationResult(False, f"Failed to save changes: {error}",
                                       {"saved": False, "undone": False, "error": error})
            _, note = self._discard_local()
            return OperationResult(False, f"Failed to save changes: {error}. {note}",
                                   {"saved": False, "undone": True, "error": error})
        self._changes_pending = False
        logger.info("Changes saved successfully")
        return OperationResult(True, "Changes saved successfully", {"saved": True})

    def _autosave(self, auto_save: bool) -> Optional[OperationResult]:
        """Save now if asked to and no transaction is open. Returns a FAILED save, else None."""
        if not auto_save or self._txn_open:
            return None
        save = self._save(undo_on_failure=True)
        return None if save.success else save

    @_locked
    def save_changes(self) -> OperationResult:
        """Save pending changes to the model.

        Inside a transaction this does not save: it reports success with ``details['deferred']``
        set, because the edits belong to the transaction and commit saves them. Outside one, a
        failed save undoes the pending edits (``details['undone']``).
        """
        if not self._ensure_connected():
            return OperationResult(False, "Not connected")
        if self._txn_open:
            return OperationResult(
                True, "Deferred: a transaction is open; commit_transaction() will save this edit.",
                {"saved": False, "deferred": True})
        return self._save(undo_on_failure=True)

    @_locked
    def discard_changes(self) -> OperationResult:
        """Discard pending changes (outside a transaction; use rollback_transaction inside one)."""
        if not self._ensure_connected():
            return OperationResult(False, "Not connected")
        if self._txn_open:
            return OperationResult(False, "A transaction is open; use rollback_transaction() to discard it.")
        ok, note = self._discard_local()
        return OperationResult(ok, "Changes discarded" if ok else f"Failed to discard changes: {note}")

    @property
    def in_transaction(self) -> bool:
        """True while a transaction started by begin_transaction() is open."""
        return self._txn_open

    @property
    def has_pending_changes(self) -> bool:
        """True while edits are staged on the local model and not yet saved."""
        with self._lock:
            return self.model is not None and self._has_local_changes()

    @_locked
    def begin_transaction(self) -> OperationResult:
        """Open a transaction: from now on edits are only staged, until commit or rollback."""
        if not self._ensure_connected():
            return OperationResult(False, "Not connected")
        if self._txn_open:
            return OperationResult(False, "A transaction is already open. Commit or roll it back first.")
        if self._has_local_changes():
            return OperationResult(False, "There are unsaved edits from an earlier call; save_changes() or "
                                          "discard_changes() first, so the transaction starts clean.")
        error = self._sync()
        if error:
            return OperationResult(
                False, f"Could not refresh the model before starting a transaction: {error}")
        self._txn_open = True
        return OperationResult(True, "Transaction started. Edits are staged until commit_transaction(); "
                                     "rollback_transaction() discards them.")

    @_locked
    def commit_transaction(self) -> OperationResult:
        """Save every edit staged since begin_transaction() as one batch and close the
        transaction. If the engine rejects the batch, nothing is saved and the transaction STAYS
        OPEN (``details['transaction_open']``): fix the cause and commit again, or roll back."""
        if not self._txn_open:
            return OperationResult(False, "No transaction is open.", {"transaction_open": False})
        if not self._ensure_connected():
            self._txn_open = False
            return OperationResult(False, "Not connected; the transaction's edits are gone and nothing "
                                          "was saved.", {"transaction_open": False})
        save = self._save(undo_on_failure=False)
        if not save.success:
            error = (save.details or {}).get("error", save.message)
            return OperationResult(
                False, f"Commit failed and nothing was saved: {error}. The transaction is still open: "
                       "fix the cause and commit again, or roll back.",
                {"transaction_open": True, "saved": False, "error": error})
        self._txn_open = False
        return OperationResult(True, "Transaction committed. Changes saved successfully",
                               {"transaction_open": False, "saved": True})

    @_locked
    def rollback_transaction(self) -> OperationResult:
        """Discard every edit staged since begin_transaction() and close the transaction."""
        if not self._txn_open:
            return OperationResult(False, "No transaction is open.", {"transaction_open": False})
        if not self._ensure_connected():
            self._txn_open = False
            return OperationResult(True, "Transaction closed; the connection was already gone and nothing "
                                         "had been saved.", {"transaction_open": False})
        ok, note = self._discard_local()
        # Either the edits were undone or the local model holding them was dropped: in both
        # cases nothing of the transaction is left, so it is closed.
        self._txn_open = False
        prefix = "Transaction rolled back." if ok else "Transaction closed."
        return OperationResult(ok, f"{prefix} {note}", {"transaction_open": False})

    def _run(self, operation: Callable[[_Journal], OperationResult], what: str) -> OperationResult:
        """Run one operation atomically on the local model: on an exception or a failed result,
        exactly the edits it made are reverted."""
        journal = _Journal()
        try:
            result = operation(journal)
        except Exception as e:
            problems = journal.rollback()
            message = f"Failed to {what}: {clean_error(e)}"
            if problems:
                message += f" (reverting the partial edit also failed: {'; '.join(problems)})"
            logger.error(message)
            return OperationResult(False, message)
        if not result.success:
            journal.rollback()
        elif len(journal):
            self._changes_pending = True
        return result

    def _pending_note(self) -> str:
        if not self._txn_open:
            return ""
        return " Staged in the open transaction; nothing is saved until commit_transaction()."

    # ==================== MODEL TRAVERSAL ====================

    def _find_table(self, name: str) -> Any:
        return self.model.Tables.Find(name) if name else None

    def _find_measure(self, name: str, table_name: Optional[str] = None) -> Tuple[Any, Any]:
        """(table, measure) for a measure name, searching one table or the whole model."""
        if table_name:
            table = self._find_table(table_name)
            return (table, table.Measures.Find(name)) if table is not None else (None, None)
        for table in self.model.Tables:
            measure = table.Measures.Find(name)
            if measure is not None:
                return table, measure
        return None, None

    def _is_column(self, table_name: Optional[str], column_name: Optional[str]) -> bool:
        table = self._find_table(table_name) if table_name else None
        return table is not None and bool(column_name) and table.Columns.Find(column_name) is not None

    def _slots(self) -> Iterator[_Slot]:
        """Every DAX expression in the model: measures (with KPI, detail-rows and dynamic format
        string expressions), calculated columns, calculated tables, calculation items, RLS
        filters, and user-defined functions where the model has them."""
        for table in self.model.Tables:
            tname = table.Name
            for measure in table.Measures:
                label = f"{tname}[{measure.Name}]"
                yield _Slot(measure, "Expression", "measure", label, tname, measure.Name)
                kpi = _opt(measure, "KPI")
                if kpi is not None:
                    for attr in ("TargetExpression", "StatusExpression", "TrendExpression"):
                        yield _Slot(kpi, attr, "kpi", f"{label} KPI {attr[:-10].lower()}", tname)
                for attr, what in (("DetailRowsDefinition", "detail rows"),
                                   ("FormatStringDefinition", "format string")):
                    definition = _opt(measure, attr)
                    if definition is not None:
                        yield _Slot(definition, "Expression", "other", f"{label} {what}", tname)
            for column in table.Columns:
                # Only calculated columns carry a DAX expression (DataColumn has no such member).
                calculated = _enum_name(_opt(column, "Type")) == "Calculated"
                if calculated or isinstance(_opt(column, "Expression"), str):
                    yield _Slot(column, "Expression", "calculated_column", f"{tname}[{column.Name}]",
                                tname, column.Name, table=tname)
            for partition in (_opt(table, "Partitions") or ()):
                # M and query partitions also have an Expression/Query; only a calculated
                # partition's is DAX.
                if _enum_name(_opt(partition, "SourceType")) == "Calculated":
                    source = _opt(partition, "Source")
                    if source is not None:
                        yield _Slot(source, "Expression", "calculated_table",
                                    f"{tname} (calculated table)", tname)
            group = _opt(table, "CalculationGroup")
            if group is not None:
                for item in (_opt(group, "CalculationItems") or ()):
                    yield _Slot(item, "Expression", "calculation_item", f"{tname}[{item.Name}]",
                                tname, item.Name)
                    definition = _opt(item, "FormatStringDefinition")
                    if definition is not None:
                        yield _Slot(definition, "Expression", "calculation_item",
                                    f"{tname}[{item.Name}] format string", tname)
                for attr in ("NoSelectionExpression", "MultipleOrEmptySelectionExpression"):
                    selection = _opt(group, attr)
                    if selection is not None:
                        yield _Slot(selection, "Expression", "calculation_item", f"{tname} {attr}", tname)
            definition = _opt(table, "DefaultDetailRowsDefinition")
            if definition is not None:
                yield _Slot(definition, "Expression", "other", f"{tname} default detail rows", tname)
        for role in (_opt(self.model, "Roles") or ()):
            for permission in (_opt(role, "TablePermissions") or ()):
                owner = _opt(_opt(permission, "Table"), "Name")
                yield _Slot(permission, "FilterExpression", "rls_filter",
                            f"role '{role.Name}' on {owner}", owner, table=owner)
        for function in (_opt(self.model, "Functions") or ()):
            yield _Slot(function, "Expression", "other", f"function {function.Name}")

    def _rewrite(self, journal: _Journal,
                 rewrite: Callable[[_Slot, str], Tuple[str, int]]) -> Dict[str, List[str]]:
        """Apply ``rewrite`` to every expression; record the edits; return labels per kind."""
        updated: Dict[str, List[str]] = {key: [] for key in _DETAIL_KEYS}
        for slot in list(self._slots()):
            text = slot.text
            if not text:
                continue
            new_text, count = rewrite(slot, text)
            if count and new_text != text:
                journal.set(slot.obj, slot.attr, new_text)
                updated[_KIND_KEYS.get(slot.kind, _OTHER_KEY)].append(slot.label)
        return updated

    @staticmethod
    def _with_recalc_note(details: Dict[str, Any]) -> Dict[str, Any]:
        """Flag results that changed calculated columns/tables: the engine has to recalculate
        them (Refresh in Power BI Desktop) before they can be queried again."""
        if details.get("updated_calculated_columns") or details.get("updated_calculated_tables"):
            details["recalculation_needed"] = True
        return details

    @staticmethod
    def _checked_name(name: Any, what: str) -> str:
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{what} must be a non-empty name")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
            raise ValueError(f"{what} contains a control character")
        return name

    # ==================== DEPENDENCY SCANNING ====================

    @_locked
    def scan_table_dependencies(self, table_name: str) -> OperationResult:
        """
        Scan for all dependencies on a table before renaming

        Args:
            table_name: Name of the table to scan

        Returns:
            OperationResult with dependency details
        """
        if not self._ensure_connected():
            return OperationResult(False, "Not connected")

        table = self._find_table(table_name)
        if not table:
            return OperationResult(False, f"Table '{table_name}' not found")
        name = table.Name

        found: Dict[str, List[Dict[str, Any]]] = {key: [] for key in _DETAIL_KEYS}
        for slot in self._slots():
            text = slot.text
            if not text:
                continue
            # A table is referenced as a qualifier (Sales[Amount]) or on its own (COUNTROWS(Sales)).
            hits = sum(1 for ref in dt.references(text)
                       if ref.kind in ("column", "table") and dt.names_equal(ref.table, name))
            if hits:
                found[_KIND_KEYS.get(slot.kind, _OTHER_KEY)].append({
                    "name": slot.name or slot.label,   # with "table": the shape callers print
                    "table": slot.home,
                    "label": slot.label,
                    "references": hits,
                    "expression": text[:200] + "..." if len(text) > 200 else text,
                })

        relationships = []
        for relationship in self.model.Relationships:
            from_table, to_table = relationship.FromTable.Name, relationship.ToTable.Name
            if dt.names_equal(from_table, name) or dt.names_equal(to_table, name):
                relationships.append({
                    "name": _opt(relationship, "Name") or f"{from_table} -> {to_table}",
                    "type": "from_table" if dt.names_equal(from_table, name) else "to_table",
                    "from_table": from_table,
                    "to_table": to_table,
                })

        total_refs = sum(len(v) for v in found.values()) + len(relationships)
        return OperationResult(
            True,
            f"Found {total_refs} references to table '{name}'",
            {
                "table_name": name,
                "total_references": total_refs,
                "measures": found["updated_measures"],
                "calculated_columns": found["updated_calculated_columns"],
                "calculated_tables": found["updated_calculated_tables"],
                "calculation_items": found["updated_calculation_items"],
                "rls_filters": found["updated_rls_filters"],
                "kpis": found["updated_kpis"],
                "other": found[_OTHER_KEY],
                "relationships": relationships,
                "warning": TABLE_RENAME_WARNING,
            }
        )

    # ==================== TABLE OPERATIONS ====================

    def _rename_table(self, journal: _Journal, old_name: str, new_name: str,
                      update_references: bool) -> OperationResult:
        new_name = self._checked_name(new_name, "New table name")
        table = self._find_table(old_name)
        if not table:
            return OperationResult(False, f"Table '{old_name}' not found")
        current = table.Name
        if current == new_name:
            return OperationResult(False, f"Table '{current}' already has that name")
        # Names are case-insensitive: a hit that is not this very table is a collision.
        if self._find_table(new_name) is not None and not dt.names_equal(current, new_name):
            return OperationResult(False, f"Table '{new_name}' already exists")

        updated = {key: [] for key in _DETAIL_KEYS}
        if update_references:
            updated = self._rewrite(journal, lambda slot, text: dt.rename_table(text, current, new_name))
        journal.set(table, "Name", new_name)

        total_updated = sum(len(v) for v in updated.values())
        logger.info(f"Table renamed: '{current}' -> '{new_name}', updated {total_updated} references")
        details = {"old_name": current, "new_name": new_name, **updated,
                   "warning": "Report visuals using this table will need manual updating in "
                              "Power BI Desktop."}
        return OperationResult(
            True,
            f"Table renamed: '{current}' -> '{new_name}'. Updated {total_updated} model references.",
            self._with_recalc_note(details))

    @_locked
    def rename_table(self, old_name: str, new_name: str, update_references: bool = True) -> OperationResult:
        """
        Rename a table and optionally update all references in the model's DAX

        NOTE: This will update model-level references (measures, KPIs, calculated columns and
        tables, calculation items, RLS filters; relationships follow automatically) but CANNOT
        update report-level visual bindings. Visuals that use this table will need to be
        manually updated in Power BI Desktop.

        The edit is staged; call save_changes() (or commit the open transaction) to persist it.

        Args:
            old_name: Current table name
            new_name: New table name
            update_references: Whether to update references in DAX expressions (default: True)

        Returns:
            OperationResult with success status and details of updated references
        """
        blocked = self._begin_write()
        if blocked:
            return blocked
        return self._run(lambda j: self._rename_table(j, old_name, new_name, update_references),
                         "rename table")

    def _batch(self, items: List[Dict[str, Any]], auto_save: bool, action: str, thing: str,
               required: Tuple[str, ...], echo: Tuple[str, ...], missing: str,
               apply: Callable[[_Journal, Dict[str, Any]], OperationResult],
               references: bool = True) -> OperationResult:
        """Shared loop of the batch methods (``action`` 'rename' / 'update', ``thing`` 'table').

        Each item is applied atomically and reported on its own; then ONE save covers the batch,
        unless a transaction is open or ``auto_save`` is off. If that save fails, every item of
        the batch has been undone and is reported as not applied.
        """
        blocked = self._begin_write()
        if blocked:
            return blocked

        results: List[Dict[str, Any]] = []
        totals: Dict[str, List[str]] = {key: [] for key in _DETAIL_KEYS}
        for item in items or []:
            is_dict = isinstance(item, dict)
            entry: Dict[str, Any] = {key: (item.get(key) if is_dict else None) for key in echo}
            if not is_dict or not all(item.get(key) for key in required):
                entry.update(success=False, error=missing)
                results.append(entry)
                continue
            result = self._run(lambda journal, it=item: apply(journal, it), f"{action} {thing}")
            details = result.details or {}
            entry.update(success=result.success, error=None if result.success else result.message)
            if references:
                for key in _DETAIL_KEYS:
                    entry[key] = list(details.get(key, []))
                    if result.success:
                        totals[key].extend(entry[key])
                for key in ("unqualified_references_left", "ambiguous_references"):
                    if details.get(key):
                        entry[key] = details[key]
            results.append(entry)

        success_count = sum(1 for r in results if r["success"])
        saved = False
        if success_count:
            failed_save = self._autosave(auto_save)
            if failed_save is not None:
                for entry in results:
                    if entry["success"]:
                        entry.update(success=False, error="Not saved; the edit was undone")
                return OperationResult(
                    False, f"No {thing} was {_PAST[action].lower()}: {failed_save.message}",
                    {"results": results, "success_count": 0, "fail_count": len(results), "saved": False})
            saved = bool(auto_save) and not self._txn_open

        fail_count = len(results) - success_count
        message = f"{_PAST[action]} {success_count} {thing}(s), {fail_count} failed"
        details: Dict[str, Any] = {
            "results": results,
            "success_count": success_count,
            "fail_count": fail_count,
            "saved": saved,
            "pending": bool(success_count) and not saved,
        }
        if references:
            total = sum(len(v) for v in totals.values())
            message += f". Updated {total} {'references' if thing == 'measure' else 'model references'}."
            details.update(totals)
            details["total_updated_measures"] = len(totals["updated_measures"])
            details["total_updated_calculated_columns"] = len(totals["updated_calculated_columns"])
            details["total_updated_references"] = total
            self._with_recalc_note(details)
        if success_count and not saved:
            message += self._pending_note()
        return OperationResult(success_count > 0, message, details)

    @_locked
    def batch_rename_tables(self, renames: List[Dict[str, str]], auto_save: bool = True,
                            update_references: bool = True) -> OperationResult:
        """
        Batch rename multiple tables with automatic reference updates

        NOTE: This will update model-level references but CANNOT update report-level visual
        bindings. Visuals that use renamed tables will need to be manually updated in Power BI
        Desktop.

        Args:
            renames: List of {"old_name": "...", "new_name": "..."} dicts
            auto_save: Save at the end (default: True). Ignored while a transaction is open:
                       the renames are then staged and saved by commit_transaction().
            update_references: Whether to update references in DAX expressions (default: True)

        Returns:
            OperationResult with details of each rename and updated references
        """
        result = self._batch(
            renames, auto_save, "rename", "table", ("old_name", "new_name"), ("old_name", "new_name"),
            "Missing name",
            lambda j, it: self._rename_table(j, it["old_name"], it["new_name"], update_references))
        if result.details and "results" in result.details and result.success:
            result.details["warning"] = (
                "IMPORTANT: Report visuals using these tables will need manual updating in Power BI "
                "Desktop. The model references have been automatically updated.")
        return result

    # ==================== COLUMN OPERATIONS ====================

    def _rename_column(self, journal: _Journal, table_name: str, old_name: str, new_name: str,
                       update_references: bool) -> OperationResult:
        new_name = self._checked_name(new_name, "New column name")
        table = self._find_table(table_name)
        if not table:
            return OperationResult(False, f"Table '{table_name}' not found")
        column = table.Columns.Find(old_name)
        if not column:
            return OperationResult(False, f"Column '{old_name}' not found in table '{table_name}'")
        tname, current = table.Name, column.Name
        if current == new_name:
            return OperationResult(False, f"Column '{tname}'[{current}] already has that name")
        if table.Columns.Find(new_name) is not None and not dt.names_equal(current, new_name):
            return OperationResult(False, f"Column '{new_name}' already exists in table '{tname}'")
        if table.Measures.Find(new_name) is not None:
            return OperationResult(False, f"A measure named '{new_name}' already exists in table '{tname}'; "
                                          "a column cannot share its name")

        updated = {key: [] for key in _DETAIL_KEYS}
        left: List[str] = []
        if update_references:
            # An unqualified [name] can only be rewritten when it cannot mean anything else:
            # no measure of that name, and either the expression runs in this table's row
            # context or no other table has a column of that name. Otherwise it is reported.
            measure_same_name = self._find_measure(current)[1] is not None
            elsewhere = any(not dt.names_equal(t.Name, tname) and t.Columns.Find(current) is not None
                            for t in self.model.Tables)

            def rewrite(slot: _Slot, text: str) -> Tuple[str, int]:
                unqualified = (not measure_same_name and not _has_string_literal(text, current)
                               and (dt.names_equal(slot.table, tname) or not elsewhere))
                new_text, count, skipped = dt.rename_column(text, tname, current, new_name,
                                                            unqualified=unqualified)
                if skipped:
                    left.append(slot.label)
                return new_text, count

            updated = self._rewrite(journal, rewrite)
        journal.set(column, "Name", new_name)

        total_updated = sum(len(v) for v in updated.values())
        renamed = f"Column renamed: '{tname}'[{current}] -> [{new_name}]"
        logger.info(f"{renamed}, updated {total_updated} references")
        message = f"{renamed}. Updated {total_updated} model references."
        details = {"table_name": tname, "old_name": current, "new_name": new_name, **updated}
        if left:
            details["unqualified_references_left"] = left
            message += (f" {len(left)} expression(s) use an unqualified [{current}] that could not be "
                        "attributed to this column and were left as written; review them.")
        return OperationResult(True, message, self._with_recalc_note(details))

    @_locked
    def rename_column(self, table_name: str, old_name: str, new_name: str,
                      update_references: bool = True) -> OperationResult:
        """
        Rename a column and optionally update all references in the model's DAX

        Qualified references (Table[Column]) are always rewritten. An unqualified [Column] is
        rewritten only where it cannot be a measure or another table's column; the rest are
        listed in ``details['unqualified_references_left']``. The edit is staged, not saved.

        Args:
            table_name: Name of the table containing the column
            old_name: Current column name
            new_name: New column name
            update_references: Whether to update references in DAX expressions (default: True)

        Returns:
            OperationResult with success status
        """
        blocked = self._begin_write()
        if blocked:
            return blocked
        return self._run(lambda j: self._rename_column(j, table_name, old_name, new_name, update_references),
                         "rename column")

    @_locked
    def batch_rename_columns(self, renames: List[Dict[str, str]], auto_save: bool = True,
                             update_references: bool = True) -> OperationResult:
        """
        Batch rename multiple columns with automatic reference updates

        Args:
            renames: List of {"table_name": "...", "old_name": "...", "new_name": "..."} dicts
            auto_save: Save at the end (default: True). Ignored while a transaction is open.
            update_references: Whether to update references in DAX expressions (default: True)

        Returns:
            OperationResult with details of each rename and updated references
        """
        return self._batch(
            renames, auto_save, "rename", "column", ("table_name", "old_name", "new_name"),
            ("table_name", "old_name", "new_name"), "Missing required field",
            lambda j, it: self._rename_column(j, it["table_name"], it["old_name"], it["new_name"],
                                              update_references))

    # ==================== MEASURE OPERATIONS ====================

    def _rename_measure(self, journal: _Journal, old_name: str, new_name: str, table_name: Optional[str],
                        update_references: bool) -> OperationResult:
        new_name = self._checked_name(new_name, "New measure name")
        if table_name and not self._find_table(table_name):
            return OperationResult(False, f"Table '{table_name}' not found")
        home_table, measure = self._find_measure(old_name, table_name)
        if not measure:
            return OperationResult(False, f"Measure '{old_name}' not found")
        home, current = home_table.Name, measure.Name
        if current == new_name:
            return OperationResult(False, f"Measure '{current}' already has that name")
        # Measure names are unique across the whole model, and distinct from the columns of
        # their own table.
        other_table, other = self._find_measure(new_name)
        if other is not None and not dt.names_equal(current, new_name):
            return OperationResult(
                False, f"Measure '{new_name}' already exists in table '{other_table.Name}'")
        if home_table.Columns.Find(new_name) is not None:
            return OperationResult(False, f"A column named '{new_name}' already exists in table '{home}'; "
                                          "a measure cannot share its name")

        updated = {key: [] for key in _DETAIL_KEYS}
        ambiguous: List[str] = []
        same_name_columns = [f"{t.Name}[{current}]" for t in self.model.Tables
                             if t.Columns.Find(current) is not None]
        if update_references:
            def rewrite(slot: _Slot, text: str) -> Tuple[str, int]:
                if slot.table and self._is_column(slot.table, current):
                    # Row context of a table that has a column of this name: an unqualified
                    # [name] there is that column far more often than the measure.
                    new_text, count, skipped = _rename_measure_qualified_only(
                        text, current, new_name, self._is_column, home)
                    if skipped:
                        ambiguous.append(slot.label)
                    return new_text, count
                return dt.rename_measure(text, current, new_name, is_column=self._is_column,
                                         measure_table=home)

            updated = self._rewrite(journal, rewrite)
        journal.set(measure, "Name", new_name)

        total_updated = sum(len(v) for v in updated.values())
        logger.info(f"Measure renamed: '{current}' -> '{new_name}', updated {total_updated} references")
        message = f"Measure renamed: '{current}' -> '{new_name}'. Updated {total_updated} references."
        details = {"old_name": current, "new_name": new_name, "table_name": home, **updated}
        if same_name_columns:
            details["columns_with_same_name"] = same_name_columns
        if ambiguous:
            details["ambiguous_references"] = ambiguous
            message += (f" {len(ambiguous)} expression(s) use an unqualified [{current}] in the row context "
                        f"of a table with a column of that name; they were left as written; review them.")
        return OperationResult(True, message, self._with_recalc_note(details))

    @_locked
    def rename_measure(self, old_name: str, new_name: str, table_name: Optional[str] = None,
                       update_references: bool = True) -> OperationResult:
        """
        Rename a measure and optionally update all references in the model's DAX

        [Measure] is rewritten everywhere it is the measure. Table[Measure] is rewritten only
        when it can be the measure (its home table, which has no such column), so a same-named
        column of another table is never touched. The edit is staged, not saved.

        Args:
            old_name: Current measure name
            new_name: New measure name
            table_name: Optional table name (if not provided, searches all tables)
            update_references: Whether to update references in DAX expressions (default: True)

        Returns:
            OperationResult with success status
        """
        blocked = self._begin_write()
        if blocked:
            return blocked
        return self._run(lambda j: self._rename_measure(j, old_name, new_name, table_name, update_references),
                         "rename measure")

    def _update_measure(self, journal: _Journal, measure_name: str, new_expression: str,
                        table_name: Optional[str]) -> OperationResult:
        if not isinstance(new_expression, str) or not new_expression.strip():
            return OperationResult(False, "A non-empty DAX expression is required")
        if table_name and not self._find_table(table_name):
            return OperationResult(False, f"Table '{table_name}' not found")
        _, measure = self._find_measure(measure_name, table_name)
        if not measure:
            return OperationResult(False, f"Measure '{measure_name}' not found")
        old_expression = measure.Expression
        journal.set(measure, "Expression", new_expression)
        logger.info(f"Measure '{measure_name}' expression updated")
        return OperationResult(True, f"Measure '{measure_name}' expression updated",
                               {"old_expression": old_expression})

    @_locked
    def update_measure_expression(self, measure_name: str, new_expression: str,
                                  table_name: Optional[str] = None) -> OperationResult:
        """
        Update a measure's DAX expression (staged, not saved)

        Args:
            measure_name: Name of the measure
            new_expression: New DAX expression
            table_name: Optional table name

        Returns:
            OperationResult with success status
        """
        blocked = self._begin_write()
        if blocked:
            return blocked
        return self._run(lambda j: self._update_measure(j, measure_name, new_expression, table_name),
                         "update measure")

    @_locked
    def batch_rename_measures(self, renames: List[Dict[str, str]], auto_save: bool = True,
                              update_references: bool = True) -> OperationResult:
        """
        Batch rename multiple measures with automatic reference updates

        Args:
            renames: List of {"old_name": "...", "new_name": "...", "table_name": "..." (optional)} dicts
            auto_save: Save at the end (default: True). Ignored while a transaction is open.
            update_references: Whether to update references in other expressions (default: True)

        Returns:
            OperationResult with details of each rename and updated references
        """
        return self._batch(
            renames, auto_save, "rename", "measure", ("old_name", "new_name"),
            ("old_name", "new_name", "table_name"), "Missing name",
            lambda j, it: self._rename_measure(j, it["old_name"], it["new_name"], it.get("table_name"),
                                               update_references))

    @_locked
    def batch_update_measures(self, updates: List[Dict[str, str]], auto_save: bool = True) -> OperationResult:
        """
        Batch update multiple measure expressions

        Args:
            updates: List of {"measure_name": "...", "expression": "...",
                     "table_name": "..." (optional)} dicts
            auto_save: Save at the end (default: True). Ignored while a transaction is open.

        Returns:
            OperationResult with details of each update
        """
        return self._batch(
            updates, auto_save, "update", "measure", ("measure_name", "expression"), ("measure_name",),
            "Missing required field",
            lambda j, it: self._update_measure(j, it["measure_name"], it["expression"], it.get("table_name")),
            references=False)

    # ==================== CREATE OPERATIONS ====================

    def _new_measure(self, name: str, spec: Dict[str, Any]) -> Any:
        measure = TOM.Measure()
        measure.Name = name
        measure.Expression = spec["expression"]
        if spec.get("format_string"):
            measure.FormatString = spec["format_string"]
        if spec.get("description"):
            measure.Description = spec["description"]
        if spec.get("display_folder"):
            measure.DisplayFolder = spec["display_folder"]
        return measure

    def _measure_problem(self, table: Any, name: str) -> Optional[str]:
        """Why a measure called ``name`` cannot be added to ``table``, or None."""
        owner, existing = self._find_measure(name)
        if existing is not None:
            return f"already exists in table '{owner.Name}'"
        if table.Columns.Find(name) is not None:
            return f"a column of '{table.Name}' already has this name"
        return None

    @staticmethod
    def _engine_errors(measures: List[Any]) -> Dict[str, str]:
        """Error messages the engine attached to saved measures (it accepts a measure whose DAX
        does not resolve and marks it instead of rejecting the save)."""
        errors = {}
        for measure in measures:
            text = _opt(measure, "ErrorMessage")
            if isinstance(text, str) and text.strip():
                errors[str(measure.Name)] = text.strip()
        return errors

    @_locked
    def create_measure(self, table_name: str, measure_name: str, expression: str,
                       format_string: Optional[str] = None, description: Optional[str] = None,
                       display_folder: Optional[str] = None) -> OperationResult:
        """
        Create a new measure (staged, not saved)

        Args:
            table_name: Table to add the measure to
            measure_name: Name of the new measure
            expression: DAX expression
            format_string: Optional format string
            description: Optional description
            display_folder: Optional display folder (use \\ for nesting)

        Returns:
            OperationResult with success status
        """
        blocked = self._begin_write()
        if blocked:
            return blocked

        def create(journal: _Journal) -> OperationResult:
            name = self._checked_name(measure_name, "Measure name").strip()
            if not isinstance(expression, str) or not expression.strip():
                return OperationResult(False, "A non-empty DAX expression is required")
            table = self._find_table(table_name)
            if not table:
                return OperationResult(False, f"Table '{table_name}' not found")
            problem = self._measure_problem(table, name)
            if problem:
                return OperationResult(False, f"Measure '{name}' cannot be created: {problem}")
            journal.add(table.Measures, self._new_measure(name, {
                "expression": expression, "format_string": format_string,
                "description": description, "display_folder": display_folder}))
            logger.info(f"Created measure: '{name}' in table '{table.Name}'")
            return OperationResult(True, f"Created measure: '{name}' in table '{table.Name}'")

        return self._run(create, "create measure")

    @_locked
    def batch_create_measures(self, table_name: str, measures: List[Dict[str, Any]],
                              auto_save: bool = True) -> OperationResult:
        """
        Create many measures on one table: all of them, or none.

        The whole batch is checked BEFORE anything is added: the table exists; every entry has a
        name and an expression; no name repeats within the batch, belongs to an existing measure
        anywhere in the model, or equals a column of the target table (the engine rejects all
        three). If adding fails part-way, the measures already added by this call are removed
        again. If the save is rejected, they are undone as well. Inside a transaction the batch
        is staged and saved by commit_transaction(); ``auto_save`` is ignored there.

        Args:
            table_name: Table to add the measures to
            measures: [{name, expression, format_string?, description?, display_folder?}]
            auto_save: Save at the end (default: True)
        """
        blocked = self._begin_write()
        if blocked:
            return blocked
        if not measures:
            return OperationResult(False, "No measures provided")
        table = self._find_table(table_name)
        if not table:
            return OperationResult(False, f"Table '{table_name}' not found")

        # Validate the whole batch up front.
        problems: List[str] = []
        specs: List[Tuple[str, Dict[str, Any]]] = []
        seen = set()
        for i, m in enumerate(measures):
            if not isinstance(m, dict):
                problems.append(f"#{i + 1}: not an object")
                continue
            name = m.get("name").strip() if isinstance(m.get("name"), str) else ""
            expression = m.get("expression")
            if not name or not isinstance(expression, str) or not expression.strip():
                problems.append(f"#{i + 1}: needs both a name and an expression")
                continue
            if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
                problems.append(f"#{i + 1}: the name contains a control character")
                continue
            if name.casefold() in seen:
                problems.append(f"'{name}': duplicated within the batch")
            seen.add(name.casefold())
            problem = self._measure_problem(table, name)
            if problem:
                problems.append(f"'{name}': {problem}")
            for option in ("format_string", "description", "display_folder"):
                if m.get(option) is not None and not isinstance(m.get(option), str):
                    problems.append(f"'{name}': {option} must be text")
            specs.append((name, m))
        if problems:
            return OperationResult(
                False, "Batch rejected; nothing was created:\n  - " + "\n  - ".join(problems))

        added: List[Any] = []

        def create(journal: _Journal) -> OperationResult:
            for name, spec in specs:
                measure = self._new_measure(name, spec)
                journal.add(table.Measures, measure)
                added.append(measure)
            return OperationResult(True, "staged")

        staged = self._run(create, "batch-create measures")
        if not staged.success:
            return OperationResult(False, f"{staged.message} Nothing was created.")

        created = [name for name, _ in specs]
        failed_save = self._autosave(auto_save)
        if failed_save is not None:
            return OperationResult(False, f"Batch not created: {failed_save.message}",
                                   {"created": [], "saved": False})
        saved = bool(auto_save) and not self._txn_open
        details: Dict[str, Any] = {"created": created, "saved": saved, "pending": not saved}
        if saved:
            errors = self._engine_errors(added)
            if errors:
                details["engine_errors"] = errors
        logger.info(f"Created {len(created)} measures in table '{table.Name}'")
        return OperationResult(
            True, f"Created {len(created)} measure(s) in '{table.Name}': " + ", ".join(created)
                  + (self._pending_note() if not saved else ""),
            details=details)

    @_locked
    def delete_measure(self, measure_name: str, table_name: Optional[str] = None) -> OperationResult:
        """
        Delete a measure (staged, not saved)

        Args:
            measure_name: Name of the measure to delete
            table_name: Optional table name

        Returns:
            OperationResult with success status
        """
        blocked = self._begin_write()
        if blocked:
            return blocked

        def delete(journal: _Journal) -> OperationResult:
            if table_name and not self._find_table(table_name):
                return OperationResult(False, f"Table '{table_name}' not found")
            found_table, measure = self._find_measure(measure_name, table_name)
            if not measure:
                return OperationResult(False, f"Measure '{measure_name}' not found")
            found_table.Measures.Remove(measure)
            self._changes_pending = True
            logger.info(f"Deleted measure: '{measure_name}'")
            return OperationResult(True, f"Deleted measure: '{measure_name}'")

        return self._run(delete, "delete measure")

    # ==================== RELATIONSHIPS ====================

    @_locked
    def create_relationship(self, from_table: str, from_column: str, to_table: str, to_column: str,
                            cardinality: str = "many_to_one", cross_filter: str = "single",
                            is_active: bool = True) -> OperationResult:
        """Create a relationship between two columns (staged, not saved).

        cardinality: many_to_one | one_to_many | one_to_one | many_to_many
        cross_filter: single | both
        """
        blocked = self._begin_write()
        if blocked:
            return blocked

        def create(journal: _Journal) -> OperationResult:
            ft = self._find_table(from_table)
            tt = self._find_table(to_table)
            if not ft:
                return OperationResult(False, f"From table '{from_table}' not found")
            if not tt:
                return OperationResult(False, f"To table '{to_table}' not found")
            fc = ft.Columns.Find(from_column)
            tc = tt.Columns.Find(to_column)
            if not fc:
                return OperationResult(False, f"Column '{from_table}'[{from_column}] not found")
            if not tc:
                return OperationResult(False, f"Column '{to_table}'[{to_column}] not found")

            many = TOM.RelationshipEndCardinality.Many
            one = TOM.RelationshipEndCardinality.One
            ends = {
                "many_to_one": (many, one), "*:1": (many, one), "m:1": (many, one),
                "one_to_many": (one, many), "1:*": (one, many), "1:m": (one, many),
                "one_to_one": (one, one), "1:1": (one, one),
                "many_to_many": (many, many), "m:m": (many, many), "*:*": (many, many),
            }
            card = (cardinality or "many_to_one").lower()
            if card not in ends:
                return OperationResult(False, f"Unknown cardinality '{cardinality}'; use many_to_one, "
                                              "one_to_many, one_to_one or many_to_many")
            cf = (cross_filter or "single").lower()
            if cf not in ("single", "one", "one_direction", "both", "bidirectional", "both_directions"):
                return OperationResult(False, f"Unknown cross_filter '{cross_filter}'; use single or both")

            rel = TOM.SingleColumnRelationship()
            rel.FromColumn = fc
            rel.ToColumn = tc
            rel.FromCardinality, rel.ToCardinality = ends[card]
            both = cf in ("both", "bidirectional", "both_directions")
            rel.CrossFilteringBehavior = (TOM.CrossFilteringBehavior.BothDirections if both
                                          else TOM.CrossFilteringBehavior.OneDirection)
            rel.IsActive = bool(is_active)
            journal.add(self.model.Relationships, rel)
            logger.info(f"Created relationship {from_table}[{from_column}] -> {to_table}[{to_column}]")
            return OperationResult(
                True,
                f"Created relationship {from_table}[{from_column}] -> {to_table}[{to_column}] ({card}, {cf})",
            )

        return self._run(create, "create relationship")

    @_locked
    def delete_relationship(self, from_table: Optional[str] = None, from_column: Optional[str] = None,
                            to_table: Optional[str] = None, to_column: Optional[str] = None,
                            name: Optional[str] = None) -> OperationResult:
        """Delete a relationship, identified by name or by from/to table[/column] (staged, not
        saved). Several matches are refused rather than deleting the first one found."""
        blocked = self._begin_write()
        if blocked:
            return blocked

        def delete(journal: _Journal) -> OperationResult:
            if not name and not (from_table and to_table):
                return OperationResult(
                    False, "Identify the relationship by name, or by from_table and to_table")
            matches = []
            for rel in self.model.Relationships:
                if name:
                    if _opt(rel, "Name") == name:
                        matches.append(rel)
                    continue
                # Only single-column relationships expose these members; others cannot match.
                f_table, t_table = _opt(_opt(rel, "FromTable"), "Name"), _opt(_opt(rel, "ToTable"), "Name")
                f_col, t_col = _opt(_opt(rel, "FromColumn"), "Name"), _opt(_opt(rel, "ToColumn"), "Name")
                if (dt.names_equal(f_table, from_table) and dt.names_equal(t_table, to_table)
                        and (not from_column or dt.names_equal(f_col, from_column))
                        and (not to_column or dt.names_equal(t_col, to_column))):
                    matches.append(rel)
            if not matches:
                return OperationResult(False, "Relationship not found")
            if len(matches) > 1:
                return OperationResult(
                    False, f"{len(matches)} relationships match; add from_column / to_column "
                           "(or the relationship name) to pick one")
            self.model.Relationships.Remove(matches[0])
            self._changes_pending = True
            logger.info("Deleted relationship")
            return OperationResult(True, "Deleted relationship")

        return self._run(delete, "delete relationship")

    # ==================== UTILITY METHODS ====================

    @_locked
    def get_model_summary(self) -> Dict[str, Any]:
        """Get a summary of the model"""
        if not self._ensure_connected():
            return {"error": "Not connected"}

        try:
            tables = []
            total_measures = 0
            total_columns = 0

            for table in self.model.Tables:
                table_info = {
                    "name": table.Name,
                    "columns": [col.Name for col in table.Columns],
                    "measures": [m.Name for m in table.Measures],
                    "column_count": table.Columns.Count,
                    "measure_count": table.Measures.Count
                }
                tables.append(table_info)
                total_measures += table.Measures.Count
                total_columns += table.Columns.Count

            return {
                "database_name": self.database.Name if self.database else None,
                "table_count": len(tables),
                "total_columns": total_columns,
                "total_measures": total_measures,
                "tables": tables,
                "changes_pending": self._has_local_changes(),
                "in_transaction": self._txn_open,
            }

        except Exception as e:
            error = clean_error(e)
            logger.error(f"Failed to get model summary: {error}")
            return {"error": error}
