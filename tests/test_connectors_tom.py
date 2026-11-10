"""
Tests for the TOM write connector (src/powerbi_tom_connector.py) against a fake Tabular Object
Model: no .NET, no Power BI Desktop.

The fake model behaves like TOM where it matters for these tests: collections find names
case-insensitively and refuse duplicates, SaveChanges is all-or-nothing, UndoLocalChanges
restores the last saved state, HasLocalChanges reflects unsaved edits, and errors carry a .NET
style stack trace.

Run: python tests/test_connectors_tom.py
"""
import copy
import logging
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import adomd_loader  # noqa: E402

# The connector loads TOM when imported. Tests must not start a .NET runtime.
_NOT_LOADED = adomd_loader.LoadResult(name="TOM", runtime="netfx")
adomd_loader.load_tom = lambda force=False: _NOT_LOADED

import powerbi_tom_connector as tomc  # noqa: E402

# The connectors log every failure these tests provoke on purpose; keep that off stderr.
logging.getLogger().addHandler(logging.NullHandler())

_failures = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f": {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def eq(name, actual, expected):
    check(name, actual == expected, f"got {actual!r}, expected {expected!r}")


STACK = "\r\n   at Microsoft.AnalysisServices.Tabular.Model.SaveChanges()\r\n   at Program.Main()"


# ---------------------------------------------------------------- fake TOM

class Coll(list):
    """Named collection: case-insensitive Find, duplicate names refused (as TOM does)."""
    fail_on_add = None

    def Find(self, name):
        return next((x for x in self if str(x.Name).casefold() == str(name).casefold()), None)

    def Add(self, item):
        name = getattr(item, "Name", None)
        if name is not None and (name == self.fail_on_add):
            raise Exception(f"The engine refused '{name}'{STACK}")
        if name is not None and self.Find(name) is not None:
            raise Exception(f"Item '{name}' already exists in the collection.{STACK}")
        self.append(item)

    def Remove(self, item):
        self.remove(item)

    @property
    def Count(self):
        return len(self)


class Named:
    """Object whose Name setter can be told to fail, like an engine-side name validation."""
    reject_name = None

    def __init__(self, name):
        self._name = name

    @property
    def Name(self):
        return self._name

    @Name.setter
    def Name(self, value):
        if value == self.reject_name:
            raise Exception(f"'{value}' is not a valid name{STACK}")
        self._name = value


class Measure(Named):
    def __init__(self, name=None, expression=None, kpi=None):
        super().__init__(name)
        self.Expression = expression
        self.KPI = kpi
        self.Description = None
        self.DisplayFolder = None
        self.ErrorMessage = ""
        self._format = None

    @property
    def FormatString(self):
        return self._format

    @FormatString.setter
    def FormatString(self, value):
        if not isinstance(value, str):
            raise TypeError("'int' value cannot be converted to System.String")
        self._format = value


class KPI:
    def __init__(self, target="", status="", trend=""):
        self.TargetExpression, self.StatusExpression, self.TrendExpression = target, status, trend


class Column(Named):
    def __init__(self, name, expression=None):
        super().__init__(name)
        self.Type = "Calculated" if expression is not None else "Data"
        if expression is not None:
            self.Expression = expression


class Partition:
    def __init__(self, source_type, expression):
        self.SourceType = source_type
        self.Source = types.SimpleNamespace(Expression=expression)


class CalcItem(Named):
    def __init__(self, name, expression):
        super().__init__(name)
        self.Expression = expression


class Table(Named):
    def __init__(self, name, columns=(), measures=(), partitions=(), items=None):
        super().__init__(name)
        self.Columns = Coll(columns)
        self.Measures = Coll(measures)
        self.Partitions = list(partitions)
        self.CalculationGroup = (types.SimpleNamespace(CalculationItems=Coll(items))
                                 if items is not None else None)


class Role(Named):
    def __init__(self, name, permissions):
        super().__init__(name)
        self.TablePermissions = [types.SimpleNamespace(Table=t, FilterExpression=e) for t, e in permissions]


class Relationship:
    Name = None

    def __init__(self, from_column=None, to_column=None, from_table=None, to_table=None):
        self.FromColumn, self.ToColumn = from_column, to_column
        self._from_table, self._to_table = from_table, to_table
        self.FromCardinality = self.ToCardinality = self.CrossFilteringBehavior = self.IsActive = None

    @property
    def FromTable(self):
        return self._from_table

    @property
    def ToTable(self):
        return self._to_table


class Model:
    def __init__(self, tables, roles=(), relationships=()):
        self.Tables = Coll(tables)
        self.Roles = list(roles)
        self.Relationships = Coll(relationships)
        self.save_error = None          # text -> SaveChanges raises it
        self.undo_error = None
        self.sync_error = None
        self.on_sync = None             # callable(model): what changed on the server meanwhile
        self.saves = self.undos = self.syncs = 0
        self._remember()

    def _state(self):
        return (self.Tables, self.Roles, self.Relationships)

    def _describe(self):
        def expressions(t):
            out = [(m.Name, m.Expression, m.KPI and (m.KPI.TargetExpression, m.KPI.StatusExpression))
                   for m in t.Measures]
            out += [(c.Name, getattr(c, "Expression", None)) for c in t.Columns]
            out += [p.Source.Expression for p in t.Partitions]
            if t.CalculationGroup:
                out += [(i.Name, i.Expression) for i in t.CalculationGroup.CalculationItems]
            return out
        return repr([(t.Name, expressions(t)) for t in self.Tables]
                    + [(r.Name, [p.FilterExpression for p in r.TablePermissions]) for r in self.Roles]
                    + [len(self.Relationships)])

    def _remember(self):
        self._saved = copy.deepcopy(self._state())
        self._saved_text = self._describe()

    @property
    def HasLocalChanges(self):
        return self._describe() != self._saved_text

    def SaveChanges(self):
        self.saves += 1
        if self.save_error:
            raise Exception(self.save_error + STACK)
        for table in self.Tables:
            for measure in table.Measures:
                broken = "BAD(" in (measure.Expression or "")
                measure.ErrorMessage = "Semantic error: cannot resolve" if broken else ""
        self._remember()

    def UndoLocalChanges(self):
        self.undos += 1
        if self.undo_error:
            raise Exception(self.undo_error + STACK)
        self.Tables, self.Roles, self.Relationships = copy.deepcopy(self._saved)

    def Sync(self):
        self.syncs += 1
        if self.sync_error:
            raise Exception(self.sync_error + STACK)
        if self.on_sync:
            self.on_sync(self)
            self._remember()


class _List(list):
    @property
    def Count(self):
        return len(self)


INSTANCES = {}      # port -> list of models (one per database)
SERVERS = []


class Server:
    def __init__(self):
        self.connected_to = None
        self.disconnects = 0
        self.Databases = _List()
        SERVERS.append(self)

    def Connect(self, data_source):
        port = int(str(data_source).rsplit(":", 1)[1])
        if port not in INSTANCES:
            raise Exception(f"A connection cannot be made to localhost:{port}{STACK}")
        self.connected_to = port
        self.Databases = _List(types.SimpleNamespace(Name=f"db{i}", Model=m)
                               for i, m in enumerate(INSTANCES[port]))

    def Disconnect(self):
        self.disconnects += 1


tomc.TOM = types.SimpleNamespace(
    Server=Server, Measure=Measure, SingleColumnRelationship=Relationship,
    RelationshipEndCardinality=types.SimpleNamespace(Many="Many", One="One"),
    CrossFilteringBehavior=types.SimpleNamespace(BothDirections="Both", OneDirection="One"))
tomc._tom_available = True


def sample_model():
    sales = Table("Sales", columns=[
        Column("Amount"), Column("Qty"), Column("Price"),
        Column("Line Total", expression="[Qty] * Sales[Price]"),
    ], measures=[
        Measure("Total Sales", "SUM(Sales[Amount])",
                kpi=KPI("[Total Sales] * 1.1", "IF([Total Sales] > 0, 1, -1)")),
        Measure("Rows", 'COUNTROWS(Sales) + COUNTROWS(FILTER(Sales, Sales[Qty] > 1)) -- Sales rows\n'
                        '+ IF(TRUE(), 0, LEN("Sales[Amount] in a string"))'),
    ], partitions=[Partition("M", "let Source = Sales in Source")])
    measures = Table("_Measures", measures=[
        Measure("Amount", "SUM(Sales[Amount])"),
        Measure("Amount YTD", "TOTALYTD([Amount], 'Date'[Date]) + SUM(Sales[Amount]) + _Measures[Amount]"),
    ])
    date = Table("Date", columns=[Column("Date"), Column("Year")])
    top = Table("Top Sales", columns=[Column("Amount")],
                partitions=[Partition("Calculated", "FILTER(Sales, Sales[Amount] > 100)")])
    calc = Table("Time Calc", items=[CalcItem("Sales YTD", "CALCULATE(SELECTEDMEASURE(), ALL(Sales))")])
    model = Model([sales, measures, date, top, calc])
    model.Roles = [Role("East", [(sales, "[Amount] > 0 && Sales[Qty] > 0")])]
    model.Relationships = Coll([Relationship(sales.Columns[0], date.Columns[0], sales, date)])
    model._remember()
    return model


def connect(model=None, port=5001):
    SERVERS.clear()
    INSTANCES.clear()
    model = model or sample_model()
    INSTANCES[port] = [model]
    connector = tomc.PowerBITOMConnector()
    check("fixture: connect", connector.connect(port) is True, str(connector.last_error))
    return connector, model


def measure(model, name):
    for table in model.Tables:
        found = table.Measures.Find(name)
        if found is not None:
            return found
    return None


def names(model, table):
    return [m.Name for m in model.Tables.Find(table).Measures]


# ---------------------------------------------------------------- transactions and saving

def test_transaction_defers_every_save():
    print("\n== transaction: nothing saves until commit ==")
    c, m = connect()
    check("not in a transaction at first", c.in_transaction is False)
    started = c.begin_transaction()
    check("begin", started.success and c.in_transaction is True, started.message)
    check("begin twice is refused", c.begin_transaction().success is False)

    check("create_measure stages", c.create_measure("Sales", "M1", "1").success)
    deferred = c.save_changes()
    check("save_changes() inside a transaction is deferred, not executed",
          deferred.success and deferred.details.get("deferred") is True and m.saves == 0, deferred.message)
    r = c.batch_rename_measures([{"old_name": "Rows", "new_name": "Row Count"}], auto_save=True)
    check("deprecated batch rename with auto_save=True does not save", r.success and m.saves == 0, r.message)
    check("  and says the edit is pending",
          r.details["pending"] is True and "commit_transaction" in r.message)
    r = c.batch_rename_tables([{"old_name": "Date", "new_name": "Calendar"}], auto_save=True)
    check("batch_rename_tables does not save", r.success and m.saves == 0)
    r = c.batch_rename_columns([{"table_name": "Sales", "old_name": "Price", "new_name": "Unit Price"}])
    check("batch_rename_columns does not save", r.success and m.saves == 0)
    r = c.batch_update_measures([{"measure_name": "M1", "expression": "2"}], auto_save=True)
    check("batch_update_measures does not save", r.success and m.saves == 0)
    r = c.batch_create_measures("Sales", [{"name": "M2", "expression": "3"}], auto_save=True)
    check("batch_create_measures does not save", r.success and m.saves == 0 and r.details["pending"] is True)
    check("delete_measure / relationships stage too",
          c.delete_measure("Amount YTD").success
          and c.create_relationship("Top Sales", "Amount", "Sales", "Amount").success and m.saves == 0)
    check("discard_changes() is refused inside a transaction", c.discard_changes().success is False
          and m.undos == 0)
    eq("the model was synced once (at begin), not before each staged edit", m.syncs, 1)

    committed = c.commit_transaction()
    check("commit saves once and closes", committed.success and m.saves == 1 and c.in_transaction is False,
          committed.message)
    eq("everything staged was in that one save", (names(m, "Sales"), m.Tables.Find("Calendar") is not None),
       (["Total Sales", "Row Count", "M1", "M2"], True))
    check("commit / rollback with no transaction say so",
          c.commit_transaction().success is False and c.rollback_transaction().success is False)


def test_failed_commit_keeps_transaction_open():
    print("\n== transaction: a failed commit stays open and can be rolled back ==")
    c, m = connect()
    before = m._describe()
    c.begin_transaction()
    c.create_measure("Sales", "Broken", "BAD(")
    c.create_measure("Sales", "Fine", "1")
    m.save_error = "Measure 'Broken': syntax error"
    failed = c.commit_transaction()
    check("commit reports the engine error", failed.success is False and "syntax error" in failed.message,
          failed.message)
    check("  without the .NET stack trace",
          "   at " not in failed.message and "SaveChanges()" not in failed.message)
    check("  the transaction is still open",
          c.in_transaction is True and failed.details["transaction_open"] is True)
    check("  the edits are still staged (nothing was undone behind the caller's back)",
          m.undos == 0 and "Broken" in names(m, "Sales") and c.has_pending_changes)

    # fix the cause and commit again
    c.delete_measure("Broken")
    m.save_error = None
    again = c.commit_transaction()
    check("after fixing the cause the same transaction commits", again.success and not c.in_transaction)
    eq("  with the good edit only", names(m, "Sales"), ["Total Sales", "Rows", "Fine"])

    c, m = connect()
    before = m._describe()
    c.begin_transaction()
    c.create_measure("Sales", "Broken", "BAD(")
    m.save_error = "rejected"
    c.commit_transaction()
    rolled = c.rollback_transaction()
    check("rollback after a failed commit works", rolled.success and not c.in_transaction, rolled.message)
    check("  via UndoLocalChanges, restoring the model", m.undos == 1 and m._describe() == before)
    check("  nothing pending afterwards", c.has_pending_changes is False)
    m.save_error = None
    check("a later, unrelated edit saves cleanly", c.create_measure("Sales", "Later", "1").success
          and c.save_changes().success and names(m, "Sales") == ["Total Sales", "Rows", "Later"])


def test_rollback():
    print("\n== transaction: rollback ==")
    c, m = connect()
    before = m._describe()
    c.begin_transaction()
    c.rename_table("Sales", "Orders")
    c.batch_create_measures("_Measures", [{"name": "X", "expression": "1"}])
    rolled = c.rollback_transaction()
    check("rolled back", rolled.success and m.undos == 1 and m.saves == 0 and m._describe() == before)

    c, m = connect()
    c.begin_transaction()
    c.create_measure("Sales", "X", "1")
    m.undo_error = "Cannot undo changes when the model is in disconnected state."
    fresh = sample_model()
    INSTANCES[5001] = [fresh]
    rolled = c.rollback_transaction()
    check("if undo is impossible the local model is dropped and re-read instead",
          rolled.success and not c.in_transaction and c.model is fresh, rolled.message)
    check("  so the staged edit is gone", "X" not in names(c.model, "Sales") and not c.has_pending_changes)

    c, m = connect()
    c.begin_transaction()
    c.create_measure("Sales", "X", "1")
    m.undo_error = "session lost"
    INSTANCES.clear()
    rolled = c.rollback_transaction()
    check("if even reconnecting fails, the transaction is closed and the connector says it is disconnected",
          rolled.success is False and not c.in_transaction and c.model is None
          and "Nothing was saved" in rolled.message, rolled.message)


def test_connect_rules():
    print("\n== connect: refused across an open transaction or staged edits ==")
    c, m = connect()
    other = sample_model()
    INSTANCES[5002] = [other]
    c.begin_transaction()
    c.create_measure("Sales", "Staged", "1")
    check("connecting to another instance is refused", c.connect(5002) is False)
    check("  with a reason", "transaction is open on port 5001" in (c.last_error or ""), str(c.last_error))
    check("  and the transaction, model and staged edit are untouched",
          c.in_transaction and c.model is m and "Staged" in names(m, "Sales") and c.current_port == 5001)
    check("reconnecting to the SAME port keeps the staged work", c.connect(5001) is True and c.model is m)
    c.rollback_transaction()
    check("after rollback the other instance can be used", c.connect(5002) is True and c.model is other)
    check("  and the old session was released", SERVERS[0].disconnects == 1)

    c, m = connect()
    INSTANCES[5002] = [sample_model()]
    c.create_measure("Sales", "Staged", "1")       # staged outside a transaction, not saved yet
    check("staged-but-unsaved edits also block switching instance", c.connect(5002) is False
          and "unsaved edits" in c.last_error)
    check("  until they are discarded", c.discard_changes().success and c.connect(5002) is True)

    SERVERS.clear()
    INSTANCES.clear()
    INSTANCES[6000] = [sample_model(), sample_model()]
    c = tomc.PowerBITOMConnector()
    check("a server with several databases is refused (it is not Power BI Desktop)", c.connect(6000) is False)
    check("  with a reason, and the session closed",
          "single database" in c.last_error and SERVERS[-1].disconnects == 1 and c.model is None,
          c.last_error)
    check("an unreachable port fails with a clean message",
          c.connect(6001) is False and "cannot be made" in c.last_error and "   at " not in c.last_error)
    check("a port that is not a port is refused", c.connect("5001;x") is False and "port" in c.last_error)
    eq("operations when not connected", c.create_measure("Sales", "M", "1").message, "Not connected")
    check("transaction methods when not connected", c.begin_transaction().success is False)

    c, m = connect()
    c.begin_transaction()
    c.disconnect()
    check("disconnect closes the transaction and forgets the model",
          not c.in_transaction and c.model is None and c.current_port is None)


def test_failed_save_outside_transaction_undoes():
    print("\n== outside a transaction: a failed save undoes the staged edit ==")
    c, m = connect()
    before = m._describe()
    check("stage",
          c.create_measure("Sales", "Amount2", "SUM(Sales[Amount])").success and c.has_pending_changes)
    m.save_error = "The name conflicts with an existing object"
    failed = c.save_changes()
    check("save fails with the engine message, no stack", failed.success is False
          and "conflicts" in failed.message and "   at " not in failed.message, failed.message)
    check("  and the staged edit was undone", failed.details["undone"] is True and m.undos == 1
          and m._describe() == before and not c.has_pending_changes)
    m.save_error = None
    ok = c.create_measure("Sales", "Other", "1")
    saved = c.save_changes()
    check("the rejected edit does not ride along with the next save",
          ok.success and saved.success and names(m, "Sales") == ["Total Sales", "Rows", "Other"])

    c, m = connect()
    c.create_measure("Sales", "X", "1")
    check("discard_changes undoes staged edits", c.discard_changes().success and m.undos == 1
          and "X" not in names(m, "Sales"))

    c, m = connect()
    c.create_measure("Sales", "X", "1")
    m.save_error, m.undo_error = "rejected", "cannot undo"
    fresh = sample_model()
    INSTANCES[5001] = [fresh]
    failed = c.save_changes()
    check("if undo fails too, the local model is replaced by a fresh one",
          failed.success is False and c.model is fresh and not c.has_pending_changes, failed.message)


# ---------------------------------------------------------------- staleness

def test_model_is_resynced_before_a_write():
    print("\n== the local model is re-synced before a write ==")
    c, m = connect()

    def someone_adds_a_measure(model):
        if model.Tables.Find("Sales").Measures.Find("Made In Desktop") is None:
            model.Tables.Find("Sales").Measures.append(Measure("Made In Desktop", "1"))
    m.on_sync = someone_adds_a_measure
    r = c.create_measure("_Measures", "made in desktop", "2")
    check("a measure created in Desktop after connect is seen (duplicate refused)",
          r.success is False and "already exists in table 'Sales'" in r.message, r.message)
    eq("  because the model was synced first", m.syncs, 1)
    c.create_measure("Sales", "A", "1")
    c.create_measure("Sales", "B", "1")
    eq("no sync while an edit is staged (it would discard or clash with it)", m.syncs, 2)
    c.save_changes()
    c.delete_measure("A")
    eq("synced again once nothing is staged", m.syncs, 3)

    c, m = connect()
    m.sync_error = "The session is no longer valid"
    fresh = sample_model()
    fresh.Tables.Find("Sales").Measures.append(Measure("Newer", "1"))
    fresh._remember()
    INSTANCES[5001] = [fresh]
    r = c.create_measure("Sales", "Newer", "2")
    check("when Sync fails a reconnect provides the fresh copy",
          c.model is fresh and r.success is False and "already exists" in r.message, r.message)

    c, m = connect()
    m.sync_error = "The session is no longer valid"
    INSTANCES.clear()
    r = c.create_measure("Sales", "Z", "1")
    check("when neither works the write is refused with the reason",
          r.success is False and "Could not refresh the model" in r.message and "   at " not in r.message,
          r.message)
    check("  and nothing was staged on a stale copy", "Z" not in names(m, "Sales"))


# ---------------------------------------------------------------- batch create

def test_batch_create_all_or_nothing():
    print("\n== batch_create_measures: all or nothing ==")
    c, m = connect()
    before = m._describe()
    r = c.batch_create_measures("Sales", [
        {"name": "New One", "expression": "1"},
        {"name": "qty", "expression": "SUM(Sales[Qty])"},            # a COLUMN of Sales
        {"name": "AMOUNT YTD", "expression": "1"},                    # a measure of another table
        {"name": "Dup", "expression": "1"}, {"name": " dup ", "expression": "2"},
        {"name": "", "expression": "1"}, {"name": "NoExpr"},
        {"name": "Fmt", "expression": "1", "format_string": 123},
    ])
    check("rejected before anything is added", r.success is False and "nothing was created" in r.message
          and m._describe() == before and m.saves == 0, r.message)
    for expected in ("'qty': a column of 'Sales' already has this name",
                     "'AMOUNT YTD': already exists in table '_Measures'",
                     "'dup': duplicated within the batch", "#6: needs both a name and an expression",
                     "#7: needs both a name and an expression", "'Fmt': format_string must be text"):
        check(f"  reports: {expected}", expected in r.message, r.message)
    check("an empty batch / unknown table are refused",
          c.batch_create_measures("Sales", []).success is False
          and c.batch_create_measures("Nope", [{"name": "a", "expression": "1"}]).success is False)

    c, m = connect()
    r = c.batch_create_measures("Sales", [
        {"name": " Margin ", "expression": "1", "format_string": "0.0%", "description": "d",
         "display_folder": "KPIs"},
        {"name": "Ratio", "expression": "DIVIDE([Margin], 2)"}])
    check("created and saved once", r.success and m.saves == 1 and r.details["saved"] is True, r.message)
    eq("  names are trimmed", (r.details["created"], names(m, "Sales")[-2:]), (["Margin", "Ratio"],) * 2)
    created = measure(m, "Margin")
    eq("  optional properties applied", (created.FormatString, created.Description, created.DisplayFolder),
       ("0.0%", "d", "KPIs"))

    c, m = connect()
    before = m._describe()
    m.Tables.Find("Sales").Measures.fail_on_add = "M3"
    r = c.batch_create_measures("Sales", [{"name": f"M{i}", "expression": str(i)} for i in (1, 2, 3, 4)])
    check("a failure while adding the third measure removes the first two again",
          r.success is False and m._describe() == before and "Nothing was created" in r.message, r.message)
    check("  nothing is left pending for a later save to pick up",
          not c.has_pending_changes and m.saves == 0 and "   at " not in r.message)

    c, m = connect()
    c.begin_transaction()
    c.create_measure("Sales", "Earlier", "1")
    m.Tables.Find("Sales").Measures.fail_on_add = "M2"
    r = c.batch_create_measures("Sales", [{"name": "M1", "expression": "1"},
                                          {"name": "M2", "expression": "2"}])
    check("inside a transaction only the batch's own additions are removed",
          r.success is False and names(m, "Sales") == ["Total Sales", "Rows", "Earlier"] and c.in_transaction)

    c, m = connect()
    before = m._describe()
    m.save_error = "Measure 'B' collides"
    r = c.batch_create_measures("Sales", [{"name": "A", "expression": "1"}, {"name": "B", "expression": "2"}])
    check("a rejected save leaves none of the batch behind",
          r.success is False and r.details["created"] == [] and m._describe() == before
          and not c.has_pending_changes, r.message)

    c, m = connect()
    r = c.batch_create_measures("Sales", [{"name": "Good", "expression": "1"},
                                          {"name": "Odd", "expression": "BAD(x)"}])
    check("measures the engine saved but marked as erroneous are reported",
          r.success and r.details.get("engine_errors") == {"Odd": "Semantic error: cannot resolve"},
          repr(r.details))
    r = c.batch_create_measures("Sales", [{"name": "Staged", "expression": "1"}], auto_save=False)
    check("auto_save=False stages without saving",
          r.success and r.details["pending"] is True and m.saves == 1)


def test_create_update_delete():
    print("\n== single measure operations ==")
    c, m = connect()
    eq("create refuses a column's name",
       c.create_measure("Sales", "Qty", "1").message,
       "Measure 'Qty' cannot be created: a column of 'Sales' already has this name")
    check("create refuses an existing measure anywhere in the model",
          "already exists in table '_Measures'" in c.create_measure("Sales", "amount", "1").message)
    check("create refuses a blank name / expression / unknown table",
          not c.create_measure("Sales", " ", "1").success and not c.create_measure("Sales", "X", " ").success
          and not c.create_measure("Nope", "X", "1").success)
    r = c.create_measure("Sales", "X", "1", format_string=5)
    check("a property the engine rejects fails cleanly and adds nothing",
          r.success is False and "X" not in names(m, "Sales") and "   at " not in r.message
          and not c.has_pending_changes, r.message)
    check("create stages; save persists", c.create_measure("Sales", "X", "1", display_folder="F").success
          and m.saves == 0 and c.save_changes().success and m.saves == 1)

    r = c.update_measure_expression("X", "2")
    check("update returns the old expression", r.success and r.details == {"old_expression": "1"})
    check("update of a missing measure / blank expression fails",
          not c.update_measure_expression("Nope", "1").success
          and not c.update_measure_expression("X", "").success)
    r = c.batch_update_measures([{"measure_name": "X", "expression": "3"},
                                 {"measure_name": "Nope", "expression": "1"}, {"measure_name": "Rows"}])
    eq("batch update: per-item results", [(i["measure_name"], i["success"]) for i in r.details["results"]],
       [("X", True), ("Nope", False), ("Rows", False)])
    eq("  message and one save", (r.message, m.saves), ("Updated 1 measure(s), 2 failed", 2))
    check("delete",
          c.delete_measure("X").success and c.save_changes().success and "X" not in names(m, "Sales"))
    check("delete of a missing measure / table fails",
          not c.delete_measure("Nope").success and not c.delete_measure("Rows", "Nope").success)


# ---------------------------------------------------------------- renames

def test_rename_table():
    print("\n== rename table: every kind of DAX is rewritten, nothing else ==")
    c, m = connect()
    r = c.rename_table("Sales", "Net Sales")
    check("renamed", r.success, r.message)
    eq("qualified reference gets the quotes the new name needs",
       measure(m, "Amount").Expression, "SUM('Net Sales'[Amount])")
    eq("bare table references, a comment and a string literal",
       measure(m, "Rows").Expression,
       "COUNTROWS('Net Sales') + COUNTROWS(FILTER('Net Sales', 'Net Sales'[Qty] > 1)) -- Sales rows\n"
       '+ IF(TRUE(), 0, LEN("Sales[Amount] in a string"))')
    eq("calculated column", m.Tables.Find("Net Sales").Columns.Find("Line Total").Expression,
       "[Qty] * 'Net Sales'[Price]")
    eq("calculated table", m.Tables.Find("Top Sales").Partitions[0].Source.Expression,
       "FILTER('Net Sales', 'Net Sales'[Amount] > 100)")
    eq("an M partition is not DAX and is left alone",
       m.Tables.Find("Net Sales").Partitions[0].Source.Expression, "let Source = Sales in Source")
    eq("calculation item", m.Tables.Find("Time Calc").CalculationGroup.CalculationItems[0].Expression,
       "CALCULATE(SELECTEDMEASURE(), ALL('Net Sales'))")
    eq("RLS filter", m.Roles[0].TablePermissions[0].FilterExpression, "[Amount] > 0 && 'Net Sales'[Qty] > 0")
    eq("KPI expressions (no table reference in them) untouched",
       measure(m, "Total Sales").KPI.TargetExpression, "[Total Sales] * 1.1")
    d = r.details
    eq("details list each kind",
       (sorted(d["updated_measures"]), d["updated_calculated_columns"], d["updated_calculated_tables"],
        d["updated_calculation_items"], d["updated_rls_filters"]),
       (["Sales[Rows]", "Sales[Total Sales]", "_Measures[Amount YTD]", "_Measures[Amount]"],
        ["Sales[Line Total]"], ["Top Sales (calculated table)"], ["Time Calc[Sales YTD]"],
        ["role 'East' on Sales"]))
    check("calculated objects are flagged as needing recalculation", d.get("recalculation_needed") is True)
    check("the rename is staged, not saved", m.saves == 0 and c.has_pending_changes)

    c, m = connect()
    c.rename_table("Sales", "Kid's Sales")
    eq("an apostrophe in the new name is escaped",
       measure(m, "Amount").Expression, "SUM('Kid''s Sales'[Amount])")
    c, m = connect()
    c.rename_table("Sales", "Orders")
    eq("a simple new name stays unquoted where the old one was", measure(m, "Amount").Expression,
       "SUM(Orders[Amount])")

    c, m = connect()
    eq("collision is case-insensitive",
       c.rename_table("Sales", "DATE").message, "Table 'DATE' already exists")
    check("a case-only rename of the same table is allowed", c.rename_table("Sales", "SALES").success
          and m.Tables.Find("sales").Name == "SALES")
    check("unknown table / blank name / same name refused",
          not c.rename_table("Nope", "X").success and not c.rename_table("Date", " ").success
          and not c.rename_table("Date", "Date").success)
    r = c.rename_table("Date", "Calendar", update_references=False)
    check("update_references=False renames only the object",
          r.success and measure(m, "Amount YTD").Expression.count("'Date'[Date]") == 1)


def test_rename_is_atomic():
    print("\n== rename: a failure half-way reverts its own edits ==")
    c, m = connect()
    before = m._describe()
    m.Tables.Find("Sales").reject_name = "Bad:Name"
    r = c.rename_table("Sales", "Bad:Name")
    check("the engine rejects the new name", r.success is False and "not a valid name" in r.message
          and "   at " not in r.message, r.message)
    check("the expressions rewritten before the failure are restored", m._describe() == before)
    check("nothing is pending", not c.has_pending_changes)

    c, m = connect()
    c.begin_transaction()
    c.create_measure("Sales", "Keep", "1")
    staged = m._describe()
    m.Tables.Find("Sales").reject_name = "Bad:Name"
    c.rename_table("Sales", "Bad:Name")
    check("inside a transaction the earlier staged edit survives the failed rename",
          m._describe() == staged and "Keep" in names(m, "Sales") and c.in_transaction)

    c, m = connect()
    before = m._describe()
    m.save_error = "rename rejected by the engine"
    r = c.batch_rename_tables([{"old_name": "Sales", "new_name": "Orders"},
                               {"old_name": "Nope", "new_name": "X"}, {"old_name": "Date"}])
    check("batch: a failed save applies nothing", r.success is False and m._describe() == before, r.message)
    eq("  and no item is reported as applied", [i["success"] for i in r.details["results"]], [False] * 3)

    c, m = connect()
    r = c.batch_rename_tables([{"old_name": "Sales", "new_name": "Orders"},
                               {"old_name": "Nope", "new_name": "X"}, {"old_name": "Date"}])
    eq("batch: per-item outcome", [(i["old_name"], i["success"], i["error"]) for i in r.details["results"]],
       [("Sales", True, None), ("Nope", False, "Table 'Nope' not found"), ("Date", False, "Missing name")])
    check("  message, counts and one save",
          r.message.startswith("Renamed 1 table(s), 2 failed. Updated 8 model references.")
          and r.details["success_count"] == 1 and r.details["total_updated_measures"] == 4 and m.saves == 1,
          r.message)


def test_rename_measure():
    print("\n== rename measure: same-named columns are not touched ==")
    c, m = connect()
    m.Tables.Find("Sales").Columns.append(Column("Scaled", expression="[Amount] * 2"))
    m._remember()
    r = c.rename_measure("Amount", "Total Amount")
    check("renamed", r.success and measure(m, "Total Amount") is not None, r.message)
    eq("[Amount] -> the measure; Sales[Amount] stays the column; _Measures[Amount] is the measure",
       measure(m, "Amount YTD").Expression,
       "TOTALYTD([Total Amount], 'Date'[Date]) + SUM(Sales[Amount]) + _Measures[Total Amount]")
    eq("column reference in another measure untouched",
       measure(m, "Total Sales").Expression, "SUM(Sales[Amount])")
    eq("unqualified [Amount] in the row context of the table that has that column is left alone",
       m.Tables.Find("Sales").Columns.Find("Scaled").Expression, "[Amount] * 2")
    eq("  same for that table's RLS filter", m.Roles[0].TablePermissions[0].FilterExpression,
       "[Amount] > 0 && Sales[Qty] > 0")
    eq("  and they are reported for review", sorted(r.details["ambiguous_references"]),
       ["Sales[Scaled]", "role 'East' on Sales"])
    eq("  along with the columns sharing the name", r.details["columns_with_same_name"],
       ["Sales[Amount]", "Top Sales[Amount]"])

    c, m = connect()
    c.rename_measure("Total Sales", "Sales [net]")
    eq("KPI expressions follow, with ] escaped", measure(m, "Sales [net]").KPI.TargetExpression,
       "[Sales [net]]] * 1.1")
    c, m = connect()
    eq("collision with a measure of another table", c.rename_measure("Rows", "amount ytd").message,
       "Measure 'amount ytd' already exists in table '_Measures'")
    check("collision with a column of the home table",
          "column named 'Qty'" in c.rename_measure("Rows", "Qty").message)
    check("unknown measure / table",
          not c.rename_measure("Nope", "X").success and not c.rename_measure("Rows", "X", "Nope").success)
    r = c.batch_rename_measures([{"old_name": "Rows", "new_name": "Row Count"},
                                 {"old_name": "Nope", "new_name": "Y"}])
    eq("batch message", r.message, "Renamed 1 measure(s), 1 failed. Updated 0 references.")
    check("  saved once, with a table name echoed", m.saves == 1 and "table_name" in r.details["results"][0])


def test_rename_column():
    print("\n== rename column: qualified always, unqualified only when unambiguous ==")
    c, m = connect()
    r = c.rename_column("Sales", "Qty", "Quantity")
    check("renamed", r.success and m.Tables.Find("Sales").Columns.Find("Quantity") is not None, r.message)
    eq("own-table calculated column: unqualified [Qty] rewritten",
       m.Tables.Find("Sales").Columns.Find("Line Total").Expression, "[Quantity] * Sales[Price]")
    eq("RLS filter on the table", m.Roles[0].TablePermissions[0].FilterExpression,
       "[Amount] > 0 && Sales[Quantity] > 0")
    check("measure with a qualified reference", "Sales[Quantity] > 1" in measure(m, "Rows").Expression)

    c, m = connect()
    measure(m, "Rows").Expression = 'SUMX(ADDCOLUMNS(Date, "Price", 1), [Price]) + SUMX(Sales, [Price])'
    m._remember()
    r = c.rename_column("Sales", "Price", "Unit Price")
    eq("an expression that defines its own \"Price\" column keeps its unqualified [Price]",
       measure(m, "Rows").Expression, 'SUMX(ADDCOLUMNS(Date, "Price", 1), [Price]) + SUMX(Sales, [Price])')
    eq("  and is reported", r.details["unqualified_references_left"], ["Sales[Rows]"])
    check("  the message asks for a review", "review" in r.message)

    c, m = connect()
    r = c.rename_column("Sales", "Amount", "Net Amount")
    eq("qualified references rewritten everywhere",
       measure(m, "Total Sales").Expression, "SUM(Sales[Net Amount])")
    eq("another table's same-named column is a different column",
       m.Tables.Find("Top Sales").Columns.Find("Amount").Name, "Amount")
    eq("a measure called [Amount] exists, so unqualified [Amount] is not assumed to be the column",
       (m.Roles[0].TablePermissions[0].FilterExpression, measure(m, "Amount YTD").Expression.startswith(
           "TOTALYTD([Amount]")), ("[Amount] > 0 && Sales[Qty] > 0", True))
    check("  those are listed for review", "role 'East' on Sales" in r.details["unqualified_references_left"])

    c, m = connect()
    c.rename_column("Sales", "Qty", "Qty [units]")
    eq("] in the new name is escaped", m.Tables.Find("Sales").Columns.Find("Line Total").Expression,
       "[Qty [units]]] * Sales[Price]")
    c, m = connect()
    eq("collision with a column", c.rename_column("Sales", "Qty", "PRICE").message,
       "Column 'PRICE' already exists in table 'Sales'")
    check("collision with a measure of the same table",
          "measure named 'Rows'" in c.rename_column("Sales", "Qty", "Rows").message)
    check("unknown table / column", not c.rename_column("Nope", "a", "b").success
          and not c.rename_column("Sales", "Nope", "b").success)
    r = c.batch_rename_columns([{"table_name": "Sales", "old_name": "Qty", "new_name": "Units"},
                                {"table_name": "Sales", "old_name": "Qty"}])
    eq("batch message", r.message, "Renamed 1 column(s), 1 failed. Updated 3 model references.")
    eq("  second item", r.details["results"][1]["error"], "Missing required field")


def test_scan_dependencies():
    print("\n== scan_table_dependencies ==")
    c, m = connect()
    r = c.scan_table_dependencies("sales")
    d = r.details
    eq("measures that reference the table, including bare COUNTROWS(Sales)",
       sorted(x["label"] for x in d["measures"]),
       ["Sales[Rows]", "Sales[Total Sales]", "_Measures[Amount YTD]", "_Measures[Amount]"])
    eq("each entry keeps the table / name pair callers print",
       sorted((x["table"], x["name"]) for x in d["measures"]),
       [("Sales", "Rows"), ("Sales", "Total Sales"), ("_Measures", "Amount"), ("_Measures", "Amount YTD")])
    eq("calculated columns / tables / calculation items / RLS",
       ([x["label"] for x in d["calculated_columns"]], [x["label"] for x in d["calculated_tables"]],
        [x["label"] for x in d["calculation_items"]], [x["label"] for x in d["rls_filters"]]),
       (["Sales[Line Total]"], ["Top Sales (calculated table)"], ["Time Calc[Sales YTD]"],
        ["role 'East' on Sales"]))
    eq("relationships", [(x["type"], x["to_table"]) for x in d["relationships"]], [("from_table", "Date")])
    eq("total", (d["total_references"], r.message), (9, "Found 9 references to table 'Sales'"))
    eq("three references in [Rows]: the string literal and the comment are not counted",
       [x["references"] for x in d["measures"] if x["label"] == "Sales[Rows]"], [3])
    check("scanning does not sync or stage anything", m.syncs == 0 and not c.has_pending_changes)
    check("unknown table", not c.scan_table_dependencies("Nope").success)


def test_relationships():
    print("\n== relationships ==")
    c, m = connect()
    r = c.create_relationship("Top Sales", "Amount", "Sales", "Amount", cardinality="one_to_many",
                              cross_filter="both")
    rel = m.Relationships[-1]
    check("created and staged", r.success and m.saves == 0 and len(m.Relationships) == 2, r.message)
    eq("  cardinality and filter direction",
       (rel.FromCardinality, rel.ToCardinality, rel.CrossFilteringBehavior,
                                             rel.IsActive), ("One", "Many", "Both", True))
    check("an unknown cardinality is refused instead of defaulting to many-to-one",
          not c.create_relationship("Top Sales", "Amount", "Sales", "Qty", cardinality="one-to-many").success)
    check("an unknown cross_filter is refused",
          not c.create_relationship("Top Sales", "Amount", "Sales", "Qty", cross_filter="sideways").success)
    check("unknown table / column",
          not c.create_relationship("Nope", "a", "Sales", "Qty").success
          and not c.create_relationship("Sales", "Nope", "Date", "Date").success)
    eq("nothing was added by the refused calls", len(m.Relationships), 2)

    c, m = connect()
    sales, date = m.Tables.Find("Sales"), m.Tables.Find("Date")
    m.Relationships.append(Relationship(sales.Columns.Find("Qty"), date.Columns.Find("Year"), sales, date))
    m._remember()
    r = c.delete_relationship(from_table="Sales", to_table="Date")
    check("two matches are refused rather than deleting the first",
          r.success is False and "2 relationships" in r.message)
    check("  nothing removed", len(m.Relationships) == 2)
    check("a column pins it down",
          c.delete_relationship("Sales", "qty", "Date").success and len(m.Relationships) == 1)
    check("not found / no criteria", not c.delete_relationship("Sales", None, "Top Sales").success
          and not c.delete_relationship().success)


def test_summary():
    print("\n== model summary ==")
    c, m = connect()
    s = c.get_model_summary()
    eq("counts", (s["table_count"], s["total_measures"], s["changes_pending"], s["in_transaction"]),
       (5, 4, False, False))
    c.begin_transaction()
    c.create_measure("Sales", "X", "1")
    s = c.get_model_summary()
    eq("pending work and the open transaction are visible",
       (s["changes_pending"], s["in_transaction"]), (True, True))
    eq("not connected", tomc.PowerBITOMConnector().get_model_summary(), {"error": "Not connected"})


if __name__ == "__main__":
    for fn in (test_transaction_defers_every_save, test_failed_commit_keeps_transaction_open, test_rollback,
               test_connect_rules, test_failed_save_outside_transaction_undoes,
               test_model_is_resynced_before_a_write, test_batch_create_all_or_nothing,
               test_create_update_delete, test_rename_table, test_rename_is_atomic, test_rename_measure,
               test_rename_column, test_scan_dependencies, test_relationships, test_summary):
        fn()
    print()
    if _failures:
        print(f"FAILED: {len(_failures)} check(s): {_failures}")
        sys.exit(1)
    print("ALL TOM CONNECTOR TESTS PASSED")
