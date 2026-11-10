"""
Data access policy engine: column and table rules checked against a DAX query before it
runs and applied to the rows it returns.

WHAT THIS IS, AND IS NOT
------------------------
This is a name-based policy over query text and result column names. It is defence in depth
for an AI client that is already allowed to query the model; it is not a security boundary.
The boundary is in the model: row-level security (RLS) for rows, object-level security (OLS)
for tables and columns. A column that must never be read should be protected by OLS; add a
``block`` rule here as a second line.

The engine sees a query's text, the DAX of the measures it uses (when the caller supplies
it), and the names of the result columns. It does not see the model's schema or data, so:
  * a calculated column or calculated table that copies protected data under another name is
    invisible to it;
  * a filter can single out one row, so an "aggregate" over one row is that row's value;
  * a filter on a masked or hashed column still confirms a guessed value by returning rows;
  * a table function this engine does not know may carry rows where it does not look.

HOW A QUERY IS JUDGED
---------------------
1. References. Every Table[Column], [Name] and table reference is collected with the shared
   DAX tokenizer (strings and comments are ignored), and measure references are expanded into
   their definitions.
2. Each referenced column is resolved to an action. ``block`` refuses the query.
   ``aggregate_only`` refuses it unless every use is a direct SUM/AVERAGE/COUNT-style call.
3. Table rules: a table whose default action is ``block`` cannot be used on its own;
   ``require_filter`` needs a filter function in the query; ``max_rows`` lowers the row cap.
4. Functions that hand rows back as text or under other names (TOCSV, TOJSON, TREATAS, UNION
   with mixed sources, COLUMNSTATISTICS) are refused when protected data can reach them.

HOW RESULTS ARE FILTERED
------------------------
A result column that is a plain model column (``Table[Column]``) gets that column's action.
Every other result column (a measure, an alias, an expression) gets the most restrictive
action among the columns the query referenced: if the query read a masked column, anything it
computed is masked too. That closes aliasing: SELECTCOLUMNS(T, "x", T[card]) is masked exactly
like T[card].

RULE MATCHING
-------------
Column names are compared as words, case-insensitively (see names.py): the rule
``card_number`` matches "Card Number", "CardNumber" and "card-number". ``match: glob`` (the
default) is an fnmatch pattern (* and ?) over the whole normalised name; ``match: word``
matches when the rule's words appear as consecutive whole words anywhere in the name. When
several rules match one column, the most restrictive wins; a column no rule matches gets the
most restrictive default of the table policies that cover its table.

FAILING CLOSED
--------------
A policy file that is missing, unreadable, not valid YAML, or contains one invalid rule
locks the engine: every query is refused with a reason naming the file and the problem.
"""
import fnmatch
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Mapping, Optional, Tuple

import yaml

import dax_tokens as dt

from .config import (
    TOP_LEVEL_KEYS,
    PolicyConfigError,
    check_keys,
    expect_bool,
    expect_choice,
    expect_int,
    expect_mapping,
    expect_str,
    read_policy_file,
)
from .names import MATCH_MODES, parse_column_key, pattern_matches, pattern_problem
from .query_analysis import FILTER_FUNCTIONS, SERIALIZERS, QueryFacts, build_query_facts
from .transforms import (
    REDACTED,
    keyed_hash,
    mask_value,
    new_numeric_coefficient,
    resolve_hash_key,
    scale_number,
)

logger = logging.getLogger(__name__)

__all__ = [
    "AccessPolicyEngine", "ColumnPolicy", "GlobalPolicy", "PolicyAction", "PolicyCheckResult",
    "PolicyConfigError", "PolicyLevel", "TablePolicy", "create_default_policy_engine", "mask_value",
    "parse_column_key",
]

_GLOBAL_KEYS = ("enabled", "default_action", "max_rows_per_query", "enable_pii_detection",
                "pii_default_action", "blocked_patterns", "audit_all_queries")
_TABLE_KEYS = ("name", "description", "sensitivity", "default_action", "max_rows", "require_filter",
               "columns")
_COLUMN_KEYS = ("name", "match", "action", "mask_strategy", "reason", "sensitivity")
_MASK_STRATEGIES = ("partial", "full")
_RELABELERS = frozenset({"UNION", "TREATAS"})


class PolicyAction(Enum):
    """Actions that can be taken on data"""
    ALLOW = "allow"         # Return data as-is
    MASK = "mask"           # Show only the tail of the value
    BLOCK = "block"         # Refuse queries that reference the column; null it in results
    AGGREGATE_ONLY = "aggregate_only"  # Usable only as SUM(col), AVERAGE(col), COUNT(col), ...
    HASH = "hash"           # Keyed hash: equal values stay equal, the value is hidden
    REDACT = "redact"       # Replace with [REDACTED]
    NUMERIC_MASK = "numeric_mask"  # Scale numbers by a per-session factor (keeps ratios)


class PolicyLevel(Enum):
    """Level at which policy applies"""
    TABLE = "table"
    COLUMN = "column"
    GLOBAL = "global"


# Restrictiveness, least to most: how much of the value each action still reveals.
_ACTION_RANK = {
    PolicyAction.ALLOW: 0,
    PolicyAction.NUMERIC_MASK: 1,
    PolicyAction.MASK: 2,
    PolicyAction.HASH: 3,
    PolicyAction.REDACT: 4,
    PolicyAction.AGGREGATE_ONLY: 5,
    PolicyAction.BLOCK: 6,
}
_ACTION_NAMES = tuple(a.value for a in PolicyAction)


def _stricter(a: PolicyAction, b: PolicyAction) -> PolicyAction:
    return a if _ACTION_RANK[a] >= _ACTION_RANK[b] else b


@dataclass
class ColumnPolicy:
    """Policy for a column, or for every column whose name matches ``name``.

    ``match`` is "glob" (fnmatch pattern over the normalised name; a plain name is an exact
    match) or "word" (the words of ``name`` appear as consecutive whole words).
    """
    name: str
    action: PolicyAction = PolicyAction.ALLOW
    mask_strategy: Optional[str] = None  # for MASK: "partial" (default) or "full"
    reason: str = ""
    sensitivity: str = "normal"  # low, normal, high, critical
    match: str = "glob"

    def matches(self, column_name: str) -> bool:
        return pattern_matches(self.name, column_name, self.match)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'match': self.match,
            'action': self.action.value,
            'mask_strategy': self.mask_strategy,
            'reason': self.reason,
            'sensitivity': self.sensitivity
        }


@dataclass
class TablePolicy:
    """Policy for a table. ``name`` may be "*" (every table) or an fnmatch pattern."""
    name: str
    default_action: PolicyAction = PolicyAction.ALLOW
    columns: Dict[str, ColumnPolicy] = field(default_factory=dict)
    max_rows: Optional[int] = None
    require_filter: bool = False
    sensitivity: str = "normal"
    description: str = ""

    def matching_rule(self, column_name: str) -> Optional[ColumnPolicy]:
        """The most restrictive explicit column rule matching ``column_name``, or None."""
        best = None
        for policy in self.columns.values():
            if policy.matches(column_name) and (
                    best is None or _ACTION_RANK[policy.action] > _ACTION_RANK[best.action]):
                best = policy
        return best

    def get_column_policy(self, column_name: str) -> ColumnPolicy:
        """The rule for a column: the most restrictive match, else the table default."""
        return self.matching_rule(column_name) or ColumnPolicy(name=column_name, action=self.default_action)

    def has_protection(self) -> bool:
        return (self.default_action != PolicyAction.ALLOW
                or any(p.action != PolicyAction.ALLOW for p in self.columns.values()))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'default_action': self.default_action.value,
            'columns': {k: v.to_dict() for k, v in self.columns.items()},
            'max_rows': self.max_rows,
            'require_filter': self.require_filter,
            'sensitivity': self.sensitivity,
            'description': self.description
        }


@dataclass
class GlobalPolicy:
    """Global policy settings"""
    enabled: bool = True
    default_action: PolicyAction = PolicyAction.ALLOW
    max_rows_per_query: int = 10000
    enable_pii_detection: bool = True
    pii_default_action: PolicyAction = PolicyAction.MASK
    blocked_patterns: List[str] = field(default_factory=list)  # Regex patterns to block
    audit_all_queries: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            'enabled': self.enabled,
            'default_action': self.default_action.value,
            'max_rows_per_query': self.max_rows_per_query,
            'enable_pii_detection': self.enable_pii_detection,
            'pii_default_action': self.pii_default_action.value,
            'blocked_patterns': self.blocked_patterns,
            'audit_all_queries': self.audit_all_queries
        }


@dataclass
class PolicyCheckResult:
    """Result of a policy check"""
    allowed: bool
    action: PolicyAction
    reason: str = ""
    violations: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    columns_to_mask: List[str] = field(default_factory=list)
    columns_to_block: List[str] = field(default_factory=list)
    max_rows: Optional[int] = None
    # The action applied to every result column that is not a plain model column.
    derived_action: PolicyAction = PolicyAction.ALLOW
    locked: bool = False                                   # refused because the config is broken
    tables: List[str] = field(default_factory=list)        # tables the query (and its measures) use
    columns: List[str] = field(default_factory=list)       # columns and measures referenced
    measures_expanded: List[str] = field(default_factory=list)


@dataclass
class _Decision:
    """A check result together with the facts needed to filter the rows of the same query."""
    result: PolicyCheckResult
    facts: Optional[QueryFacts] = None
    relabel_risk: bool = False   # plain result names cannot be trusted (UNION / TREATAS in play)


@lru_cache(maxsize=1024)
def _table_pattern(pattern: str) -> "re.Pattern[str]":
    return re.compile(fnmatch.translate(pattern.casefold()))


def _table_matches(pattern: str, table: str) -> bool:
    if pattern == "*":
        return True
    if "*" not in pattern and "?" not in pattern:
        return pattern.casefold() == table.casefold()
    return bool(_table_pattern(pattern).match(table.casefold()))


class AccessPolicyEngine:
    """
    Engine for enforcing data access policies. Read the module docstring for what it can and
    cannot guarantee.

    Usage:
        engine = AccessPolicyEngine(config_path="config/policies.yaml")
        check = engine.check_query(query, measure_definitions=measures)
        if check.allowed:
            rows, report = engine.apply_to_results(rows, query=query, measure_definitions=measures)
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Args:
            config_path: YAML policy file. When given and unusable (missing, unparseable, or
                containing an invalid rule) the engine is locked and refuses every query.
                None means "no policies": everything is allowed.
        """
        self.global_policy = GlobalPolicy()
        self.table_policies: Dict[str, TablePolicy] = {}
        self._compiled_blocked_patterns: List[re.Pattern] = []
        self.config_source: Optional[str] = None
        self.config_error: Optional[str] = None
        self.policies_loaded = False
        # One factor per engine (per server session) for NUMERIC_MASK; see transforms.py.
        self.numeric_coefficient = new_numeric_coefficient()
        self._hash_key, self.hash_key_persistent, key_problem = resolve_hash_key()
        if key_problem:
            self.lock(key_problem)

        if config_path:
            self.load_from_file(config_path)

    # ------------------------------------------------------------------ configuration

    @property
    def locked(self) -> bool:
        return self.config_error is not None

    def lock(self, reason: str) -> None:
        """Refuse every query from now on, giving ``reason``. Used for configuration errors."""
        self.config_error = reason
        logger.error("Access policies are LOCKED, all queries will be refused: %s", reason)

    def load_from_file(self, config_path: str) -> bool:
        """Load policies from a YAML file. Returns True when loaded; on any problem the engine
        is locked (see ``config_error``) and False is returned."""
        self.config_source = str(config_path)
        try:
            self._install(read_policy_file(config_path))
        except PolicyConfigError as e:
            self.lock(f"policy file {config_path}: {e}")
            return False
        logger.info("Loaded access policies from: %s", config_path)
        return True

    def load_from_dict(self, config: Dict[str, Any], source: Optional[str] = None) -> None:
        """Load policies from a dictionary shaped like the YAML file.

        Raises PolicyConfigError when the configuration is invalid, and leaves the engine
        locked, so a caller that ignores the exception still cannot run unprotected.
        """
        if source:
            self.config_source = source
        try:
            self._install(config)
        except PolicyConfigError as e:
            self.lock(f"policy configuration {source or '(dict)'}: {e}")
            raise

    def _install(self, config: Any) -> None:
        """Validate the whole configuration, then replace the current policies with it."""
        expect_mapping(config, "top level")
        check_keys(config, TOP_LEVEL_KEYS, "top level")
        global_policy, patterns = self._parse_global(config.get("global"))
        tables = self._parse_tables(config.get("tables"))
        self.global_policy = global_policy
        self._compiled_blocked_patterns = patterns
        self.table_policies = tables
        self.policies_loaded = True

    @staticmethod
    def _action(value: Any, where: str) -> PolicyAction:
        return PolicyAction(expect_choice(value, _ACTION_NAMES, where))

    def _parse_global(self, section: Any) -> Tuple[GlobalPolicy, List[re.Pattern]]:
        policy = GlobalPolicy()
        if section is None:
            return policy, []
        g = expect_mapping(section, "global")
        check_keys(g, _GLOBAL_KEYS, "global")
        if g.get("enabled") is not None:
            policy.enabled = expect_bool(g["enabled"], "global.enabled")
        if g.get("default_action") is not None:
            policy.default_action = self._action(g["default_action"], "global.default_action")
        if g.get("max_rows_per_query") is not None:
            policy.max_rows_per_query = expect_int(g["max_rows_per_query"], "global.max_rows_per_query", 1)
        if g.get("enable_pii_detection") is not None:
            policy.enable_pii_detection = expect_bool(
                g["enable_pii_detection"], "global.enable_pii_detection")
        if g.get("pii_default_action") is not None:
            policy.pii_default_action = self._action(g["pii_default_action"], "global.pii_default_action")
        if g.get("audit_all_queries") is not None:
            policy.audit_all_queries = expect_bool(g["audit_all_queries"], "global.audit_all_queries")
        raw_patterns = g.get("blocked_patterns") or []  # the key with every entry commented out is None
        if not isinstance(raw_patterns, list):
            raise PolicyConfigError("global.blocked_patterns: expected a list of regular expressions")
        compiled = []
        for i, pattern in enumerate(raw_patterns):
            text = expect_str(pattern, f"global.blocked_patterns[{i}]")
            try:
                compiled.append(re.compile(text, re.IGNORECASE))
            except re.error as e:
                raise PolicyConfigError(
                    f"global.blocked_patterns[{i}]: not a valid regular expression ({e})") from e
        policy.blocked_patterns = list(raw_patterns)
        return policy, compiled

    def _parse_tables(self, section: Any) -> Dict[str, TablePolicy]:
        if section is None:  # "tables:" with every entry commented out
            return {}
        if not isinstance(section, list):
            raise PolicyConfigError("tables: expected a list of table policies")
        tables: Dict[str, TablePolicy] = {}
        for i, raw in enumerate(section):
            where = f"tables[{i}]"
            t = expect_mapping(raw, where)
            check_keys(t, _TABLE_KEYS, where)
            name = expect_str(t.get("name"), f"{where}.name").strip()
            where = f"tables[{i}] ('{name}')"
            if any(ord(ch) < 32 for ch in name):
                raise PolicyConfigError(f"{where}: the name contains a control character")
            if name.lower() in tables:
                raise PolicyConfigError(f"{where}: this table is listed twice")
            policy = TablePolicy(name=name.lower())
            if t.get("default_action") is not None:
                policy.default_action = self._action(t["default_action"], f"{where}.default_action")
            if t.get("max_rows") is not None:
                policy.max_rows = expect_int(t["max_rows"], f"{where}.max_rows", 1)
            if t.get("require_filter") is not None:
                policy.require_filter = expect_bool(t["require_filter"], f"{where}.require_filter")
            if t.get("sensitivity") is not None:
                policy.sensitivity = expect_str(t["sensitivity"], f"{where}.sensitivity")
            if t.get("description") is not None:
                policy.description = str(t["description"])
            raw_columns = t.get("columns")
            if raw_columns is not None and not isinstance(raw_columns, list):
                raise PolicyConfigError(f"{where}.columns: expected a list of column rules")
            for j, raw_column in enumerate(raw_columns or []):
                cwhere = f"{where}.columns[{j}]"
                c = expect_mapping(raw_column, cwhere)
                check_keys(c, _COLUMN_KEYS, cwhere)
                column_name = expect_str(c.get("name"), f"{cwhere}.name").strip()
                cwhere = f"{where} column '{column_name}'"
                mode = c.get("match", "glob")
                if mode not in MATCH_MODES:
                    mode = expect_choice(mode, MATCH_MODES, f"{cwhere}.match")
                problem = pattern_problem(column_name, mode)
                if problem:
                    raise PolicyConfigError(f"{cwhere}: {problem}")
                if "action" not in c or c["action"] is None:
                    raise PolicyConfigError(f"{cwhere}: 'action' is required")
                column = ColumnPolicy(name=column_name.lower(), match=mode,
                                      action=self._action(c["action"], f"{cwhere}.action"))
                if c.get("mask_strategy") is not None:
                    column.mask_strategy = expect_choice(c["mask_strategy"], _MASK_STRATEGIES,
                                                         f"{cwhere}.mask_strategy")
                if c.get("reason") is not None:
                    column.reason = str(c["reason"])
                if c.get("sensitivity") is not None:
                    column.sensitivity = expect_str(c["sensitivity"], f"{cwhere}.sensitivity")
                key = column.name if mode == "glob" else f"{column.name} ({mode})"
                if key in policy.columns:
                    raise PolicyConfigError(f"{cwhere}: this rule is listed twice")
                policy.columns[key] = column
            tables[policy.name] = policy
        return tables

    def add_table_policy(self, policy: TablePolicy):
        """Add or update a table policy"""
        self.table_policies[policy.name.lower()] = policy

    def add_column_policy(self, table_name: str, column_policy: ColumnPolicy):
        """Add a column rule to a table (created if needed). Raises PolicyConfigError for a
        name that cannot be used as a pattern."""
        problem = pattern_problem(column_policy.name, column_policy.match)
        if problem:
            raise PolicyConfigError(f"column rule '{column_policy.name}': {problem}")
        table_lower = table_name.lower()
        if table_lower not in self.table_policies:
            self.table_policies[table_lower] = TablePolicy(name=table_name)
        self.table_policies[table_lower].columns[column_policy.name.lower()] = column_policy

    def get_table_policy(self, table_name: str) -> Optional[TablePolicy]:
        """The policy registered under exactly this table name (not a wildcard match)."""
        return self.table_policies.get(table_name.lower().strip('[]\''))

    # ------------------------------------------------------------------ resolution

    def _policies_for(self, table: str) -> List[TablePolicy]:
        """Every table policy that covers ``table``: its own, pattern matches, and "*"."""
        return [policy for key, policy in self.table_policies.items() if _table_matches(key, table)]

    def _explicit_rule(self, policies: List[TablePolicy], column: str) -> Optional[ColumnPolicy]:
        best = None
        for policy in policies:
            rule = policy.matching_rule(column)
            if rule is not None and (best is None or _ACTION_RANK[rule.action] > _ACTION_RANK[best.action]):
                best = rule
        return best

    def resolve_column_policy(self, table: Optional[str], column: str) -> ColumnPolicy:
        """The effective policy for ``table[column]``.

        Explicit column rules of every policy covering the table are consulted and the most
        restrictive match wins. With no match, the column gets the most restrictive default
        among those policies and the global default.

        With ``table`` None (a measure, an alias, an unqualified [Name]) the owning table is
        unknown, so the explicit rules of ALL tables are consulted by name; table defaults are
        not, because they belong to columns of a specific table.
        """
        if table is None:
            return self._resolve_unqualified(column, ())
        policies = self._policies_for(table)
        rule = self._explicit_rule(policies, column)
        if rule is not None:
            return rule
        action = self.global_policy.default_action
        for policy in policies:
            action = _stricter(action, policy.default_action)
        return ColumnPolicy(name=column, action=action)

    def _resolve_unqualified(self, column: str, candidate_tables: Tuple[str, ...]) -> ColumnPolicy:
        """Policy for a [Name] whose table is not written. It may be a column of any table the
        query uses (row context), so each of those is tried as well as every explicit rule."""
        best = self._explicit_rule(list(self.table_policies.values()), column)
        if best is None:
            wildcard = self.table_policies.get("*")
            action = self.global_policy.default_action
            if wildcard is not None:
                action = _stricter(action, wildcard.default_action)
            best = ColumnPolicy(name=column, action=action)
        for table in candidate_tables:
            candidate = self.resolve_column_policy(table, column)
            if _ACTION_RANK[candidate.action] > _ACTION_RANK[best.action]:
                best = candidate
        return best

    def _table_protected(self, table: str) -> bool:
        """True when some rule could apply to a column of ``table``."""
        return (self.global_policy.default_action != PolicyAction.ALLOW
                or any(policy.has_protection() for policy in self._policies_for(table)))

    def _any_protection(self) -> bool:
        return (self.global_policy.default_action != PolicyAction.ALLOW
                or any(policy.has_protection() for policy in self.table_policies.values()))

    # ------------------------------------------------------------------ query check

    @staticmethod
    def extract_references(query: str) -> Tuple[List[str], List[str]]:
        """(tables, column-and-measure names) referenced by a DAX query, each sorted.

        Uses the shared tokenizer, so names inside string literals and comments are ignored and
        the cost is linear in the query length. ``check_query`` does this itself; the method
        remains for callers that want the lists.
        """
        if not query:
            return [], []
        tables, columns = dt.referenced_names(query)
        return sorted(tables), sorted({name for _, name in columns})

    def check_query(
        self,
        query: str,
        tables: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
        measure_definitions: Optional[Mapping[str, str]] = None,
    ) -> PolicyCheckResult:
        """
        Decide whether a query may run.

        Args:
            query: The DAX query text. This is what is analysed.
            tables, columns: Used only when ``query`` is empty (a caller that knows what will
                be read but has no query text). Names in ``columns`` are unqualified.
            measure_definitions: Measure name -> DAX for the connected model. Measures the
                query references are expanded through it, transitively, before the check.
                Without it a measure is judged by its name alone.

        Returns:
            PolicyCheckResult. ``allowed`` False carries the reason; ``max_rows`` is the row
            cap to apply; ``derived_action`` is what happens to computed result columns.
        """
        return self._decide(query, tables, columns, measure_definitions).result

    def _denied(self, violation: Dict[str, Any], locked: bool = False) -> _Decision:
        return _Decision(PolicyCheckResult(
            allowed=False, action=PolicyAction.BLOCK, reason=violation["message"],
            violations=[violation], locked=locked, max_rows=self.global_policy.max_rows_per_query))

    def _decide(self, query: Optional[str], tables: Optional[List[str]] = None,
                columns: Optional[List[str]] = None,
                measure_definitions: Optional[Mapping[str, str]] = None) -> _Decision:
        if self.config_error:
            return self._denied({
                'type': 'config_error',
                'message': f"access policies are locked because the configuration is invalid "
                           f"({self.config_error}); fix it and restart the server"}, locked=True)
        if not self.global_policy.enabled:
            return _Decision(PolicyCheckResult(allowed=True, action=PolicyAction.ALLOW))

        if query:
            facts = build_query_facts(query, measure_definitions)
        else:
            facts = QueryFacts()
            for table in tables or []:
                facts.bare_tables.setdefault(table.casefold(), table)
                facts.tables.setdefault(table.casefold(), table)
            for column in columns or []:
                facts.columns[(None, column.casefold())] = (None, column, False)

        violations: List[Dict[str, Any]] = []
        warnings: List[str] = []
        columns_to_mask: List[str] = []
        columns_to_block: List[str] = []
        derived = PolicyAction.ALLOW
        max_rows = self.global_policy.max_rows_per_query

        for pattern in self._compiled_blocked_patterns:
            if query and pattern.search(query):
                violations.append({
                    'type': 'blocked_pattern',
                    'pattern': pattern.pattern,
                    'message': f"Query matches blocked pattern: {pattern.pattern}"
                })

        # ---- table rules
        referenced_tables = tuple(facts.tables.values())
        filtered = bool(facts.query_functions & FILTER_FUNCTIONS)
        for folded, table in facts.tables.items():
            for policy in self._policies_for(table):
                if policy.max_rows:
                    max_rows = min(max_rows, policy.max_rows)
                if policy.require_filter and query and not filtered:
                    violations.append({
                        'type': 'filter_required',
                        'table': table,
                        'message': f"Table '{table}' may only be queried with a filter: use "
                                   f"{', '.join(sorted(FILTER_FUNCTIONS))} to narrow the rows"
                    })
                if policy.default_action == PolicyAction.BLOCK and folded in facts.bare_tables:
                    violations.append({
                        'type': 'table_blocked',
                        'table': table,
                        'message': f"Access to table '{table}' is blocked"
                    })

        # ---- column rules
        expanded_measures = {name.casefold() for name in facts.measures_expanded}
        for (folded_table, _), (table, name, aggregated) in facts.columns.items():
            label = f"{table}[{name}]" if table is not None else f"[{name}]"
            if table is None or folded_table in facts.defined_tables:
                # Unqualified, or qualified by a table the query defined itself: the real
                # owner is one of the tables the query uses.
                policy = self._resolve_unqualified(name, referenced_tables)
            else:
                policy = self.resolve_column_policy(table, name)
            action = policy.action
            if action == PolicyAction.ALLOW:
                continue
            if action == PolicyAction.BLOCK:
                columns_to_block.append(label)
                message = policy.reason or f"Column '{name}' access blocked by policy"
                violations.append({'type': 'column_blocked', 'column': name, 'reason': message,
                                   'message': message})
            elif action == PolicyAction.AGGREGATE_ONLY:
                # A known measure is not the column: its own definition is checked instead.
                if not aggregated and not (table is None and name.casefold() in expanded_measures):
                    columns_to_block.append(label)
                    violations.append({
                        'type': 'aggregate_only',
                        'column': name,
                        'message': f"Column '{name}' may only be used directly inside an aggregation "
                                   f"such as SUM, AVERAGE, COUNT or DISTINCTCOUNT"
                    })
            else:
                columns_to_mask.append(label)
                warnings.append(f"Column '{name}' is protected ({action.value}): it and every value "
                                f"computed in this query are returned transformed")
                derived = _stricter(derived, action)

        # ---- functions that return rows as text or under other names
        for sink in facts.sinks:
            protected = sorted(t for t in sink.tables if self._table_protected(t))
            untraced = sink.untraced and any(self._table_protected(t) for t in facts.bare_tables.values())
            if not protected and not untraced:
                continue
            what = (f"table {', '.join(repr(t) for t in protected)}" if protected
                    else "a table with protected columns")
            if sink.function in SERIALIZERS:
                message = (f"{sink.function} over {what} is not allowed: serialised text cannot be "
                           f"checked column by column. Return the table expression itself instead")
            else:
                message = (f"{sink.function} over {what} is not allowed: it can return protected "
                           f"columns under other names. Select the columns explicitly with SELECTCOLUMNS")
            violations.append({'type': 'row_relabel', 'function': sink.function,
                               'tables': protected, 'message': message})
        if "COLUMNSTATISTICS" in facts.functions and self._any_protection():
            violations.append({
                'type': 'column_statistics',
                'message': "COLUMNSTATISTICS is not allowed while column policies are active: "
                           "it returns the minimum and maximum of every column"})

        allowed = not violations
        result = PolicyCheckResult(
            allowed=allowed,
            action=PolicyAction.ALLOW if allowed else PolicyAction.BLOCK,
            reason=violations[0]['message'] if violations else "",
            violations=violations,
            warnings=warnings,
            columns_to_mask=sorted(set(columns_to_mask)),
            columns_to_block=sorted(set(columns_to_block)),
            max_rows=max_rows,
            derived_action=derived,
            tables=sorted(referenced_tables, key=str.casefold),
            columns=sorted(f"{t}[{n}]" if t is not None else f"[{n}]" for t, n, _ in facts.columns.values()),
            measures_expanded=sorted(facts.measures_expanded),
        )
        relabel = derived != PolicyAction.ALLOW and bool(facts.functions & _RELABELERS)
        return _Decision(result, facts, relabel)

    # ------------------------------------------------------------------ results

    def apply_to_results(
        self,
        results: List[Dict[str, Any]],
        table_name: Optional[str] = None,
        query: Optional[str] = None,
        measure_definitions: Optional[Mapping[str, str]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Apply policies to the rows a query returned.

        Args:
            results: Row dictionaries keyed by result column name
            table_name: Table to assume for result keys that carry none
            query: The query that produced the rows. Pass it: without it only the result
                column names can be checked, and an aliased column is not recognised.
            measure_definitions: As for ``check_query``

        Returns:
            (processed_results, report). If the query itself is not allowed, every value is
            nulled and the report carries ``denied`` and ``reason``.
        """
        decision = self._decide(query, measure_definitions=measure_definitions) if (
            query or self.config_error) else None
        return self._apply(results, decision, table_name)

    def _apply(self, results: List[Dict[str, Any]], decision: Optional[_Decision],
               table_name: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        if decision is None and self.config_error:
            decision = self._decide(None)
        if decision is not None and not decision.result.allowed:
            blocked = sorted({key for row in results for key in row}, key=str)
            return ([{key: None for key in row} for row in results],
                    {'applied': True, 'denied': True, 'reason': decision.result.reason,
                     'locked': decision.result.locked, 'rows_processed': len(results),
                     'blocked_columns': blocked, 'masked_columns': [], 'transformed_columns': blocked,
                     'derived_action': PolicyAction.BLOCK.value})
        if not self.global_policy.enabled or not results:
            return results, {'applied': False}

        facts = decision.facts if decision else None
        derived = decision.result.derived_action if decision else PolicyAction.ALLOW
        relabel = decision.relabel_risk if decision else False
        referenced_tables = tuple(facts.tables.values()) if facts else ()

        processed = []
        blocked_columns = set()
        masked_columns = set()
        # Rows share a schema, so each distinct key is resolved once.
        policy_cache: Dict[Any, ColumnPolicy] = {}

        for row in results:
            processed_row = {}
            for col_name, value in row.items():
                policy = policy_cache.get(col_name)
                if policy is None:
                    policy = self._result_key_policy(col_name, table_name, facts, derived, relabel,
                                                     referenced_tables)
                    policy_cache[col_name] = policy
                action = policy.action
                if action == PolicyAction.ALLOW:
                    processed_row[col_name] = value
                    continue
                if action in (PolicyAction.BLOCK, PolicyAction.AGGREGATE_ONLY):
                    processed_row[col_name] = None
                    blocked_columns.add(col_name)
                    continue
                if action == PolicyAction.REDACT:
                    processed_row[col_name] = REDACTED
                elif action == PolicyAction.HASH:
                    processed_row[col_name] = None if value is None else keyed_hash(value, self._hash_key)
                elif action == PolicyAction.MASK:
                    processed_row[col_name] = mask_value(value, policy.mask_strategy)
                else:  # NUMERIC_MASK
                    processed_row[col_name] = scale_number(value, self.numeric_coefficient)
                masked_columns.add(col_name)
            processed.append(processed_row)

        blocked = sorted(blocked_columns, key=str)
        masked = sorted(masked_columns, key=str)
        return processed, {
            'applied': True,
            'denied': False,
            'rows_processed': len(results),
            'blocked_columns': blocked,
            'masked_columns': masked,
            'transformed_columns': blocked + masked,
            'derived_action': derived.value,
        }

    def _result_key_policy(self, key: Any, table_name: Optional[str], facts: Optional[QueryFacts],
                           derived: PolicyAction, relabel: bool,
                           referenced_tables: Tuple[str, ...]) -> ColumnPolicy:
        """Policy for one result column.

        A ``Table[Column]`` key naming a model column gets that column's policy. Anything else
        is computed by the query: it gets the stricter of the policy its own name resolves to
        and the action derived from what the query referenced.
        """
        table, column = parse_column_key(key)
        defined_table = bool(facts and table is not None and table.casefold() in facts.defined_tables)
        defined_column = bool(facts and table is not None
                              and (table.casefold(), column.casefold()) in facts.defined_columns)
        if table is not None and not defined_table and not defined_column:
            policy = self.resolve_column_policy(table, column)
            if relabel and _ACTION_RANK[derived] > _ACTION_RANK[policy.action]:
                return ColumnPolicy(name=column, action=derived)
            return policy
        if table is None and table_name:
            policy = self.resolve_column_policy(table_name, column)
        elif defined_table:
            # Rows of a query-defined table come from the tables the query used.
            policy = self._resolve_unqualified(column, referenced_tables)
        elif defined_column:
            policy = self.resolve_column_policy(table, column)
        else:
            policy = self._resolve_unqualified(column, ())
        if policy.action == PolicyAction.AGGREGATE_ONLY:
            # A computed column that merely shares the name is not the raw column, and the
            # query check has already insisted that the column is only used aggregated.
            policy = ColumnPolicy(name=column, action=PolicyAction.ALLOW)
        if _ACTION_RANK[derived] > _ACTION_RANK[policy.action]:
            return ColumnPolicy(name=column, action=derived)
        return policy

    # ------------------------------------------------------------------ introspection

    def get_column_action(self, table_name: str, column_name: str) -> PolicyAction:
        """Get the action for a specific column"""
        return self.resolve_column_policy(table_name, column_name).action

    def get_sensitive_columns(self, table_name: str) -> List[str]:
        """Names (patterns) of the explicit non-allow column rules covering a table"""
        sensitive = []
        for policy in self._policies_for(table_name):
            for col_policy in policy.columns.values():
                if col_policy.action != PolicyAction.ALLOW and col_policy.name not in sensitive:
                    sensitive.append(col_policy.name)
        return sensitive

    def export_config(self) -> Dict[str, Any]:
        """Export the current configuration in the shape of the YAML file"""
        tables = []
        for policy in self.table_policies.values():
            entry = policy.to_dict()
            entry['columns'] = [
                {k: v for k, v in column.to_dict().items() if v not in (None, "")}
                for column in policy.columns.values()
            ]
            tables.append({k: v for k, v in entry.items() if v not in (None, "")})
        return {'global': self.global_policy.to_dict(), 'tables': tables}

    def export_to_file(self, path: str):
        """Export configuration to a YAML file"""
        config = self.export_config()
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        logger.info(f"Exported policy config to: {path}")


# Convenience function
def create_default_policy_engine() -> AccessPolicyEngine:
    """A policy engine with the same baseline rules as the shipped config/policies.yaml,
    for use without a file: credentials and national IDs blocked, card numbers masked."""
    engine = AccessPolicyEngine()
    rules = [
        ('ssn', 'word', PolicyAction.BLOCK, 'Social Security Number'),
        ('social security', 'word', PolicyAction.BLOCK, 'Social Security Number'),
        ('*password*', 'glob', PolicyAction.BLOCK, 'Password field'),
        ('secret', 'word', PolicyAction.BLOCK, 'Secret field'),
        ('api key', 'word', PolicyAction.BLOCK, 'API key'),
        ('access token', 'word', PolicyAction.BLOCK, 'Token field'),
        ('refresh token', 'word', PolicyAction.BLOCK, 'Token field'),
        ('token', 'glob', PolicyAction.BLOCK, 'Token field'),
        ('credit card', 'word', PolicyAction.MASK, 'Credit card number'),
        ('card number', 'word', PolicyAction.MASK, 'Credit card number'),
    ]
    wildcard_table = TablePolicy(name='*')
    for name, mode, action, reason in rules:
        wildcard_table.columns[f"{name} ({mode})"] = ColumnPolicy(
            name=name, match=mode, action=action, reason=reason, sensitivity='critical')
    engine.table_policies['*'] = wildcard_table
    engine.policies_loaded = True
    return engine
