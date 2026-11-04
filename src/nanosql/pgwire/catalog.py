"""Emulación del catálogo del sistema de PostgreSQL sobre el catálogo de nanosql.

psql, psycopg y SQLAlchemy no sólo ejecutan el SQL del usuario: también
interrogan `pg_catalog` (`\\dt`, `\\d tabla`, `has_table`, la detección de
hstore...) y lanzan consultas sin FROM (`SELECT version()`, `SELECT 1`).
Esas consultas usan construcciones que el subconjunto SQL de nanosql no
tiene (CASE, `OPERATOR(pg_catalog.~)`, `= ANY(ARRAY[...])`, subconsultas
correlacionadas) contra relaciones que no existen.

En lugar de reimplementar un motor SQL paralelo, este módulo reconoce la
*forma* de esas consultas (qué relaciones del sistema tocan y qué filtros
aplican) y construye la respuesta a partir del catálogo real de nanosql:

* las filas salen de un modelo de relaciones (tablas, índices, columnas)
  derivado de `Catalog`;
* los filtros que importan se extraen del texto (`relname = 'x'`,
  `relname ~ '^(x)$'`, `c.oid = '16384'`, `relkind IN (...)`);
* la proyección se resuelve item a item de la lista del SELECT: alias
  conocido, literal, columna (`c.relname` → `relname`) o función
  (`pg_get_indexdef(...)` → `pg_get_indexdef`); lo desconocido es NULL.

Las consultas al catálogo que no se reconocen devuelven un resultado vacío
con las columnas de su lista de SELECT — así psql muestra "sin políticas",
"sin triggers", etc. en vez de fallar. Todo esto está documentado en el
README como emulación por patrones, no como SQL general sobre pg_catalog.
"""

from __future__ import annotations

import platform
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from .. import __version__
from ..catalog import Catalog, ColumnSchema, ColumnValue, TableSchema
from .sqltext import Tok, meaningful, tokenize
from .types import OID_BOOL, OID_INT4, OID_INT8, OID_OID, OID_TEXT

PG_COMPAT_VERSION = "16.0"
PG_COMPAT_VERSION_NUM = "160000"

TABLE_OID_BASE = 16384
INDEX_OID_BASE = 32768
NAMESPACE_PUBLIC_OID = 2200

_PG_TYPE_NAMES = {
    "INTEGER": "bigint",
    "FLOAT": "double precision",
    "STRING": "text",
    "BOOLEAN": "boolean",
}
_PG_TYPE_OIDS = {"INTEGER": OID_INT8, "FLOAT": 701, "STRING": OID_TEXT, "BOOLEAN": OID_BOOL}

# Relaciones del sistema cuya respuesta honesta en nanosql es "ninguna fila":
# no hay herencia, políticas RLS, publicaciones, triggers, claves foráneas,
# estadísticas extendidas, funciones de usuario ni extensiones.
_EMPTY_RELATIONS = frozenset(
    {
        "pg_inherits",
        "pg_policy",
        "pg_statistic_ext",
        "pg_publication",
        "pg_publication_rel",
        "pg_publication_namespace",
        "pg_constraint",
        "pg_trigger",
        "pg_rewrite",
        "pg_foreign_table",
        "pg_partitioned_table",
        "pg_extension",
        "pg_depend",
        "pg_description",
        "pg_event_trigger",
        "pg_sequence",
        "pg_proc",
        "pg_enum",
        "pg_range",
        "pg_aggregate",
        "pg_operator",
        "pg_cast",
        "pg_conversion",
        "pg_language",
        "pg_opclass",
        "pg_collation",
        "pg_type",
        "pg_views",
        "pg_matviews",
        "pg_sequences",
        "pg_shdescription",
        "pg_stat_activity",
        "pg_locks",
    }
)


@dataclass(frozen=True, slots=True)
class CatalogResult:
    columns: tuple[tuple[str, int], ...]  # (nombre, OID de tipo)
    rows: list[tuple[ColumnValue, ...]]
    tag: str


@dataclass(slots=True)
class SessionInfo:
    user: str
    database: str
    pid: int
    settings: dict[str, str] = field(default_factory=dict)
    transaction_isolation: Callable[[], str] = lambda: "read committed"


def server_version_string() -> str:
    return (
        f"PostgreSQL {PG_COMPAT_VERSION} (nanosql {__version__}, wire-compatible server) "
        f"on {platform.machine()}-{platform.system().lower()}, "
        f"Python {platform.python_version()}, 64-bit"
    )


# ---------------------------------------------------------------------------
# Modelo de relaciones derivado del catálogo de nanosql
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IndexInfo:
    oid: int
    name: str
    table: TableSchema
    columns: tuple[str, ...]
    unique: bool
    primary: bool
    constraint: str | None  # 'p', 'u' o None (índice sin restricción)


def _indexes_of(catalog: Catalog, table: TableSchema, oids: dict[str, int]) -> list[IndexInfo]:
    """Índices visibles de una tabla, con los nombres implícitos que pondría
    PostgreSQL a las restricciones de columna (`t_pkey`, `t_col_key`)."""
    found: list[IndexInfo] = []
    for column in table.columns:
        if column.primary_key:
            name = f"{table.name}_pkey"
            found.append(IndexInfo(oids.get(name, 0), name, table, (column.name,), True, True, "p"))
        elif column.unique:
            name = f"{table.name}_{column.name}_key"
            found.append(
                IndexInfo(oids.get(name, 0), name, table, (column.name,), True, False, "u")
            )
    for index in catalog.indexes_of(table.name):
        primary = index.name == f"{table.name}_pkey" and index.unique
        found.append(
            IndexInfo(
                oids.get(index.name, 0),
                index.name,
                table,
                index.columns,
                index.unique,
                primary,
                "p" if primary else None,
            )
        )
    return found


class RelationModel:
    """Tablas e índices de nanosql con OIDs estables (orden del catálogo)."""

    def __init__(self, catalog: Catalog, owner: str) -> None:
        self.catalog = catalog
        self.owner = owner
        self.table_oids: dict[str, int] = {}
        self.tables_by_oid: dict[int, TableSchema] = {}
        for i, table in enumerate(catalog.tables.values()):
            oid = TABLE_OID_BASE + i
            self.table_oids[table.name] = oid
            self.tables_by_oid[oid] = table
        index_oids: dict[str, int] = {}
        self.indexes: list[IndexInfo] = []
        counter = INDEX_OID_BASE
        for table in catalog.tables.values():
            names = [info.name for info in _indexes_of(catalog, table, {})]
            for name in names:
                index_oids[name] = counter
                counter += 1
        for table in catalog.tables.values():
            self.indexes.extend(_indexes_of(catalog, table, index_oids))

    def indexes_of(self, table: TableSchema) -> list[IndexInfo]:
        return [index for index in self.indexes if index.table.name == table.name]

    def relation_rows(self) -> list[dict[str, ColumnValue]]:
        rows: list[dict[str, ColumnValue]] = []
        for table in self.catalog.tables.values():
            has_index = bool(self.indexes_of(table))
            rows.append(
                self._relation_row(
                    self.table_oids[table.name], table.name, "r", "table", has_index, ""
                )
            )
        for index in self.indexes:
            rows.append(
                self._relation_row(index.oid, index.name, "i", "index", False, index.table.name)
            )
        return rows

    def _relation_row(
        self, oid: int, name: str, kind: str, label: str, has_index: bool, table: str
    ) -> dict[str, ColumnValue]:
        return {
            "oid": oid,
            "relname": name,
            "nspname": "public",
            "relnamespace": NAMESPACE_PUBLIC_OID,
            "relkind": kind,
            "schema": "public",
            "name": name,
            "type": label,
            "owner": self.owner,
            "pg_get_userbyid": self.owner,
            "table": table or None,
            "relhasindex": has_index,
            "relchecks": 0,
            "relhasrules": False,
            "relhastriggers": False,
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "relhasoids": False,
            "relispartition": False,
            "reltablespace": 0,
            "relpersistence": "p",
            "relreplident": "d",
            "amname": "heap" if kind == "r" else "btree",
            "persistence": "permanent",
            "access method": "heap" if kind == "r" else "btree",
        }


# ---------------------------------------------------------------------------
# Proyección de la lista del SELECT
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SelectItem:
    tokens: tuple[Tok, ...]
    alias: str | None

    @property
    def output_name(self) -> str:
        if self.alias is not None:
            return self.alias
        column = self.column_ref()
        if column is not None:
            return column
        function = self.outer_function()
        if function is not None:
            return function
        if self.tokens and self.tokens[0].upper == "CASE":
            return "case"
        return "?column?"

    def literal(self) -> tuple[bool, ColumnValue]:
        toks = [t for t in self.tokens]
        # literal opcionalmente con cast: 'x'::text, false, 0, NULL::text
        if toks and (len(toks) == 1 or (len(toks) >= 3 and toks[1].text == "::")):
            head = toks[0]
            if head.kind == "string":
                return True, head.value
            if head.kind == "number":
                return True, float(head.text) if "." in head.text else int(head.text)
            if head.upper in ("TRUE", "FALSE"):
                return True, head.upper == "TRUE"
            if head.upper == "NULL":
                return True, None
        return False, None

    def column_ref(self) -> str | None:
        toks = self.tokens
        idents = [t for t in toks if t.text != "."]
        if toks and all(t.kind in ("ident", "qident") or t.text == "." for t in toks):
            if idents and len(toks) == 2 * len(idents) - 1:
                last = idents[-1]
                return last.value if last.kind == "qident" else last.lower
        return None

    def outer_function(self) -> str | None:
        toks = self.tokens
        for i, tok in enumerate(toks):
            if tok.kind == "ident" and i + 1 < len(toks) and toks[i + 1].text == "(":
                return tok.lower
            if tok.kind not in ("ident",) and tok.text != ".":
                break
        return None

    def first_inner_function(self) -> str | None:
        toks = self.tokens
        for i, tok in enumerate(toks):
            if (
                tok.kind == "ident"
                and tok.upper not in ("SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN")
                and i + 1 < len(toks)
                and toks[i + 1].text == "("
            ):
                return tok.lower
        return None

    def resolve(self, row: dict[str, ColumnValue]) -> ColumnValue:
        if self.alias is not None and self.alias.lower() in row:
            return row[self.alias.lower()]
        is_literal, value = self.literal()
        if is_literal:
            return value
        column = self.column_ref()
        if column is not None:
            return row.get(column)
        function = self.outer_function()
        if function is not None and function in row:
            return row[function]
        if self.tokens and self.tokens[0].text == "(":
            inner = self.first_inner_function()
            if inner is not None:
                return row.get(inner)
        return None


def _split_top_level(tokens: Sequence[Tok], separator: str) -> list[list[Tok]]:
    parts: list[list[Tok]] = [[]]
    depth = 0
    for tok in tokens:
        if tok.text in ("(", "["):
            depth += 1
        elif tok.text in (")", "]"):
            depth -= 1
        if depth == 0 and tok.text == separator:
            parts.append([])
        else:
            parts[-1].append(tok)
    return [p for p in parts if p]


def select_items(tokens: Sequence[Tok]) -> tuple[list[SelectItem], bool]:
    """Items de la primera lista SELECT de nivel superior y si hay FROM."""
    start = next((i for i, t in enumerate(tokens) if t.upper == "SELECT"), None)
    if start is None:
        return [], False
    depth = 0
    end = len(tokens)
    has_from = False
    for i in range(start + 1, len(tokens)):
        tok = tokens[i]
        if tok.text == "(":
            depth += 1
        elif tok.text == ")":
            depth -= 1
        elif depth == 0 and tok.upper in ("FROM", "WHERE", "ORDER", "LIMIT", "UNION", "GROUP"):
            end = i
            has_from = tok.upper == "FROM"
            break
    body = list(tokens[start + 1 : end])
    if body and body[0].upper == "DISTINCT":
        body = body[1:]
    items: list[SelectItem] = []
    for part in _split_top_level(body, ","):
        alias: str | None = None
        if len(part) >= 3 and part[-2].upper == "AS" and part[-1].kind in ("ident", "qident"):
            alias = part[-1].value if part[-1].kind == "qident" else part[-1].text.lower()
            part = part[:-2]
        elif (
            len(part) >= 2
            and part[-1].kind in ("ident", "qident")
            and part[-2].kind in ("ident", "qident", "string", "number")
            and part[-2].text != "."
            and part[-1].upper not in ("END",)
            and not (part[-1].kind == "ident" and len(part) == 2 and part[0].text == ".")
        ):
            # alias sin AS: `expr nombre`
            alias = part[-1].value if part[-1].kind == "qident" else part[-1].text.lower()
            part = part[:-1]
        items.append(SelectItem(tuple(part), alias))
    return items, has_from


def _infer_oid(name: str, values: list[ColumnValue]) -> int:
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return OID_BOOL
        if isinstance(value, int):
            return OID_OID if name.endswith("oid") or name == "oid" else OID_INT4
        return OID_TEXT
    return OID_TEXT


def project(
    items: list[SelectItem], rows: list[dict[str, ColumnValue]], tag_prefix: str = "SELECT"
) -> CatalogResult:
    names = [item.output_name for item in items]
    out_rows = [tuple(item.resolve(row) for item in items) for row in rows]
    columns = tuple(
        (name, _infer_oid(name.lower(), [r[i] for r in out_rows])) for i, name in enumerate(names)
    )
    return CatalogResult(columns=columns, rows=out_rows, tag=f"{tag_prefix} {len(out_rows)}")


# ---------------------------------------------------------------------------
# Filtros extraídos del texto
# ---------------------------------------------------------------------------

_LITERAL = r"'((?:[^']|'')*)'(?:\s*::\s*[\w.]+(?:\s+\w+)?)?"


def _unquote(raw: str) -> str:
    return raw.replace("''", "'")


def _relkind_filter(text: str) -> set[str] | None:
    match = re.search(r"relkind\s+IN\s*\(([^)]*)\)", text, re.I)
    if match is None:
        match = re.search(r"relkind\s*=\s*ANY\s*\(\s*ARRAY\s*\[([^\]]*)\]", text, re.I)
    if match is not None:
        return {_unquote(m) for m in re.findall(r"'((?:[^']|'')*)'", match.group(1))}
    single = re.search(r"relkind\s*=\s*" + _LITERAL, text, re.I)
    if single is not None:
        return {_unquote(single.group(1))}
    return None


def _name_filters(text: str, column: str) -> list[Callable[[str], bool]]:
    filters: list[Callable[[str], bool]] = []
    for match in re.finditer(rf"\b{column}\s*=\s*" + _LITERAL, text, re.I):
        filters.append(_equals(_unquote(match.group(1))))
    in_list = re.finditer(
        rf"\b{column}\s+IN\s*\(([^)]*)\)|\b{column}\s*=\s*ANY\s*\(\s*ARRAY\s*\[([^\]]*)\]",
        text,
        re.I,
    )
    for match in in_list:
        body = match.group(1) if match.group(1) is not None else match.group(2)
        allowed = {_unquote(m) for m in re.findall(r"'((?:[^']|'')*)'", body)}
        filters.append(_member_of(frozenset(allowed)))
    pattern = rf"\b{column}\s+(?:OPERATOR\s*\(\s*pg_catalog\.~\s*\)|~)\s*" + _LITERAL
    for match in re.finditer(pattern, text, re.I):
        filters.append(_regex_search(_unquote(match.group(1))))
    return filters


def _equals(expected: str) -> Callable[[str], bool]:
    def check(value: str) -> bool:
        return value == expected

    return check


def _member_of(allowed: frozenset[str]) -> Callable[[str], bool]:
    def check(value: str) -> bool:
        return value in allowed

    return check


def _regex_search(pattern: str) -> Callable[[str], bool]:
    try:
        compiled = re.compile(pattern)
    except re.error:
        return lambda value: False

    def check(value: str) -> bool:
        return compiled.search(value) is not None

    return check


def _oid_filter(text: str, column: str) -> int | None:
    match = re.search(rf"\b{column}\s*=\s*'?(\d+)'?", text, re.I)
    return int(match.group(1)) if match else None


def _schema_excluded(text: str) -> bool:
    """`nspname = 'algo'` con algo distinto de public → ninguna fila."""
    for match in re.finditer(r"\bnspname\s*=\s*" + _LITERAL, text, re.I):
        if _unquote(match.group(1)) != "public":
            return True
    for match in re.finditer(
        r"\bnspname\s+(?:OPERATOR\s*\(\s*pg_catalog\.~\s*\)|~)\s*" + _LITERAL, text, re.I
    ):
        try:
            if re.search(_unquote(match.group(1)), "public") is None:
                return True
        except re.error:
            return True
    return False


# ---------------------------------------------------------------------------
# Emulador
# ---------------------------------------------------------------------------

_SYSTEM_IDENT = re.compile(r"^(pg_[a-z_]+|information_schema)$")


def references_system_catalog(tokens: Sequence[Tok]) -> bool:
    return any(
        t.kind == "ident" and _SYSTEM_IDENT.match(t.lower) is not None
        for t in tokens
        if t.lower not in ("pg_sleep",)
    )


def relations_mentioned(tokens: Sequence[Tok]) -> set[str]:
    names: set[str] = set()
    for i, tok in enumerate(tokens):
        if tok.kind != "ident":
            continue
        name = tok.lower
        if name.startswith("pg_") and not (i + 1 < len(tokens) and tokens[i + 1].text == "("):
            names.add(name)
        if name == "information_schema" and i + 2 < len(tokens) and tokens[i + 1].text == ".":
            names.add(f"information_schema.{tokens[i + 2].lower}")
    return names


def _default_sql(column: ColumnSchema) -> str | None:
    if not column.has_default:
        return None
    value = column.default
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'::text"
    return repr(value)


class CatalogEmulator:
    def __init__(self, catalog: Catalog, session: SessionInfo) -> None:
        self._catalog = catalog
        self._session = session

    # -- consultas sin FROM -------------------------------------------------

    def _function_value(self, item: SelectItem) -> tuple[bool, ColumnValue]:
        toks = item.tokens
        name = item.outer_function()
        if name is None:
            keyword = toks[0].upper if len(toks) == 1 else ""
            if keyword in ("CURRENT_USER", "SESSION_USER", "USER", "CURRENT_ROLE"):
                return True, self._session.user
            if keyword in ("CURRENT_CATALOG",):
                return True, self._session.database
            if keyword == "CURRENT_SCHEMA":
                return True, "public"
            return False, None
        args = [t for t in toks if t.kind == "string"]
        session = self._session
        simple: dict[str, ColumnValue] = {
            "version": server_version_string(),
            "current_schema": "public",
            "current_database": session.database,
            "current_user": session.user,
            "session_user": session.user,
            "pg_backend_pid": session.pid,
            "pg_is_in_recovery": False,
            "inet_server_port": None,
            "pg_postmaster_start_time": None,
        }
        if name in simple:
            return True, simple[name]
        if name == "current_setting" and args:
            return True, self.show_value(args[0].value)
        if name == "current_schemas":
            return True, "{public}"
        if name == "set_config" and len(args) >= 2:
            session.settings[args[0].value.lower()] = args[1].value
            return True, args[1].value
        if name == "to_regclass" and args:
            table = args[0].value.split(".")[-1].strip('"')
            return True, table if table in self._catalog.tables else None
        return False, None

    def constant_select(self, tokens: Sequence[Tok]) -> CatalogResult | None:
        items, has_from = select_items(tokens)
        if has_from or not items:
            return None
        row: list[ColumnValue] = []
        for item in items:
            is_literal, value = item.literal()
            if not is_literal:
                is_literal, value = self._function_value(item)
            if not is_literal:
                return None
            row.append(value)
        names = [item.output_name for item in items]
        columns = tuple(
            (name, _constant_oid(value)) for name, value in zip(names, row, strict=True)
        )
        return CatalogResult(columns=columns, rows=[tuple(row)], tag="SELECT 1")

    # -- SHOW -----------------------------------------------------------------

    def show_value(self, name: str) -> str:
        key = name.strip().lower()
        if key in ("transaction_isolation", "transaction isolation level"):
            return self._session.transaction_isolation()
        if key in self._session.settings:
            return self._session.settings[key]
        defaults = default_settings(self._session)
        if key in defaults:
            return defaults[key]
        from ..errors import ProgrammingError

        raise ProgrammingError(
            f'parámetro de configuración no reconocido "{name}"', sqlstate="42704"
        )

    def show_all(self) -> CatalogResult:
        merged = default_settings(self._session)
        merged.update(self._session.settings)
        merged["transaction_isolation"] = self._session.transaction_isolation()
        rows: list[tuple[ColumnValue, ...]] = [
            (name, value, "") for name, value in sorted(merged.items())
        ]
        return CatalogResult(
            columns=(("name", OID_TEXT), ("setting", OID_TEXT), ("description", OID_TEXT)),
            rows=rows,
            tag=f"SHOW {len(rows)}",
        )

    # -- pg_catalog / information_schema -------------------------------------

    def handle(self, sql: str) -> CatalogResult | None:
        tokens = meaningful(tokenize(sql))
        if not tokens or tokens[0].upper not in ("SELECT", "WITH"):
            return None
        constant = self.constant_select(tokens)
        if constant is not None:
            return constant
        if not references_system_catalog(tokens):
            return None
        items, _ = select_items(tokens)
        relations = relations_mentioned(tokens)
        model = RelationModel(self._catalog, self._session.user)
        text = "".join(
            " " if t.kind == "ws" else t.text for t in tokenize(sql) if t.kind != "comment"
        )
        names = {item.output_name.lower() for item in items}

        if "pg_index" in relations and ("indisprimary" in names or "pg_get_indexdef" in text):
            return project(items, self._index_rows(model, text))
        if "pg_attribute" in relations and "attname" in names:
            return project(items, self._attribute_rows(model, text))
        if relations & _EMPTY_RELATIONS:
            return project(items, [])
        if "information_schema.tables" in relations:
            return project(items, self._information_schema_tables(model, text))
        if "information_schema.columns" in relations:
            return project(items, self._information_schema_columns(model, text))
        if "information_schema.schemata" in relations:
            return project(items, [{"schema_name": "public", "catalog_name": "nanosql"}])
        if "pg_class" in relations:
            return project(items, self._relation_rows(model, text))
        if "pg_namespace" in relations:
            row: dict[str, ColumnValue] = {
                "oid": NAMESPACE_PUBLIC_OID,
                "nspname": "public",
                "name": "public",
                "owner": self._session.user,
                "pg_get_userbyid": self._session.user,
            }
            return project(items, [] if _schema_excluded(text) else [row])
        if "pg_database" in relations:
            db: dict[str, ColumnValue] = {
                "datname": self._session.database,
                "name": self._session.database,
                "owner": self._session.user,
                "pg_get_userbyid": self._session.user,
                "encoding": "UTF8",
                "pg_encoding_to_char": "UTF8",
                "collate": "C",
                "ctype": "C",
                "datcollate": "C",
                "datctype": "C",
            }
            return project(items, [db])
        if relations & {"pg_roles", "pg_user", "pg_authid"}:
            role: dict[str, ColumnValue] = {
                "rolname": self._session.user,
                "usename": self._session.user,
                "rolsuper": True,
                "rolcanlogin": True,
                "rolinherit": True,
                "rolcreaterole": True,
                "rolcreatedb": True,
                "rolreplication": False,
                "rolbypassrls": False,
                "rolconnlimit": -1,
            }
            return project(items, [role])
        if "pg_settings" in relations:
            merged = default_settings(self._session)
            merged.update(self._session.settings)
            return project(
                items, [{"name": key, "setting": value} for key, value in sorted(merged.items())]
            )
        # Consulta de catálogo no reconocida: vacía, con sus columnas.
        return project(items, [])

    def _relation_rows(self, model: RelationModel, text: str) -> list[dict[str, ColumnValue]]:
        if _schema_excluded(text):
            return []
        rows = model.relation_rows()
        kinds = _relkind_filter(text)
        if kinds is not None:
            rows = [r for r in rows if r["relkind"] in kinds]
        for name_filter in _name_filters(text, "relname"):
            rows = [r for r in rows if name_filter(str(r["relname"]))]
        oid = _oid_filter(text, r"c\.oid") or _oid_filter(text, r"pg_class\.oid")
        if oid is not None:
            rows = [r for r in rows if r["oid"] == oid]
        rows.sort(key=lambda r: (str(r["schema"]), str(r["name"])))
        return rows

    def _attribute_rows(self, model: RelationModel, text: str) -> list[dict[str, ColumnValue]]:
        oid = _oid_filter(text, r"attrelid")
        table = model.tables_by_oid.get(oid) if oid is not None else None
        if table is None:
            return []
        rows: list[dict[str, ColumnValue]] = []
        for number, column in enumerate(table.columns, start=1):
            type_name = _PG_TYPE_NAMES[column.column_type.name]
            rows.append(
                {
                    "attname": column.name,
                    "attnum": number,
                    "format_type": type_name,
                    "atttypid": _PG_TYPE_OIDS[column.column_type.name],
                    "pg_get_expr": _default_sql(column),
                    "attnotnull": column.not_null,
                    "attcollation": None,
                    "attidentity": "",
                    "attgenerated": "",
                    "atthasdef": column.has_default,
                    "attstorage": "x" if column.column_type.name == "STRING" else "p",
                    "attcompression": "",
                    "attstattarget": None,
                    "col_description": None,
                }
            )
        return rows

    def _index_rows(self, model: RelationModel, text: str) -> list[dict[str, ColumnValue]]:
        oid = _oid_filter(text, r"c\.oid")
        table = model.tables_by_oid.get(oid) if oid is not None else None
        if table is None:
            return []
        rows: list[dict[str, ColumnValue]] = []
        for index in model.indexes_of(table):
            cols = ", ".join(index.columns)
            unique = "UNIQUE " if index.unique else ""
            constraint_def: str | None = None
            if index.constraint == "p":
                constraint_def = f"PRIMARY KEY ({cols})"
            elif index.constraint == "u":
                constraint_def = f"UNIQUE ({cols})"
            rows.append(
                {
                    "relname": index.name,
                    "indisprimary": index.primary,
                    "indisunique": index.unique,
                    "indisclustered": False,
                    "indisvalid": True,
                    "pg_get_indexdef": (
                        f"CREATE {unique}INDEX {index.name} ON public.{table.name} "
                        f"USING btree ({cols})"
                    ),
                    "pg_get_constraintdef": constraint_def,
                    "contype": index.constraint,
                    "condeferrable": False if index.constraint else None,
                    "condeferred": False if index.constraint else None,
                    "indisreplident": False,
                    "reltablespace": 0,
                }
            )
        rows.sort(key=lambda r: (not r["indisprimary"], str(r["relname"])))
        return rows

    def _information_schema_tables(
        self, model: RelationModel, text: str
    ) -> list[dict[str, ColumnValue]]:
        if re.search(r"table_schema\s*=\s*'(?!public')", text, re.I):
            return []
        rows: list[dict[str, ColumnValue]] = [
            {
                "table_catalog": self._session.database,
                "table_schema": "public",
                "table_name": table.name,
                "table_type": "BASE TABLE",
            }
            for table in model.catalog.tables.values()
        ]
        for name_filter in _name_filters(text, "table_name"):
            rows = [r for r in rows if name_filter(str(r["table_name"]))]
        return rows

    def _information_schema_columns(
        self, model: RelationModel, text: str
    ) -> list[dict[str, ColumnValue]]:
        if re.search(r"table_schema\s*=\s*'(?!public')", text, re.I):
            return []
        rows: list[dict[str, ColumnValue]] = []
        for table in model.catalog.tables.values():
            for number, column in enumerate(table.columns, start=1):
                rows.append(
                    {
                        "table_catalog": self._session.database,
                        "table_schema": "public",
                        "table_name": table.name,
                        "column_name": column.name,
                        "ordinal_position": number,
                        "data_type": _PG_TYPE_NAMES[column.column_type.name],
                        "is_nullable": "NO" if column.not_null else "YES",
                        "column_default": _default_sql(column),
                    }
                )
        for name_filter in _name_filters(text, "table_name"):
            rows = [r for r in rows if name_filter(str(r["table_name"]))]
        return rows


def _constant_oid(value: ColumnValue) -> int:
    if isinstance(value, bool):
        return OID_BOOL
    if isinstance(value, int):
        return OID_INT4
    if isinstance(value, float):
        return 701
    return OID_TEXT


def default_settings(session: SessionInfo) -> dict[str, str]:
    return {
        "server_version": PG_COMPAT_VERSION,
        "server_version_num": PG_COMPAT_VERSION_NUM,
        "server_encoding": "UTF8",
        "client_encoding": "UTF8",
        "standard_conforming_strings": "on",
        "datestyle": "ISO, MDY",
        "timezone": "UTC",
        "intervalstyle": "postgres",
        "integer_datetimes": "on",
        "search_path": '"$user", public',
        "max_identifier_length": "63",
        "application_name": "",
        "is_superuser": "on",
        "session_authorization": session.user,
        "default_transaction_isolation": "read committed",
        "default_transaction_read_only": "off",
        "transaction_read_only": "off",
        "lc_messages": "C",
        "lc_monetary": "C",
        "lc_numeric": "C",
        "lc_time": "C",
        "extra_float_digits": "1",
        "bytea_output": "hex",
        "statement_timeout": "0",
        "lock_timeout": "0",
        "idle_in_transaction_session_timeout": "0",
        "client_min_messages": "notice",
        "max_connections": "100",
        "in_hot_standby": "off",
        "scram_iterations": "4096",
        "password_encryption": "scram-sha-256",
    }
