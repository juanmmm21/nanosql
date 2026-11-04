"""Materialización bottom-up de subconsultas NO correlacionadas.

`sql-query-parser` produce nodos `Subquery` (subconsulta escalar) e
`InSubquery` (`expr [NOT] IN (SELECT ...)`); este módulo los elimina del AST
ANTES de cualquier traducción al optimizador — el mismo punto del pipeline
en que `params.bind_statement` elimina los `Placeholder` — de forma que el
resto del pipeline (optimizador → ejecutor vectorizado) funciona sin cambios.

Estrategia en dos pasadas:

1. **Detección estática de correlación** (`_check_uncorrelated`): toda
   `ColumnRef` de una subconsulta debe resolverse contra las tablas DE esa
   subconsulta. Una referencia que solo resuelve contra una consulta
   envolvente es una subconsulta correlacionada — fuera del alcance de v1 —
   y se rechaza con `NotSupportedError` y mensaje claro, nunca con un error
   críptico aguas abajo. Como en SQL, el ámbito interno hace sombra al
   externo: una columna presente en ambos resuelve contra el interno.
2. **Expansión bottom-up** (`expand_statement`): cada subconsulta, ya
   garantizada no correlacionada, es un SELECT independiente — se ejecuta
   recursivamente (con el MISMO txn/snapshot que la consulta externa, eso
   lo garantiza el runner que inyecta el engine) y su resultado se
   sustituye en el AST: un `InList` de literales para `IN (SELECT ...)`,
   un literal escalar para la posición escalar. Las subconsultas anidadas
   las da la propia recursión.

Coste documentado (README): el resultado de una subconsulta IN se
materializa entero como lista de literales en memoria — proporcional al
número de filas que proyecta, sin spill a disco.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sql_query_parser.models as pm

from .catalog import Catalog
from .errors import DataError, NotSupportedError, ProgrammingError
from .params import literal_for_value

if TYPE_CHECKING:
    from .engine import StatementResult

# Ejecuta un SELECT ya expandido (sin subconsultas) y devuelve su resultado.
# El engine lo liga a la transacción de la sentencia externa: es lo que
# garantiza que subconsulta y consulta externa vean el mismo snapshot MVCC.
SelectRunner = Callable[["pm.SelectStatement"], "StatementResult"]


# ---------------------------------------------------------------------------
# Ámbitos y detección estática de correlación
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Scope:
    """Nombres visibles dentro de un SELECT: cualificadores de tabla (el
    alias oculta el nombre real, como en SQL estándar) y nombres de columna
    de todas sus tablas."""

    qualifiers: frozenset[str]
    columns: frozenset[str]

    def resolves(self, ref: pm.ColumnRef) -> bool:
        if ref.table is not None:
            return ref.table in self.qualifiers
        return ref.name in self.columns


def _scope_of(select: pm.SelectStatement, catalog: Catalog) -> _Scope:
    tables = [select.from_table, *(join.table for join in select.joins)]
    qualifiers = frozenset(table.alias or table.name for table in tables)
    columns: set[str] = set()
    for table in tables:
        schema = catalog.tables.get(table.name)
        # Tabla inexistente: no se aborta aquí — la ejecución de ese SELECT
        # producirá el error tipado normal ("la tabla ... no existe").
        if schema is not None:
            columns.update(schema.column_names())
    return _Scope(qualifiers=qualifiers, columns=frozenset(columns))


def _select_expressions(select: pm.SelectStatement) -> Iterator[pm.Expression]:
    for item in select.columns:
        yield item.expression
    for join in select.joins:
        yield join.on
    if select.where is not None:
        yield select.where
    yield from select.group_by
    if select.having is not None:
        yield select.having
    for order_item in select.order_by:
        yield order_item.expression


def _expression_children(expr: pm.Expression) -> Iterator[pm.Expression]:
    """Hijos directos de un nodo de expresión, SIN atravesar la frontera de
    una subconsulta — los ámbitos de una subconsulta se procesan aparte."""
    if isinstance(expr, pm.BinaryOp):
        yield expr.left
        yield expr.right
    elif isinstance(expr, pm.UnaryOp):
        yield expr.operand
    elif isinstance(expr, pm.Between):
        yield expr.operand
        yield expr.low
        yield expr.high
    elif isinstance(expr, pm.InList):
        yield expr.operand
        yield from expr.values
    elif isinstance(expr, pm.Like):
        yield expr.operand
        yield expr.pattern
    elif isinstance(expr, pm.IsNull):
        yield expr.operand
    elif isinstance(expr, pm.FunctionCall):
        yield from expr.arguments
    elif isinstance(expr, pm.InSubquery):
        yield expr.operand


def _iter_subqueries(expr: pm.Expression) -> Iterator[pm.Subquery]:
    """Subconsultas inmediatas de `expr` (las anidadas más profundo se
    alcanzan recursivamente al procesar cada nivel)."""
    if isinstance(expr, pm.Subquery):
        yield expr
        return
    if isinstance(expr, pm.InSubquery):
        yield expr.subquery
    for child in _expression_children(expr):
        yield from _iter_subqueries(child)


def _check_uncorrelated(
    select: pm.SelectStatement, outer_scopes: tuple[_Scope, ...], catalog: Catalog
) -> None:
    """Rechaza (con `NotSupportedError`) cualquier `ColumnRef` de `select`
    que NO resuelva contra las tablas del propio `select` pero SÍ contra
    alguna consulta envolvente. Una referencia que no resuelve en ningún
    ámbito se deja pasar: la ejecución dará su error normal de columna no
    resuelta, que es el diagnóstico correcto para una errata."""
    inner = _scope_of(select, catalog)
    for expr in _select_expressions(select):
        _check_expression(expr, inner, outer_scopes, catalog)


def _check_expression(
    expr: pm.Expression,
    inner: _Scope,
    outer_scopes: tuple[_Scope, ...],
    catalog: Catalog,
) -> None:
    if isinstance(expr, pm.ColumnRef):
        if inner.resolves(expr):
            return
        if any(scope.resolves(expr) for scope in outer_scopes):
            name = f"{expr.table}.{expr.name}" if expr.table is not None else expr.name
            raise NotSupportedError(
                f"subconsulta correlacionada no soportada: la columna '{name}' no "
                "pertenece a ninguna tabla de la subconsulta y se resuelve contra "
                "la consulta externa (v1 solo soporta subconsultas no correlacionadas)"
            )
        return
    if isinstance(expr, pm.Subquery):
        _check_uncorrelated(expr.statement, (*outer_scopes, inner), catalog)
        return
    if isinstance(expr, pm.InSubquery):
        _check_expression(expr.operand, inner, outer_scopes, catalog)
        _check_uncorrelated(expr.subquery.statement, (*outer_scopes, inner), catalog)
        return
    for child in _expression_children(expr):
        _check_expression(child, inner, outer_scopes, catalog)


# ---------------------------------------------------------------------------
# Expansión (materialización) bottom-up
# ---------------------------------------------------------------------------


def _materialize_scalar(select: pm.SelectStatement, run_select: SelectRunner) -> pm.Literal:
    """`(SELECT ...)` escalar → literal. Semántica SQL estándar: sin filas
    es NULL; más de una fila es un error de cardinalidad — `DataError`
    porque depende de los datos, no de la forma de la sentencia."""
    result = run_select(select)
    if len(result.columns) != 1:
        raise ProgrammingError(
            f"una subconsulta escalar debe proyectar exactamente 1 columna, "
            f"proyecta {len(result.columns)}"
        )
    if len(result.rows) > 1:
        raise DataError(f"la subconsulta escalar devolvió {len(result.rows)} filas (máximo una)")
    if not result.rows:
        return pm.Literal(None, pm.LiteralType.NULL)
    return literal_for_value(result.rows[0][0])


def _materialize_in_values(
    select: pm.SelectStatement, run_select: SelectRunner
) -> tuple[pm.Expression, ...]:
    """`IN (SELECT ...)` → tupla de literales para un `InList`. Una tupla
    vacía es válida y correcta: el ejecutor evalúa `x IN ()` como FALSE y
    `x NOT IN ()` como TRUE, incluso con operando NULL (semántica de
    conjunto vacío). Los NULL del resultado se conservan como literales
    NULL: la lógica trivalente del ejecutor hace el resto (`x NOT IN (...,
    NULL)` nunca es TRUE)."""
    result = run_select(select)
    if len(result.columns) != 1:
        raise ProgrammingError(
            f"la subconsulta de IN debe proyectar exactamente 1 columna, "
            f"proyecta {len(result.columns)}"
        )
    return tuple(literal_for_value(row[0]) for row in result.rows)


def _expand_expression(
    expr: pm.Expression, catalog: Catalog, run_select: SelectRunner
) -> pm.Expression:
    if isinstance(expr, pm.Subquery):
        expanded = _expand_select(expr.statement, catalog, run_select)
        return _materialize_scalar(expanded, run_select)
    if isinstance(expr, pm.InSubquery):
        expanded = _expand_select(expr.subquery.statement, catalog, run_select)
        return pm.InList(
            operand=_expand_expression(expr.operand, catalog, run_select),
            values=_materialize_in_values(expanded, run_select),
            negated=expr.negated,
        )
    if isinstance(expr, pm.BinaryOp):
        return pm.BinaryOp(
            left=_expand_expression(expr.left, catalog, run_select),
            operator=expr.operator,
            right=_expand_expression(expr.right, catalog, run_select),
        )
    if isinstance(expr, pm.UnaryOp):
        return pm.UnaryOp(
            operator=expr.operator,
            operand=_expand_expression(expr.operand, catalog, run_select),
        )
    if isinstance(expr, pm.Between):
        return pm.Between(
            operand=_expand_expression(expr.operand, catalog, run_select),
            low=_expand_expression(expr.low, catalog, run_select),
            high=_expand_expression(expr.high, catalog, run_select),
            negated=expr.negated,
        )
    if isinstance(expr, pm.InList):
        return pm.InList(
            operand=_expand_expression(expr.operand, catalog, run_select),
            values=tuple(_expand_expression(v, catalog, run_select) for v in expr.values),
            negated=expr.negated,
        )
    if isinstance(expr, pm.Like):
        return pm.Like(
            operand=_expand_expression(expr.operand, catalog, run_select),
            pattern=_expand_expression(expr.pattern, catalog, run_select),
            negated=expr.negated,
        )
    if isinstance(expr, pm.IsNull):
        return pm.IsNull(
            operand=_expand_expression(expr.operand, catalog, run_select),
            negated=expr.negated,
        )
    if isinstance(expr, pm.FunctionCall):
        return pm.FunctionCall(
            name=expr.name,
            arguments=tuple(_expand_expression(a, catalog, run_select) for a in expr.arguments),
            distinct=expr.distinct,
            star_argument=expr.star_argument,
        )
    # Literal, ColumnRef, Star, Placeholder (ya ligado antes de llegar aquí):
    # hojas sin subconsultas dentro.
    return expr


def _expand_optional(
    expr: pm.Expression | None, catalog: Catalog, run_select: SelectRunner
) -> pm.Expression | None:
    return None if expr is None else _expand_expression(expr, catalog, run_select)


def _expand_select(
    select: pm.SelectStatement, catalog: Catalog, run_select: SelectRunner
) -> pm.SelectStatement:
    if not any(True for expr in _select_expressions(select) for _ in _iter_subqueries(expr)):
        return select
    return pm.SelectStatement(
        columns=tuple(
            pm.SelectItem(
                expression=_expand_expression(item.expression, catalog, run_select),
                alias=item.alias,
            )
            for item in select.columns
        ),
        from_table=select.from_table,
        distinct=select.distinct,
        joins=tuple(
            pm.Join(
                join_type=join.join_type,
                table=join.table,
                on=_expand_expression(join.on, catalog, run_select),
            )
            for join in select.joins
        ),
        where=_expand_optional(select.where, catalog, run_select),
        group_by=tuple(_expand_expression(expr, catalog, run_select) for expr in select.group_by),
        having=_expand_optional(select.having, catalog, run_select),
        order_by=tuple(
            pm.OrderByItem(
                expression=_expand_expression(item.expression, catalog, run_select),
                direction=item.direction,
            )
            for item in select.order_by
        ),
        limit=select.limit,
        offset=select.offset,
    )


# ---------------------------------------------------------------------------
# API pública para el engine
# ---------------------------------------------------------------------------


def _statement_expressions(stmt: pm.Statement) -> Iterator[pm.Expression]:
    if isinstance(stmt, pm.SelectStatement):
        yield from _select_expressions(stmt)
    elif isinstance(stmt, pm.InsertStatement):
        for row in stmt.rows:
            yield from row
        if stmt.source is not None:
            yield from _select_expressions(stmt.source)
    elif isinstance(stmt, pm.UpdateStatement):
        for assignment in stmt.assignments:
            yield assignment.value
        if stmt.where is not None:
            yield stmt.where
    elif isinstance(stmt, pm.DeleteStatement):
        if stmt.where is not None:
            yield stmt.where


def contains_subquery(stmt: pm.Statement) -> bool:
    return any(True for expr in _statement_expressions(stmt) for _ in _iter_subqueries(expr))


def _subquery_tables(select: pm.SelectStatement) -> Iterator[str]:
    yield select.from_table.name
    for join in select.joins:
        yield join.table.name
    for expr in _select_expressions(select):
        for subquery in _iter_subqueries(expr):
            yield from _subquery_tables(subquery.statement)


def statement_tables(stmt: pm.Statement) -> list[str]:
    """Todas las tablas que la sentencia puede leer, INCLUIDAS las de sus
    subconsultas a cualquier profundidad — el engine debe adquirir el lock
    SHARED de todas antes de ejecutar (y en orden total, cosa que ya hace
    `_acquire_table_locks`)."""
    tables: list[str] = []
    if isinstance(stmt, pm.SelectStatement):
        tables.extend(_subquery_tables(stmt))
        return tables
    if isinstance(stmt, pm.InsertStatement | pm.UpdateStatement | pm.DeleteStatement):
        tables.append(stmt.table)
    if isinstance(stmt, pm.InsertStatement) and stmt.source is not None:
        tables.extend(statement_tables(stmt.source))
    for expr in _statement_expressions(stmt):
        for subquery in _iter_subqueries(expr):
            tables.extend(_subquery_tables(subquery.statement))
    return tables


def _outer_scope(stmt: pm.Statement, catalog: Catalog) -> tuple[_Scope, ...]:
    """Ámbito externo contra el que se detecta correlación en las
    subconsultas de primer nivel de la sentencia."""
    if isinstance(stmt, pm.SelectStatement):
        return (_scope_of(stmt, catalog),)
    if isinstance(stmt, pm.UpdateStatement | pm.DeleteStatement):
        schema = catalog.tables.get(stmt.table)
        columns = frozenset(schema.column_names()) if schema is not None else frozenset()
        return (_Scope(qualifiers=frozenset({stmt.table}), columns=columns),)
    if isinstance(stmt, pm.InsertStatement) and stmt.source is not None:
        # INSERT ... SELECT: las subconsultas viven dentro del SELECT origen
        # y su ámbito externo es el de ese SELECT.
        return (_scope_of(stmt.source, catalog),)
    # INSERT ... VALUES: sus VALUES no ven ninguna tabla externa — una
    # referencia de columna dentro de una subconsulta de INSERT solo puede
    # ser de la propia subconsulta.
    return ()


def expand_statement(
    stmt: pm.Statement, catalog: Catalog, run_select: SelectRunner
) -> pm.Statement:
    """Devuelve `stmt` sin nodos `Subquery`/`InSubquery`: cada subconsulta,
    verificada como no correlacionada, ejecutada vía `run_select` y
    sustituida por sus literales. Sin subconsultas, devuelve `stmt` tal
    cual (sin reconstruir el árbol)."""
    if not contains_subquery(stmt):
        return stmt

    outer = _outer_scope(stmt, catalog)
    for expr in _statement_expressions(stmt):
        _check_expression(
            expr,
            inner=outer[0] if outer else _Scope(frozenset(), frozenset()),
            outer_scopes=outer[1:] if outer else (),
            catalog=catalog,
        )

    if isinstance(stmt, pm.SelectStatement):
        return _expand_select(stmt, catalog, run_select)
    if isinstance(stmt, pm.InsertStatement):
        return pm.InsertStatement(
            table=stmt.table,
            columns=stmt.columns,
            rows=tuple(
                tuple(_expand_expression(e, catalog, run_select) for e in row) for row in stmt.rows
            ),
            source=(
                _expand_select(stmt.source, catalog, run_select)
                if stmt.source is not None
                else None
            ),
        )
    if isinstance(stmt, pm.UpdateStatement):
        return pm.UpdateStatement(
            table=stmt.table,
            assignments=tuple(
                pm.Assignment(
                    column=a.column,
                    value=_expand_expression(a.value, catalog, run_select),
                )
                for a in stmt.assignments
            ),
            where=_expand_optional(stmt.where, catalog, run_select),
        )
    if isinstance(stmt, pm.DeleteStatement):
        return pm.DeleteStatement(
            table=stmt.table,
            where=_expand_optional(stmt.where, catalog, run_select),
        )
    return stmt
