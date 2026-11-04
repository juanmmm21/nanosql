"""Ligado de parámetros qmark (`?`) sobre el AST del parser.

`sql-query-parser` produce nodos `Placeholder(index)` para cada `?`; este
módulo los sustituye por literales ANTES de cualquier traducción o
evaluación, devolviendo un AST nuevo (los nodos son dataclasses congeladas)
en el que el resto de `nanosql` no necesita saber que existieron parámetros.
Ligar por sustitución de literales es seguro aquí porque nunca se
reconstruye texto SQL — el valor viaja ya tipado por el árbol, sin ninguna
oportunidad de inyección.
"""

from __future__ import annotations

from collections.abc import Sequence

import sql_query_parser.models as pm

from .catalog import ColumnValue
from .errors import DataError, ProgrammingError


def literal_for_value(value: ColumnValue) -> pm.Literal:
    """Literal tipado del AST del parser para un valor Python plano. Público
    dentro del paquete: lo comparte `subqueries.py` para materializar los
    resultados de una subconsulta como literales."""
    # bool antes que int: en Python bool es subclase de int.
    if value is None:
        return pm.Literal(None, pm.LiteralType.NULL)
    if isinstance(value, bool):
        return pm.Literal(value, pm.LiteralType.BOOLEAN)
    if isinstance(value, int):
        return pm.Literal(value, pm.LiteralType.INTEGER)
    if isinstance(value, float):
        return pm.Literal(value, pm.LiteralType.FLOAT)
    if isinstance(value, str):
        return pm.Literal(value, pm.LiteralType.STRING)
    raise DataError(
        f"tipo de parámetro no soportado: {type(value).__name__} "
        "(admitidos: int, float, str, bool, None)"
    )


class _Binder:
    def __init__(self, params: Sequence[ColumnValue] | None) -> None:
        self._params = params
        self.placeholders_seen = 0

    def bind(self, placeholder: pm.Placeholder) -> pm.Literal:
        self.placeholders_seen += 1
        if self._params is None or placeholder.index >= len(self._params):
            supplied = 0 if self._params is None else len(self._params)
            raise ProgrammingError(
                f"la sentencia usa el parámetro ?{placeholder.index + 1} pero solo "
                f"se pasaron {supplied} valores"
            )
        return literal_for_value(self._params[placeholder.index])

    def finish(self) -> None:
        supplied = 0 if self._params is None else len(self._params)
        if self.placeholders_seen != supplied:
            raise ProgrammingError(
                f"la sentencia tiene {self.placeholders_seen} placeholders '?' "
                f"pero se pasaron {supplied} parámetros"
            )


def _bind_expr(expr: pm.Expression, binder: _Binder) -> pm.Expression:
    if isinstance(expr, pm.Placeholder):
        return binder.bind(expr)
    if isinstance(expr, pm.Literal | pm.ColumnRef | pm.Star):
        return expr
    if isinstance(expr, pm.BinaryOp):
        return pm.BinaryOp(
            left=_bind_expr(expr.left, binder),
            operator=expr.operator,
            right=_bind_expr(expr.right, binder),
        )
    if isinstance(expr, pm.UnaryOp):
        return pm.UnaryOp(operator=expr.operator, operand=_bind_expr(expr.operand, binder))
    if isinstance(expr, pm.Between):
        return pm.Between(
            operand=_bind_expr(expr.operand, binder),
            low=_bind_expr(expr.low, binder),
            high=_bind_expr(expr.high, binder),
            negated=expr.negated,
        )
    if isinstance(expr, pm.InList):
        return pm.InList(
            operand=_bind_expr(expr.operand, binder),
            values=tuple(_bind_expr(v, binder) for v in expr.values),
            negated=expr.negated,
        )
    if isinstance(expr, pm.Like):
        return pm.Like(
            operand=_bind_expr(expr.operand, binder),
            pattern=_bind_expr(expr.pattern, binder),
            negated=expr.negated,
        )
    if isinstance(expr, pm.IsNull):
        return pm.IsNull(operand=_bind_expr(expr.operand, binder), negated=expr.negated)
    if isinstance(expr, pm.FunctionCall):
        return pm.FunctionCall(
            name=expr.name,
            arguments=tuple(_bind_expr(a, binder) for a in expr.arguments),
            distinct=expr.distinct,
            star_argument=expr.star_argument,
        )
    if isinstance(expr, pm.Subquery):
        # Los placeholders de una subconsulta comparten la numeración de su
        # sentencia externa (contrato del parser): se ligan con el mismo
        # binder, atravesando la subconsulta entera.
        return pm.Subquery(statement=_bind_select(expr.statement, binder))
    if isinstance(expr, pm.InSubquery):
        return pm.InSubquery(
            operand=_bind_expr(expr.operand, binder),
            subquery=pm.Subquery(statement=_bind_select(expr.subquery.statement, binder)),
            negated=expr.negated,
        )
    # Nodo de expresión desconocido: se deja pasar tal cual — si contiene
    # algo no soportado lo dirá (tipado) la capa de traducción, no esta.
    return expr


def _optional(expr: pm.Expression | None, binder: _Binder) -> pm.Expression | None:
    return None if expr is None else _bind_expr(expr, binder)


def _bind_select(stmt: pm.SelectStatement, binder: _Binder) -> pm.SelectStatement:
    return pm.SelectStatement(
        columns=tuple(
            pm.SelectItem(expression=_bind_expr(i.expression, binder), alias=i.alias)
            for i in stmt.columns
        ),
        from_table=stmt.from_table,
        distinct=stmt.distinct,
        joins=tuple(
            pm.Join(
                join_type=j.join_type,
                table=j.table,
                on=_bind_expr(j.on, binder),
            )
            for j in stmt.joins
        ),
        where=_optional(stmt.where, binder),
        group_by=tuple(_bind_expr(e, binder) for e in stmt.group_by),
        having=_optional(stmt.having, binder),
        order_by=tuple(
            pm.OrderByItem(expression=_bind_expr(i.expression, binder), direction=i.direction)
            for i in stmt.order_by
        ),
        limit=stmt.limit,
        offset=stmt.offset,
    )


def bind_statement(stmt: pm.Statement, params: Sequence[ColumnValue] | None) -> pm.Statement:
    """Devuelve `stmt` con cada `?` sustituido por su parámetro tipado.

    Valida la aridad en ambas direcciones: un `?` sin valor y un valor sin
    `?` son errores del programador. El DDL no admite parámetros (los
    DEFAULT de un CREATE TABLE son parte del esquema, no de una ejecución).
    """
    binder = _Binder(params)
    bound: pm.Statement
    if isinstance(stmt, pm.SelectStatement):
        bound = _bind_select(stmt, binder)
    elif isinstance(stmt, pm.InsertStatement):
        bound = pm.InsertStatement(
            table=stmt.table,
            columns=stmt.columns,
            rows=tuple(tuple(_bind_expr(e, binder) for e in row) for row in stmt.rows),
            source=_bind_select(stmt.source, binder) if stmt.source is not None else None,
        )
    elif isinstance(stmt, pm.UpdateStatement):
        bound = pm.UpdateStatement(
            table=stmt.table,
            assignments=tuple(
                pm.Assignment(column=a.column, value=_bind_expr(a.value, binder))
                for a in stmt.assignments
            ),
            where=_optional(stmt.where, binder),
        )
    elif isinstance(stmt, pm.DeleteStatement):
        bound = pm.DeleteStatement(table=stmt.table, where=_optional(stmt.where, binder))
    elif isinstance(
        stmt, pm.CreateTableStatement | pm.CreateIndexStatement | pm.DropTableStatement
    ):
        if params:
            raise ProgrammingError("el DDL no admite parámetros '?'")
        bound = stmt
    else:
        bound = stmt
    binder.finish()
    return bound
