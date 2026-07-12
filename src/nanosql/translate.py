"""Traducción estructural entre los tres motores del pipeline de consultas.

`sql-query-parser`, `cost-based-query-optimizer` y `vectorized-execution-
engine` no se importan entre sí (regla del ecosistema): cada uno redefine
localmente los nodos que necesita, con los mismos nombres y campos. Este
módulo es el único sitio donde las tres jerarquías coexisten, y convierte:

  * AST del parser → AST del optimizador (`select_to_optimizer`), y
  * plan físico del optimizador → plan físico del ejecutor
    (`physical_plan_to_executor`).

La conversión es deliberadamente mecánica — recorrer el árbol y reconstruir
el nodo homólogo — sin ninguna reescritura semántica: toda decisión de
planificación pertenece al optimizador y toda semántica de evaluación al
ejecutor. Los enums se mapean por nombre de miembro (idénticos en los tres
módulos, verificado aquí en el arranque del import, no confiado a fe ciega).
"""

from __future__ import annotations

from dataclasses import dataclass

import cost_based_query_optimizer.models as om
import cost_based_query_optimizer.plan as oplan
import sql_query_parser.models as pm
import vectorized_execution_engine.models as vm
from vectorized_execution_engine.expressions import expression_key

from .errors import InternalError, NotSupportedError, ProgrammingError


def _check_enum_parity() -> None:
    """Los mapeos por nombre de miembro de abajo asumen que los tres módulos
    declaran exactamente los mismos miembros. Se comprueba una vez al
    importar: si un subproyecto evoluciona y diverge, el fallo es inmediato
    y explícito aquí, no un `KeyError` a mitad de una consulta."""
    pairs: list[tuple[object, object]] = [
        (pm.BinaryOperator, om.BinaryOperator),
        (om.BinaryOperator, vm.BinaryOperator),
        (pm.UnaryOperator, om.UnaryOperator),
        (om.UnaryOperator, vm.UnaryOperator),
        (pm.LiteralType, om.LiteralType),
        (om.LiteralType, vm.LiteralType),
        (pm.JoinType, om.JoinType),
        (om.JoinType, vm.JoinType),
        (pm.OrderDirection, om.OrderDirection),
        (om.OrderDirection, vm.OrderDirection),
    ]
    for left, right in pairs:
        left_names = {member.name for member in left}  # type: ignore[attr-defined]
        right_names = {member.name for member in right}  # type: ignore[attr-defined]
        if left_names != right_names:
            raise InternalError(
                f"los enums {left!r} y {right!r} han divergido entre subproyectos: "
                f"{left_names ^ right_names}"
            )


_check_enum_parity()


# ---------------------------------------------------------------------------
# Parser → Optimizador
# ---------------------------------------------------------------------------


def expression_to_optimizer(expr: pm.Expression) -> om.Expression:
    if isinstance(expr, pm.Literal):
        return om.Literal(value=expr.value, literal_type=om.LiteralType[expr.literal_type.name])
    if isinstance(expr, pm.ColumnRef):
        return om.ColumnRef(name=expr.name, table=expr.table)
    if isinstance(expr, pm.Star):
        return om.Star()
    if isinstance(expr, pm.BinaryOp):
        return om.BinaryOp(
            left=expression_to_optimizer(expr.left),
            operator=om.BinaryOperator[expr.operator.name],
            right=expression_to_optimizer(expr.right),
        )
    if isinstance(expr, pm.UnaryOp):
        return om.UnaryOp(
            operator=om.UnaryOperator[expr.operator.name],
            operand=expression_to_optimizer(expr.operand),
        )
    if isinstance(expr, pm.Between):
        return om.Between(
            operand=expression_to_optimizer(expr.operand),
            low=expression_to_optimizer(expr.low),
            high=expression_to_optimizer(expr.high),
            negated=expr.negated,
        )
    if isinstance(expr, pm.InList):
        return om.InList(
            operand=expression_to_optimizer(expr.operand),
            values=tuple(expression_to_optimizer(value) for value in expr.values),
            negated=expr.negated,
        )
    if isinstance(expr, pm.Like):
        return om.Like(
            operand=expression_to_optimizer(expr.operand),
            pattern=expression_to_optimizer(expr.pattern),
            negated=expr.negated,
        )
    if isinstance(expr, pm.IsNull):
        return om.IsNull(operand=expression_to_optimizer(expr.operand), negated=expr.negated)
    if isinstance(expr, pm.FunctionCall):
        # El parser conserva el nombre tal cual se escribió (`count`,
        # `Count`...); el optimizador clasifica agregadas comparando contra
        # AGGREGATE_FUNCTION_NAMES en mayúsculas — se normaliza aquí.
        return om.FunctionCall(
            name=expr.name.upper(),
            arguments=tuple(expression_to_optimizer(arg) for arg in expr.arguments),
            distinct=expr.distinct,
            star_argument=expr.star_argument,
        )
    raise NotSupportedError(
        f"expresión SQL no soportada por el planificador: {type(expr).__name__}"
    )


def _table_ref_to_optimizer(ref: pm.TableRef) -> om.TableRef:
    return om.TableRef(name=ref.name, alias=ref.alias)


def _select_item_output_name(item: pm.SelectItem) -> str:
    """Nombre con el que el ejecutor expondrá este item proyectado: el alias
    si existe, el nombre desnudo para una referencia de columna simple, y en
    cualquier otro caso la clave canónica de la expresión — calculada con el
    `expression_key` REAL del ejecutor (no una imitación), traduciendo el
    item por el mismo camino que seguirá el plan."""
    if item.alias is not None:
        return item.alias
    if isinstance(item.expression, pm.ColumnRef):
        return item.expression.name
    return expression_key(
        optimizer_expression_to_executor(expression_to_optimizer(item.expression))
    )


@dataclass(frozen=True, slots=True)
class TranslatedSelect:
    """`SELECT` ya traducido al AST del optimizador, más los nombres de las
    columnas ocultas que la traducción añadió a la proyección (claves de
    `ORDER BY` no proyectadas por el usuario): el engine debe recortarlas
    del resultado final antes de devolver filas."""

    statement: om.SelectStatement
    hidden_columns: tuple[str, ...] = ()


def _normalize_order_by(
    stmt: pm.SelectStatement,
) -> tuple[tuple[om.OrderByItem, ...], tuple[om.SelectItem, ...], tuple[str, ...]]:
    """Reescribe cada clave de `ORDER BY` como referencia a una columna de
    SALIDA del `SELECT`, y devuelve `(order_by, items_extra, nombres_ocultos)`.

    Contrato heredado del ecosistema: el optimizador coloca el `Sort` por
    encima del `Project` (semántica SQL: se ordena el resultado), así que en
    ejecución las claves de ordenación se resuelven contra el esquema YA
    proyectado — donde `u.name AS n` se llama `n`, no `u.name`. Para ordenar
    por una expresión que el usuario no proyectó (SQL válido: `SELECT v FROM
    t ORDER BY id`), la clave se añade como columna proyectada oculta con un
    alias interno y el engine la recorta del resultado — la alternativa,
    inyectar nodos de plan propios, invadiría la responsabilidad del
    optimizador.
    """
    if not stmt.order_by:
        return (), (), ()
    has_star = any(isinstance(item.expression, pm.Star) for item in stmt.columns)
    named_items = [
        (item, _select_item_output_name(item))
        for item in stmt.columns
        if not isinstance(item.expression, pm.Star)
    ]
    output_names = {name for _, name in named_items}

    normalized: list[om.OrderByItem] = []
    extra_items: list[om.SelectItem] = []
    hidden_names: list[str] = []
    for position, order_item in enumerate(stmt.order_by):
        expr = order_item.expression
        direction = om.OrderDirection[order_item.direction.name]
        replacement: om.Expression | None = None
        for item, output_name in named_items:
            if item.expression == expr:
                replacement = om.ColumnRef(name=output_name)
                break
        if replacement is None and isinstance(expr, pm.ColumnRef) and expr.table is None:
            if expr.name in output_names:
                replacement = om.ColumnRef(name=expr.name)
        if replacement is None and has_star:
            # Con `SELECT *` las columnas de origen atraviesan el proyector
            # con su nombre cualificado: la clave original se resuelve tal
            # cual, sin columna oculta.
            replacement = expression_to_optimizer(expr)
        if replacement is None and stmt.distinct:
            # Con DISTINCT, una clave de ORDER BY que no sea columna de la
            # lista SELECT cambiaría el resultado (la columna oculta entraría
            # en la deduplicación). SQL estándar lo prohíbe; nosotros también.
            raise ProgrammingError(
                "con SELECT DISTINCT, ORDER BY solo puede referenciar "
                "columnas de la propia lista SELECT"
            )
        if replacement is None:
            hidden = f"__order_by_{position}__"
            extra_items.append(
                om.SelectItem(expression=expression_to_optimizer(expr), alias=hidden)
            )
            hidden_names.append(hidden)
            replacement = om.ColumnRef(name=hidden)
        normalized.append(om.OrderByItem(expression=replacement, direction=direction))
    return tuple(normalized), tuple(extra_items), tuple(hidden_names)


def select_to_optimizer(stmt: pm.SelectStatement) -> TranslatedSelect:
    order_by, extra_items, hidden_names = _normalize_order_by(stmt)
    columns = tuple(
        om.SelectItem(expression=expression_to_optimizer(item.expression), alias=item.alias)
        for item in stmt.columns
    )
    statement = om.SelectStatement(
        columns=columns + extra_items,
        from_table=_table_ref_to_optimizer(stmt.from_table),
        distinct=stmt.distinct,
        joins=tuple(
            om.Join(
                join_type=om.JoinType[join.join_type.name],
                table=_table_ref_to_optimizer(join.table),
                on=expression_to_optimizer(join.on),
            )
            for join in stmt.joins
        ),
        where=expression_to_optimizer(stmt.where) if stmt.where is not None else None,
        group_by=tuple(expression_to_optimizer(expr) for expr in stmt.group_by),
        having=expression_to_optimizer(stmt.having) if stmt.having is not None else None,
        order_by=order_by,
        limit=stmt.limit,
        offset=stmt.offset,
    )
    return TranslatedSelect(statement=statement, hidden_columns=hidden_names)


# ---------------------------------------------------------------------------
# Optimizador → Ejecutor
# ---------------------------------------------------------------------------


def optimizer_expression_to_executor(expr: om.Expression) -> vm.Expression:
    if isinstance(expr, om.Literal):
        return vm.Literal(value=expr.value, literal_type=vm.LiteralType[expr.literal_type.name])
    if isinstance(expr, om.ColumnRef):
        return vm.ColumnRef(name=expr.name, table=expr.table)
    if isinstance(expr, om.Star):
        return vm.Star()
    if isinstance(expr, om.BinaryOp):
        return vm.BinaryOp(
            left=optimizer_expression_to_executor(expr.left),
            operator=vm.BinaryOperator[expr.operator.name],
            right=optimizer_expression_to_executor(expr.right),
        )
    if isinstance(expr, om.UnaryOp):
        return vm.UnaryOp(
            operator=vm.UnaryOperator[expr.operator.name],
            operand=optimizer_expression_to_executor(expr.operand),
        )
    if isinstance(expr, om.Between):
        return vm.Between(
            operand=optimizer_expression_to_executor(expr.operand),
            low=optimizer_expression_to_executor(expr.low),
            high=optimizer_expression_to_executor(expr.high),
            negated=expr.negated,
        )
    if isinstance(expr, om.InList):
        return vm.InList(
            operand=optimizer_expression_to_executor(expr.operand),
            values=tuple(optimizer_expression_to_executor(value) for value in expr.values),
            negated=expr.negated,
        )
    if isinstance(expr, om.Like):
        return vm.Like(
            operand=optimizer_expression_to_executor(expr.operand),
            pattern=optimizer_expression_to_executor(expr.pattern),
            negated=expr.negated,
        )
    if isinstance(expr, om.IsNull):
        return vm.IsNull(
            operand=optimizer_expression_to_executor(expr.operand), negated=expr.negated
        )
    if isinstance(expr, om.FunctionCall):
        return vm.FunctionCall(
            name=expr.name,
            arguments=tuple(optimizer_expression_to_executor(arg) for arg in expr.arguments),
            distinct=expr.distinct,
            star_argument=expr.star_argument,
        )
    raise InternalError(
        f"el optimizador produjo una expresión que el traductor no conoce: {type(expr).__name__}"
    )


def _function_call_to_executor(call: om.FunctionCall) -> vm.FunctionCall:
    translated = optimizer_expression_to_executor(call)
    if not isinstance(translated, vm.FunctionCall):  # pragma: no cover - imposible por firma
        raise InternalError("la traducción de una FunctionCall no produjo una FunctionCall")
    return translated


def _table_ref_to_executor(ref: om.TableRef) -> vm.TableRef:
    return vm.TableRef(name=ref.name, alias=ref.alias)


def physical_plan_to_executor(plan: oplan.PhysicalOperator) -> vm.PhysicalOperator:
    """Convierte el árbol físico elegido por el optimizador en el árbol
    homólogo del ejecutor. Los costes/cardinalidades estimados se copian tal
    cual: el ejecutor no los usa para decidir (ya está todo decidido), pero
    conservarlos mantiene `explain` coherente a ambos lados."""
    if isinstance(plan, oplan.PhysicalTableScan):
        return vm.PhysicalTableScan(
            table=_table_ref_to_executor(plan.table),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalIndexScan):
        return vm.PhysicalIndexScan(
            table=_table_ref_to_executor(plan.table),
            index_name=plan.index_name,
            index_predicate=optimizer_expression_to_executor(plan.index_predicate),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalFilter):
        return vm.PhysicalFilter(
            input=physical_plan_to_executor(plan.input),
            predicate=optimizer_expression_to_executor(plan.predicate),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalNestedLoopJoin):
        return vm.PhysicalNestedLoopJoin(
            left=physical_plan_to_executor(plan.left),
            right=physical_plan_to_executor(plan.right),
            join_type=vm.JoinType[plan.join_type.name],
            condition=(
                optimizer_expression_to_executor(plan.condition)
                if plan.condition is not None
                else None
            ),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalHashJoin):
        return vm.PhysicalHashJoin(
            left=physical_plan_to_executor(plan.left),
            right=physical_plan_to_executor(plan.right),
            join_type=vm.JoinType[plan.join_type.name],
            condition=optimizer_expression_to_executor(plan.condition),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalHashAggregate):
        return vm.PhysicalHashAggregate(
            input=physical_plan_to_executor(plan.input),
            group_by=tuple(optimizer_expression_to_executor(expr) for expr in plan.group_by),
            aggregates=tuple(_function_call_to_executor(call) for call in plan.aggregates),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalProject):
        return vm.PhysicalProject(
            input=physical_plan_to_executor(plan.input),
            items=tuple(
                vm.SelectItem(
                    expression=optimizer_expression_to_executor(item.expression),
                    alias=item.alias,
                )
                for item in plan.items
            ),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalDistinct):
        return vm.PhysicalDistinct(
            input=physical_plan_to_executor(plan.input),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalSort):
        return vm.PhysicalSort(
            input=physical_plan_to_executor(plan.input),
            order_by=tuple(
                vm.OrderByItem(
                    expression=optimizer_expression_to_executor(item.expression),
                    direction=vm.OrderDirection[item.direction.name],
                )
                for item in plan.order_by
            ),
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    if isinstance(plan, oplan.PhysicalLimit):
        return vm.PhysicalLimit(
            input=physical_plan_to_executor(plan.input),
            limit=plan.limit,
            offset=plan.offset,
            estimated_rows=plan.estimated_rows,
            estimated_cost=plan.estimated_cost,
        )
    raise InternalError(
        f"el optimizador produjo un operador físico que el traductor no conoce: "
        f"{type(plan).__name__}"
    )
