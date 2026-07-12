"""Modo distribuido opcional: nanosql replicado por Raft y shardeado.

Extensión NO requerida para el uso embebido de nodo único (extra de
instalación `nanosql[distributed]`). Integra los dos subproyectos de
distribución del ecosistema, cerrando los 10:

* `ReplicatedNanosql` — N réplicas en memoria coordinadas por el cluster
  Raft simulado de `raft-replication-log`: cada sentencia de escritura se
  propone al líder, se compromete por quorum y cada nodo la aplica a su
  base de datos nanosql local (la máquina de estados replicada ES nanosql).
  Los snapshots de Raft serializan la base de datos completa y permiten
  poner al día a un nodo rezagado sin reenviar todo el log.

* `ShardedNanosql` — K bases de datos nanosql independientes detrás del
  fan-out/merge de `distributed-query-router`: escrituras enrutadas por
  hash de la clave de shard, lecturas repartidas con merge de agregados
  distribuidos correcto (AVG descompuesto en suma y cuenta parciales, la
  media se divide una única vez al final).

Igual que sus dos subproyectos de base, este módulo es una simulación en
proceso (sin sockets): el objetivo es demostrar la integración real de los
protocolos, no un despliegue multi-máquina — decisión de scope heredada y
documentada en el README.
"""

from __future__ import annotations

import json
import random
from collections.abc import Iterator, Sequence

from distributed_query_router.errors import NodeUnavailableError, RouterError
from distributed_query_router.models import (
    AggregateAccumulator,
    AggregateFunction,
    AggregateSpec,
    ConsistencyLevel,
    GroupKey,
    MergedResult,
    MergeSpec,
    NodeId,
    PartialResult,
    ReadRequest,
    Row,
    ShardId,
    ShardReplicaSet,
    WriteAck,
    WriteRequest,
)
from distributed_query_router.pipeline import DistributedRouter
from distributed_query_router.protocols import InMemoryLeadershipOracle
from distributed_query_router.sharding import HashShardingScheme, ShardDirectory
from raft_replication_log.cluster import Cluster
from raft_replication_log.errors import RaftError
from raft_replication_log.pipeline import build_cluster, elect_leader, replicate_command
from sql_query_parser import SqlParserError, parse

from .catalog import ColumnValue, TableSchema
from .driver import Connection, connect
from .errors import NotSupportedError, OperationalError, ProgrammingError
from .subqueries import contains_subquery

# Ticks extra tras comprometer una entrada: los seguidores conocen el nuevo
# commit_index del líder en el siguiente heartbeat (cada 3 ticks en la
# configuración por defecto del cluster) y sólo entonces la aplican a su
# máquina de estados local.
_CATCH_UP_TICKS = 8

_COLUMN_TYPE_TO_SQL = {
    "INTEGER": "INTEGER",
    "FLOAT": "FLOAT",
    "STRING": "TEXT",
    "BOOLEAN": "BOOLEAN",
}


def sql_literal(value: ColumnValue) -> str:
    """Representa un valor Python como literal SQL del subconjunto del
    parser (comillas simples dobladas para escapar dentro de cadenas)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _create_table_sql(table: TableSchema) -> str:
    parts: list[str] = []
    for column in table.columns:
        sql_type = _COLUMN_TYPE_TO_SQL[column.column_type.name]
        flags = ""
        if column.primary_key:
            flags += " PRIMARY KEY"
        elif column.not_null:
            flags += " NOT NULL"
        if column.unique:
            flags += " UNIQUE"
        if column.has_default:
            flags += f" DEFAULT {sql_literal(column.default)}"
        parts.append(f"{column.name} {sql_type}{flags}")
    return f"CREATE TABLE {table.name} ({', '.join(parts)})"


def _insert_sql(table: str, columns: Sequence[str], rows: Sequence[Sequence[ColumnValue]]) -> str:
    values = ", ".join("(" + ", ".join(sql_literal(cell) for cell in row) + ")" for row in rows)
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES {values}"


# ---------------------------------------------------------------------------
# Replicación por Raft
# ---------------------------------------------------------------------------


class NanosqlStateMachine:
    """`raft_replication_log.protocols.StateMachine` cuyo estado es una base
    de datos nanosql `:memory:` completa. Cada comando comprometido es una
    sentencia SQL que se aplica en autocommit; los comandos llegan a todas
    las réplicas en el mismo orden (la garantía de Raft), así que todas las
    bases de datos locales convergen al mismo contenido."""

    def __init__(self) -> None:
        self.connection: Connection = connect(":memory:", autocommit=True)

    def apply(self, command: bytes) -> None:
        self.connection.execute(command.decode("utf-8"))

    def take_snapshot(self) -> bytes:
        """Serializa esquema y datos como JSON. Se reconstruye vía SQL
        público (no tocando el engine por dentro), con literales escapados
        por `sql_literal`."""
        catalog = self.connection._require_engine().catalog
        ddl = [_create_table_sql(table) for table in catalog.tables.values()]
        for index in catalog.indexes.values():
            unique = "UNIQUE " if index.unique else ""
            ddl.append(
                f"CREATE {unique}INDEX {index.name} ON {index.table} ({', '.join(index.columns)})"
            )
        data: dict[str, dict[str, object]] = {}
        for table in catalog.tables.values():
            cursor = self.connection.execute(f"SELECT * FROM {table.name}")
            data[table.name] = {
                "columns": list(table.column_names()),
                "rows": [list(row) for row in cursor.fetchall()],
            }
        return json.dumps({"ddl": ddl, "data": data}, ensure_ascii=False).encode("utf-8")

    def restore_snapshot(self, state: bytes) -> None:
        payload = json.loads(state.decode("utf-8"))
        self.connection.close()
        self.connection = connect(":memory:", autocommit=True)
        for statement in payload["ddl"]:
            self.connection.execute(statement)
        for table, content in payload["data"].items():
            rows = content["rows"]
            if rows:
                self.connection.execute(_insert_sql(table, content["columns"], rows))

    def close(self) -> None:
        self.connection.close()


class ReplicatedNanosql:
    """Cluster nanosql replicado por consenso Raft (simulado, en proceso).

    Las escrituras (`execute`) se comprometen por quorum antes de aplicarse;
    las lecturas (`query`) van por defecto al líder (consistencia fuerte) o
    a una réplica concreta (`node_id=...`, lectura posiblemente rezagada
    hasta el siguiente heartbeat — útil para demostrar la convergencia).
    """

    def __init__(self, num_nodes: int = 3, *, seed: int = 7) -> None:
        node_ids = tuple(f"node-{n}" for n in range(num_nodes))
        machines = [NanosqlStateMachine() for _ in node_ids]
        self._cluster: Cluster = build_cluster(node_ids, seed=seed)
        # Se sustituye la máquina de estados por defecto de cada nodo antes
        # de que el cluster aplique ningún comando: así conservamos las
        # referencias para poder consultar la base de datos local de cada
        # réplica desde fuera.
        for node_id, machine in zip(node_ids, machines, strict=True):
            self._cluster.nodes[node_id].state_machine = machine
        self._machines: dict[str, NanosqlStateMachine] = dict(zip(node_ids, machines, strict=True))

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(self._machines)

    def leader(self) -> str:
        try:
            leader = self._cluster.current_leader()
            return leader if leader is not None else elect_leader(self._cluster)
        except RaftError as exc:
            raise OperationalError(f"el cluster Raft no tiene quorum: {exc}") from exc

    def execute(self, sql: str) -> int:
        """Propone `sql` como comando replicado y espera a que quede
        comprometido por quorum y aplicado por las réplicas alcanzables.
        Devuelve el índice de log asignado."""
        try:
            index = replicate_command(self._cluster, sql.encode("utf-8"))
        except RaftError as exc:
            raise OperationalError(f"no se pudo comprometer la sentencia: {exc}") from exc
        self._cluster.tick(_CATCH_UP_TICKS)
        return int(index)

    def query(self, sql: str, *, node_id: str | None = None) -> list[tuple[ColumnValue, ...]]:
        target = node_id if node_id is not None else self.leader()
        machine = self._machines.get(target)
        if machine is None:
            raise ProgrammingError(f"nodo desconocido: {target!r}")
        return machine.connection.execute(sql).fetchall()

    def crash_node(self, node_id: str) -> None:
        self._cluster.crash_node(node_id)

    def recover_node(self, node_id: str) -> None:
        self._cluster.recover_node(node_id)
        self._cluster.tick(_CATCH_UP_TICKS * 3)  # que el líder lo ponga al día

    def tick(self, times: int = 1) -> None:
        self._cluster.tick(times)

    def close(self) -> None:
        for machine in self._machines.values():
            machine.close()


# ---------------------------------------------------------------------------
# Sharding + fan-out/merge
# ---------------------------------------------------------------------------


class _ShardNodeExecutor:
    """`distributed_query_router.protocols.NodeExecutor` sobre un conjunto de
    bases de datos nanosql locales (una por nodo). El payload de cada
    petición es JSON: `{"kind": "sql"}` para lecturas literales y
    `{"kind": "aggregate"}` para agregados parciales por shard."""

    def __init__(self, connections: dict[NodeId, Connection]) -> None:
        self._connections = connections
        self.failed_nodes: set[NodeId] = set()

    def _connection(self, node_id: NodeId, shard_id: ShardId) -> Connection:
        if node_id in self.failed_nodes:
            raise NodeUnavailableError(shard_id, node_id)
        return self._connections[node_id]

    def execute_read(
        self, node_id: NodeId, shard_id: ShardId, request: ReadRequest
    ) -> PartialResult:
        payload = json.loads(request.payload.decode("utf-8"))
        connection = self._connection(node_id, shard_id)
        if payload["kind"] == "sql":
            cursor = connection.execute(payload["sql"])
            description = cursor.description or []
            names = [d[0] for d in description]
            rows: tuple[Row, ...] = tuple(
                dict(zip(names, row, strict=True)) for row in cursor.fetchall()
            )
            return PartialResult(shard_id=shard_id, node_id=node_id, rows=rows)
        if payload["kind"] == "aggregate":
            return self._partial_aggregate(node_id, shard_id, connection, payload)
        raise RouterError(f"payload de lectura desconocido: {payload['kind']!r}")

    def _partial_aggregate(
        self,
        node_id: NodeId,
        shard_id: ShardId,
        connection: Connection,
        payload: dict[str, object],
    ) -> PartialResult:
        table = str(payload["table"])
        raw_group_by = payload["group_by"]
        raw_aggregates = payload["aggregates"]
        if not isinstance(raw_group_by, list) or not isinstance(raw_aggregates, list):
            raise RouterError(f"payload de agregación malformado: {payload!r}")
        group_by = [str(c) for c in raw_group_by]
        specs = [
            (AggregateFunction(str(f)), str(col) if col is not None else None, str(out))
            for f, col, out in raw_aggregates
        ]
        where = payload.get("where")

        # Cada agregado se descompone en sus estados parciales distributivos:
        # AVG necesita SUM y COUNT por separado (la media de medias pondera
        # mal entre shards de tamaño distinto — el merge del router divide
        # una única vez al final).
        select_parts = list(group_by)
        for position, (function, column, _out) in enumerate(specs):
            argument = column if column is not None else "*"
            if function is AggregateFunction.AVG:
                select_parts.append(f"SUM({argument}) AS p{position}_sum")
                select_parts.append(f"COUNT({argument}) AS p{position}_count")
            else:
                select_parts.append(f"{function.value}({argument}) AS p{position}")
        sql = f"SELECT {', '.join(select_parts)} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        if group_by:
            sql += f" GROUP BY {', '.join(group_by)}"

        cursor = connection.execute(sql)
        description = cursor.description or []
        names = [d[0] for d in description]
        grouped: dict[GroupKey, dict[str, AggregateAccumulator]] = {}
        for raw in cursor.fetchall():
            row = dict(zip(names, raw, strict=True))
            key: GroupKey = tuple(row[g] for g in group_by)
            accumulators: dict[str, AggregateAccumulator] = {}
            for position, (function, _column, out) in enumerate(specs):
                if function is AggregateFunction.AVG:
                    partial_sum = row[f"p{position}_sum"]
                    partial_count = row[f"p{position}_count"]
                    if not isinstance(partial_count, int):
                        raise RouterError(f"COUNT parcial no entero: {partial_count!r}")
                    if partial_count == 0:
                        continue  # este shard no aporta nada a la media
                    accumulators[out] = AggregateAccumulator(
                        function=function,
                        sum_value=float(_as_number(partial_sum)),
                        count_value=partial_count,
                    )
                    continue
                value = row[f"p{position}"]
                if function is AggregateFunction.COUNT:
                    if not isinstance(value, int):
                        raise RouterError(f"COUNT parcial no entero: {value!r}")
                    accumulators[out] = AggregateAccumulator(function=function, count_value=value)
                elif function is AggregateFunction.SUM:
                    if value is None:
                        continue  # SUM sobre cero filas en este shard
                    accumulators[out] = AggregateAccumulator(
                        function=function, sum_value=float(_as_number(value))
                    )
                elif function is AggregateFunction.MIN:
                    if value is None:
                        continue
                    accumulators[out] = AggregateAccumulator(function=function, min_value=value)
                elif function is AggregateFunction.MAX:
                    if value is None:
                        continue
                    accumulators[out] = AggregateAccumulator(function=function, max_value=value)
            if accumulators:
                grouped[key] = accumulators
        return PartialResult(shard_id=shard_id, node_id=node_id, grouped_aggregates=grouped)

    def execute_write(self, node_id: NodeId, shard_id: ShardId, request: WriteRequest) -> WriteAck:
        payload = json.loads(request.payload.decode("utf-8"))
        connection = self._connection(node_id, shard_id)
        row: dict[str, ColumnValue] = payload["row"]
        columns = list(row.keys())
        connection.execute(_insert_sql(str(payload["table"]), columns, [list(row.values())]))
        return WriteAck(shard_id=shard_id, node_id=node_id)


def _as_number(value: ColumnValue) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise RouterError(f"valor no numérico en un agregado parcial: {value!r}")
    return float(value)


class ShardedNanosql:
    """K bases de datos nanosql independientes tras el router de shards.

    El esquema (DDL) se replica en todos los shards; cada fila vive en el
    shard que decide el hash de su clave (`HashShardingScheme`). Las
    lecturas hacen fan-out a todos los shards y el merge del router
    reconstruye filas o agregados globales — con `is_partial=True` (nunca
    una excepción) si algún shard estaba caído.
    """

    def __init__(self, num_shards: int = 4, *, seed: int = 7) -> None:
        if num_shards < 1:
            raise ProgrammingError(f"num_shards debe ser >= 1, recibido {num_shards}")
        self._scheme = HashShardingScheme(num_shards)
        shard_ids = self._scheme.all_shard_ids()
        self._connections: dict[NodeId, Connection] = {
            f"{shard_id}-node": connect(":memory:", autocommit=True) for shard_id in shard_ids
        }
        directory = ShardDirectory(
            {
                shard_id: ShardReplicaSet(shard_id=shard_id, replicas=(f"{shard_id}-node",))
                for shard_id in shard_ids
            }
        )
        oracle = InMemoryLeadershipOracle({shard_id: f"{shard_id}-node" for shard_id in shard_ids})
        self._executor = _ShardNodeExecutor(self._connections)
        self._router = DistributedRouter(
            directory, oracle, self._scheme, self._executor, rng=random.Random(seed)
        )

    @property
    def shard_ids(self) -> tuple[str, ...]:
        return self._scheme.all_shard_ids()

    def create_table(self, ddl_sql: str) -> None:
        """El DDL se aplica en todos los shards: el esquema es global, sólo
        las filas están particionadas."""
        for connection in self._connections.values():
            connection.execute(ddl_sql)

    def insert(self, table: str, row: dict[str, ColumnValue], *, shard_key: str) -> str:
        key = row.get(shard_key)
        if key is None or isinstance(key, bool) or not isinstance(key, int | str):
            raise ProgrammingError(
                f"la clave de shard {shard_key!r} debe ser un entero o cadena no nulo"
            )
        payload = json.dumps({"table": table, "row": row}, ensure_ascii=False).encode("utf-8")
        try:
            ack = self._router.execute_write(WriteRequest(shard_key=key, payload=payload))
        except RouterError as exc:
            raise OperationalError(f"la escritura shardeada falló: {exc}") from exc
        return ack.shard_id

    def select(
        self, sql: str, *, consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    ) -> MergedResult:
        """Fan-out de un SELECT literal a todos los shards; las filas se
        concatenan sin deduplicar (cada fila vive en exactamente un shard)."""
        try:
            statement = parse(sql)
        except SqlParserError:
            statement = None  # el error lo reportará (bien formateado) cada shard
        if statement is not None and contains_subquery(statement):
            # Cada shard evaluaría la subconsulta SOLO contra sus filas
            # locales: la pertenencia global resultante sería incorrecta en
            # silencio. Mejor un error honesto que un resultado plausible.
            raise NotSupportedError(
                "las subconsultas no están soportadas en modo shardeado: cada shard "
                "las evaluaría únicamente con sus filas locales"
            )
        payload = json.dumps({"kind": "sql", "sql": sql}).encode("utf-8")
        try:
            return self._router.execute_read(ReadRequest(payload=payload, consistency=consistency))
        except RouterError as exc:
            raise OperationalError(f"la lectura shardeada falló: {exc}") from exc

    def aggregate(
        self,
        table: str,
        *,
        group_by: Sequence[str] = (),
        aggregates: Sequence[tuple[AggregateFunction, str | None, str]],
        where: str | None = None,
        consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
    ) -> MergedResult:
        """Agregación distribuida: cada shard calcula estados parciales y el
        merge del router los combina (`AVG` ponderado correctamente)."""
        payload = json.dumps(
            {
                "kind": "aggregate",
                "table": table,
                "group_by": list(group_by),
                "aggregates": [[f.value, col, out] for f, col, out in aggregates],
                "where": where,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        merge_spec = MergeSpec(
            group_by=tuple(group_by),
            aggregates=tuple(
                AggregateSpec(function=f, output_name=out) for f, _col, out in aggregates
            ),
        )
        try:
            return self._router.execute_read(
                ReadRequest(payload=payload, consistency=consistency, merge_spec=merge_spec)
            )
        except RouterError as exc:
            raise OperationalError(f"la agregación shardeada falló: {exc}") from exc

    def fail_shard(self, shard_id: str) -> None:
        self._executor.failed_nodes.add(f"{shard_id}-node")

    def heal_shard(self, shard_id: str) -> None:
        self._executor.failed_nodes.discard(f"{shard_id}-node")

    def shard_row_counts(self, table: str) -> dict[str, int]:
        """Distribución real de filas por shard (para demos y tests)."""
        counts: dict[str, int] = {}
        for node_id, connection in self._connections.items():
            rows = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchall()
            counts[node_id.removesuffix("-node")] = int(rows[0][0] or 0)
        return counts

    def connections(self) -> Iterator[tuple[str, Connection]]:
        yield from self._connections.items()

    def close(self) -> None:
        for connection in self._connections.values():
            connection.close()
