"""Generador determinista de datos "TPC-H-like" y sus consultas.

NO es TPC-H oficial ni su `dbgen`: reproduce el esquema (8 tablas con sus
columnas principales), las proporciones de cardinalidad por factor de
escala y la forma de las consultas, con valores pseudoaleatorios de una
semilla fija — el mismo `scale` y `seed` producen byte a byte los mismos
datos. Fechas como texto ISO (`YYYY-MM-DD`), comparables en los tres
motores sin aritmética de fechas, y precios/descuentos como reales.

Las consultas son las de TPC-H que caben en el subconjunto SQL de nanosql
(sin CASE, sin EXISTS ni subconsultas correlacionadas, sin funciones de
fecha), con los parámetros por defecto de la especificación; Q12 se
reescribe sin CASE y por eso se llama `Q12*`.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass

Row = tuple[object, ...]

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
NATIONS = [
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4),
    ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
    ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0), ("MOROCCO", 0),
    ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3), ("SAUDI ARABIA", 4),
    ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1),
]  # fmt: skip
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SHIPMODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
INSTRUCTIONS = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
TYPES = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
CONTAINERS = ["SM CASE", "SM BOX", "MED BAG", "MED BOX", "LG CASE", "LG BOX", "JUMBO PKG"]

SCHEMA = {
    "region": "r_regionkey INTEGER PRIMARY KEY, r_name TEXT NOT NULL",
    "nation": "n_nationkey INTEGER PRIMARY KEY, n_name TEXT NOT NULL, n_regionkey INTEGER NOT NULL",
    "supplier": (
        "s_suppkey INTEGER PRIMARY KEY, s_name TEXT NOT NULL, s_nationkey INTEGER NOT NULL, "
        "s_acctbal FLOAT NOT NULL"
    ),
    "customer": (
        "c_custkey INTEGER PRIMARY KEY, c_name TEXT NOT NULL, c_nationkey INTEGER NOT NULL, "
        "c_acctbal FLOAT NOT NULL, c_mktsegment TEXT NOT NULL"
    ),
    "part": (
        "p_partkey INTEGER PRIMARY KEY, p_name TEXT NOT NULL, p_brand TEXT NOT NULL, "
        "p_type TEXT NOT NULL, p_size INTEGER NOT NULL, p_container TEXT NOT NULL, "
        "p_retailprice FLOAT NOT NULL"
    ),
    "partsupp": (
        "ps_id INTEGER PRIMARY KEY, ps_partkey INTEGER NOT NULL, ps_suppkey INTEGER NOT NULL, "
        "ps_availqty INTEGER NOT NULL, ps_supplycost FLOAT NOT NULL"
    ),
    "orders": (
        "o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER NOT NULL, o_orderstatus TEXT NOT NULL, "
        "o_totalprice FLOAT NOT NULL, o_orderdate TEXT NOT NULL, o_orderpriority TEXT NOT NULL, "
        "o_shippriority INTEGER NOT NULL"
    ),
    "lineitem": (
        "l_id INTEGER PRIMARY KEY, l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, "
        "l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL, l_quantity FLOAT NOT NULL, "
        "l_extendedprice FLOAT NOT NULL, l_discount FLOAT NOT NULL, l_tax FLOAT NOT NULL, "
        "l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipdate TEXT NOT NULL, "
        "l_commitdate TEXT NOT NULL, l_receiptdate TEXT NOT NULL, l_shipinstruct TEXT NOT NULL, "
        "l_shipmode TEXT NOT NULL"
    ),
}

INDEXES = [
    "CREATE INDEX lineitem_orderkey ON lineitem (l_orderkey)",
    "CREATE INDEX orders_custkey ON orders (o_custkey)",
]

_DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def _date(day_number: int) -> str:
    """Día 0 = 1992-01-01; calendario sin bisiestos (determinista y suficiente)."""
    year = 1992 + day_number // 365
    remainder = day_number % 365
    month = 0
    while remainder >= _DAYS_IN_MONTH[month]:
        remainder -= _DAYS_IN_MONTH[month]
        month += 1
    return f"{year:04d}-{month + 1:02d}-{remainder + 1:02d}"


@dataclass(frozen=True)
class Dataset:
    scale: float
    seed: int = 42

    def counts(self) -> dict[str, int]:
        s = self.scale
        return {
            "region": 5,
            "nation": 25,
            "supplier": max(10, int(10_000 * s)),
            "customer": max(150, int(150_000 * s)),
            "part": max(200, int(200_000 * s)),
            "orders": max(1500, int(1_500_000 * s)),
        }

    def tables(self) -> Iterator[tuple[str, list[str], Iterator[Row]]]:
        counts = self.counts()
        rng = random.Random(self.seed)
        yield "region", ["r_regionkey", "r_name"], iter(list(enumerate(REGIONS)))
        yield (
            "nation",
            ["n_nationkey", "n_name", "n_regionkey"],
            iter([(i, name, region) for i, (name, region) in enumerate(NATIONS)]),
        )
        suppliers = counts["supplier"]
        yield (
            "supplier",
            ["s_suppkey", "s_name", "s_nationkey", "s_acctbal"],
            iter(
                [
                    (k, f"Supplier#{k:09d}", rng.randrange(25), round(rng.uniform(-999, 9999), 2))
                    for k in range(1, suppliers + 1)
                ]
            ),
        )
        customers = counts["customer"]
        yield (
            "customer",
            ["c_custkey", "c_name", "c_nationkey", "c_acctbal", "c_mktsegment"],
            iter(
                [
                    (
                        k,
                        f"Customer#{k:09d}",
                        rng.randrange(25),
                        round(rng.uniform(-999, 9999), 2),
                        rng.choice(SEGMENTS),
                    )
                    for k in range(1, customers + 1)
                ]
            ),
        )
        parts = counts["part"]
        part_rows = []
        for k in range(1, parts + 1):
            part_rows.append(
                (
                    k,
                    f"part {k}",
                    f"Brand#{rng.randint(1, 5)}{rng.randint(1, 5)}",
                    f"{rng.choice(TYPES)} {rng.choice(['ANODIZED', 'BRUSHED', 'PLATED'])} "
                    f"{rng.choice(['BRASS', 'COPPER', 'STEEL', 'TIN'])}",
                    rng.randint(1, 50),
                    rng.choice(CONTAINERS),
                    round(900 + (k % 200) + rng.random() * 100, 2),
                )
            )
        yield (
            "part",
            ["p_partkey", "p_name", "p_brand", "p_type", "p_size", "p_container", "p_retailprice"],
            iter(part_rows),
        )
        partsupp = []
        ps_id = 1
        for k in range(1, parts + 1):
            for j in range(4):
                partsupp.append(
                    (
                        ps_id,
                        k,
                        (k + j * (suppliers // 4 + 1)) % suppliers + 1,
                        rng.randint(1, 9999),
                        round(rng.uniform(1, 1000), 2),
                    )
                )
                ps_id += 1
        yield (
            "partsupp",
            ["ps_id", "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"],
            iter(partsupp),
        )
        orders: list[Row] = []
        lineitems: list[Row] = []
        line_id = 1
        for k in range(1, counts["orders"] + 1):
            orderdate = rng.randrange(0, 365 * 7 - 151)
            lines = rng.randint(1, 7)
            total = 0.0
            statuses = set()
            for number in range(1, lines + 1):
                quantity = float(rng.randint(1, 50))
                partkey = rng.randint(1, parts)
                price = round(quantity * (900 + (partkey % 200)) / 10, 2)
                discount = rng.randint(0, 10) / 100
                tax = rng.randint(0, 8) / 100
                ship = orderdate + rng.randint(1, 121)
                commit = orderdate + rng.randint(30, 90)
                receipt = ship + rng.randint(1, 30)
                status = "F" if ship <= 365 * 6 + 167 else "O"
                statuses.add(status)
                flag = rng.choice(["R", "A"]) if receipt <= 365 * 3 + 168 else "N"
                total += price * (1 - discount) * (1 + tax)
                lineitems.append(
                    (
                        line_id,
                        k,
                        partkey,
                        rng.randint(1, suppliers),
                        number,
                        quantity,
                        price,
                        discount,
                        tax,
                        flag,
                        status,
                        _date(ship),
                        _date(commit),
                        _date(receipt),
                        rng.choice(INSTRUCTIONS),
                        rng.choice(SHIPMODES),
                    )
                )
                line_id += 1
            order_status = "F" if statuses == {"F"} else ("O" if statuses == {"O"} else "P")
            orders.append(
                (
                    k,
                    rng.randint(1, customers),
                    order_status,
                    round(total, 2),
                    _date(orderdate),
                    rng.choice(PRIORITIES),
                    0,
                )
            )
        yield (
            "orders",
            [
                "o_orderkey",
                "o_custkey",
                "o_orderstatus",
                "o_totalprice",
                "o_orderdate",
                "o_orderpriority",
                "o_shippriority",
            ],
            iter(orders),
        )
        yield (
            "lineitem",
            [
                "l_id",
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_linenumber",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_returnflag",
                "l_linestatus",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "l_shipinstruct",
                "l_shipmode",
            ],
            iter(lineitems),
        )


QUERIES: dict[str, str] = {
    "Q1": """
        SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty,
               SUM(l_extendedprice) AS sum_base_price,
               SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
               SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
               AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price,
               AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= '1998-09-02'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus""",
    "Q3": """
        SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               o_orderdate, o_shippriority
        FROM customer JOIN orders ON c_custkey = o_custkey
             JOIN lineitem ON l_orderkey = o_orderkey
        WHERE c_mktsegment = 'BUILDING' AND o_orderdate < '1995-03-15'
          AND l_shipdate > '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10""",
    "Q5": """
        SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer JOIN orders ON c_custkey = o_custkey
             JOIN lineitem ON l_orderkey = o_orderkey
             JOIN supplier ON l_suppkey = s_suppkey
             JOIN nation ON s_nationkey = n_nationkey
             JOIN region ON n_regionkey = r_regionkey
        WHERE c_nationkey = s_nationkey AND r_name = 'ASIA'
          AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC""",
    "Q6": """
        SELECT SUM(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24""",
    "Q10": """
        SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               c_acctbal, n_name
        FROM customer JOIN orders ON c_custkey = o_custkey
             JOIN lineitem ON l_orderkey = o_orderkey
             JOIN nation ON c_nationkey = n_nationkey
        WHERE o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'
          AND l_returnflag = 'R'
        GROUP BY c_custkey, c_name, c_acctbal, n_name
        ORDER BY revenue DESC
        LIMIT 20""",
    "Q12*": """
        SELECT l_shipmode, COUNT(*) AS line_count
        FROM orders JOIN lineitem ON o_orderkey = l_orderkey
        WHERE l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate
          AND l_shipdate < l_commitdate AND l_receiptdate >= '1994-01-01'
          AND l_receiptdate < '1995-01-01'
          AND o_orderpriority IN ('1-URGENT', '2-HIGH')
        GROUP BY l_shipmode
        ORDER BY l_shipmode""",
    "Q18": """
        SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
               SUM(l_quantity) AS total_qty
        FROM customer JOIN orders ON c_custkey = o_custkey
             JOIN lineitem ON o_orderkey = l_orderkey
        WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey
                             HAVING SUM(l_quantity) > 250)
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate
        LIMIT 100""",
    "point": "SELECT o_totalprice, o_orderdate FROM orders WHERE o_orderkey = 777",
}
