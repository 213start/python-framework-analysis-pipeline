"""
TPC-H public runner — attach to a Flink cluster, register UDF, execute query.

Usage:
    python runner.py --query q06 --data /data/tpch/sf10 [--cluster host:port]

If --cluster is omitted, runs in local mini-cluster mode.
"""

import argparse
import importlib
import os
import sys

# ---------------------------------------------------------------------------
# TPC-H table DDLs — pipe-delimited .tbl files produced by dbgen
# ---------------------------------------------------------------------------

TPCH_DDL = {
    "nation": (
        "CREATE TABLE IF NOT EXISTS nation ("
        "  N_NATIONKEY  BIGINT,"
        "  N_NAME       STRING,"
        "  N_REGIONKEY  BIGINT,"
        "  N_COMMENT    STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/nation.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
    "region": (
        "CREATE TABLE IF NOT EXISTS region ("
        "  R_REGIONKEY  BIGINT,"
        "  R_NAME       STRING,"
        "  R_COMMENT    STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/region.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
    "part": (
        "CREATE TABLE IF NOT EXISTS part ("
        "  P_PARTKEY     BIGINT,"
        "  P_NAME        STRING,"
        "  P_MFGR        STRING,"
        "  P_BRAND       STRING,"
        "  P_TYPE        STRING,"
        "  P_SIZE        BIGINT,"
        "  P_CONTAINER   STRING,"
        "  P_RETAILPRICE DECIMAL(15, 2),"
        "  P_COMMENT     STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/part.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
    "supplier": (
        "CREATE TABLE IF NOT EXISTS supplier ("
        "  S_SUPPKEY   BIGINT,"
        "  S_NAME      STRING,"
        "  S_ADDRESS   STRING,"
        "  S_NATIONKEY BIGINT,"
        "  S_PHONE     STRING,"
        "  S_ACCTBAL   DECIMAL(15, 2),"
        "  S_COMMENT   STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/supplier.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
    "partsupp": (
        "CREATE TABLE IF NOT EXISTS partsupp ("
        "  PS_PARTKEY    BIGINT,"
        "  PS_SUPPKEY    BIGINT,"
        "  PS_AVAILQTY   BIGINT,"
        "  PS_SUPPLYCOST DECIMAL(15, 2),"
        "  PS_COMMENT    STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/partsupp.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
    "customer": (
        "CREATE TABLE IF NOT EXISTS customer ("
        "  C_CUSTKEY    BIGINT,"
        "  C_NAME       STRING,"
        "  C_ADDRESS    STRING,"
        "  C_NATIONKEY  BIGINT,"
        "  C_PHONE      STRING,"
        "  C_ACCTBAL    DECIMAL(15, 2),"
        "  C_MKTSEGMENT STRING,"
        "  C_COMMENT    STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/customer.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
    "orders": (
        "CREATE TABLE IF NOT EXISTS orders ("
        "  O_ORDERKEY      BIGINT,"
        "  O_CUSTKEY       BIGINT,"
        "  O_ORDERSTATUS   STRING,"
        "  O_TOTALPRICE    DECIMAL(15, 2),"
        "  O_ORDERDATE     STRING,"
        "  O_ORDERPRIORITY STRING,"
        "  O_CLERK         STRING,"
        "  O_SHIPPRIORITY  BIGINT,"
        "  O_COMMENT       STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/orders.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
    "lineitem": (
        "CREATE TABLE IF NOT EXISTS lineitem ("
        "  L_ORDERKEY      BIGINT,"
        "  L_PARTKEY       BIGINT,"
        "  L_SUPPKEY       BIGINT,"
        "  L_LINENUMBER    BIGINT,"
        "  L_QUANTITY      DECIMAL(15, 2),"
        "  L_EXTENDEDPRICE DECIMAL(15, 2),"
        "  L_DISCOUNT      DECIMAL(15, 2),"
        "  L_TAX           DECIMAL(15, 2),"
        "  L_RETURNFLAG    STRING,"
        "  L_LINESTATUS    STRING,"
        "  L_SHIPDATE      STRING,"
        "  L_COMMITDATE    STRING,"
        "  L_RECEIPTDATE   STRING,"
        "  L_SHIPINSTRUCT  STRING,"
        "  L_SHIPMODE      STRING,"
        "  L_COMMENT       STRING"
        ") WITH ("
        "  'connector' = 'filesystem',"
        "  'path' = '{data}/lineitem.tbl',"
        "  'format' = 'csv',"
        "  'csv.field-delimiter' = '|',"
        "  'csv.ignore-parse-errors' = 'true'"
        ")"
    ),
}

# Tables required per query (to avoid registering unnecessary tables)
QUERY_TABLES = {
    "q01": ["lineitem"],
    "q03": ["customer", "orders", "lineitem"],
    "q04": ["orders", "lineitem"],
    "q05": ["customer", "orders", "lineitem", "supplier", "nation", "region"],
    "q06": ["lineitem"],
    "q09": ["part", "supplier", "lineitem", "partsupp", "orders", "nation"],
    "q10": ["customer", "orders", "lineitem", "nation"],
    "q12": ["orders", "lineitem"],
    "q13": ["customer", "orders"],
    "q14": ["lineitem", "part"],
    "q18": ["customer", "orders", "lineitem"],
    "q19": ["lineitem", "part"],
    "q22": ["customer", "orders"],
}


def main():
    parser = argparse.ArgumentParser(
        description="TPC-H PyFlink UDF Runner — attach to a Flink cluster"
    )
    parser.add_argument(
        "--query", required=True,
        help="Query ID, e.g. q01, q06, q12",
    )
    parser.add_argument(
        "--data", required=True,
        help="Path to the TPC-H .tbl data directory",
    )
    parser.add_argument(
        "--cluster",
        help="Flink JobManager address (host:port). "
             "If omitted, runs in local mini-cluster mode.",
    )
    args = parser.parse_args()

    # Lazy-import PyFlink so the UDF modules themselves stay pure-Python.
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment, DataTypes
    from pyflink.table.udf import udf as register_udf

    # 1. Create execution environment
    if args.cluster:
        host, port = args.cluster.split(":")
        env = StreamExecutionEnvironment.create_remote_environment(
            host, int(port)
        )
    else:
        env = StreamExecutionEnvironment.get_execution_environment()

    t_env = StreamTableEnvironment.create(env)

    # 2. Register only the tables needed for this query
    needed = QUERY_TABLES.get(args.query, list(TPCH_DDL.keys()))
    data_dir = os.path.abspath(args.data)
    for table_name in needed:
        ddl = TPCH_DDL[table_name].format(data=data_dir)
        t_env.execute_sql(ddl)

    # 3. Load the UDF module (pure Python, no PyFlink imports)
    udf_dir = os.path.join(os.path.dirname(__file__), "udf")
    if udf_dir not in sys.path:
        sys.path.insert(0, udf_dir)

    mod = importlib.import_module(args.query)

    udf_func = getattr(mod, "udf_" + args.query)
    result_type_str = getattr(mod, "UDF_RESULT_TYPE")

    # Parse result type string to Flink DataType
    result_type = _parse_type(result_type_str)

    # Register UDF
    func_name = f"udf_{args.query}"
    t_env.create_temporary_function(
        func_name,
        register_udf(result_type=result_type, func=udf_func),
    )

    # 4. Execute the query
    sql = getattr(mod, "SQL")
    print(f"Executing: {args.query}")
    print(f"SQL: {sql}")
    result = t_env.sql_query(sql)

    # 5. Collect and print results
    result.execute().print()


def _parse_type(type_str):
    """Parse a simple Flink SQL type string to a PyFlink DataType.

    Supports: FLOAT, INT, BIGINT, STRING, DECIMAL(p,s),
              ROW<`name` TYPE, ...>
    """
    from pyflink.table import DataTypes

    s = type_str.strip()

    if s == "FLOAT":
        return DataTypes.FLOAT()
    if s == "INT":
        return DataTypes.INT()
    if s == "BIGINT":
        return DataTypes.BIGINT()
    if s == "STRING":
        return DataTypes.STRING()
    if s.startswith("DECIMAL"):
        inner = s[s.index("(") + 1 : s.index(")")]
        p, sc = inner.split(",")
        return DataTypes.DECIMAL(int(p), int(sc))

    if s.startswith("ROW<"):
        inner = s[4:-1]  # strip ROW< ... >
        fields = []
        # Simple split on comma — doesn't handle nested ROWs
        for part in _split_row_fields(inner):
            name, ftype = part.strip().split(" ", 1)
            name = name.strip("`")
            fields.append(DataTypes.FIELD(name, _parse_type(ftype)))
        return DataTypes.ROW(*fields)

    raise ValueError(f"Unsupported type: {s}")


def _split_row_fields(s):
    """Split ROW field definitions, respecting potential nesting."""
    parts = []
    depth = 0
    current = []
    for ch in s:
        if ch == "<":
            depth += 1
            current.append(ch)
        elif ch == ">":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


if __name__ == "__main__":
    main()
