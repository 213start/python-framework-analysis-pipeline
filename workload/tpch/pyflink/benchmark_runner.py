"""TPC-H PyFlink UDF Benchmark Runner — measure framework overhead.

Operator chain:
    datagen → MarkStart (Java, record java_start_time)
            → timed_qXX (Python UDTF, wrap UDF + measure py_duration)
            → CalcOverhead (Java, record java_end_time, compute overhead)
            → blackhole

framework_overhead = java_end_time - java_start_time - py_duration

Usage:
    python benchmark_runner.py --query q06 [--cluster host:port] [--rows 10M]
"""

import argparse
import importlib
import json
import os
import sys
import tempfile
import time

# ---------------------------------------------------------------------------
# Column type mapping — TPC-H column name → Flink SQL type for datagen
# ---------------------------------------------------------------------------

COLUMN_TYPES = {
    # lineitem
    "l_shipdate": "STRING", "l_commitdate": "STRING", "l_receiptdate": "STRING",
    "l_quantity": "DOUBLE", "l_extendedprice": "DOUBLE",
    "l_discount": "DOUBLE", "l_tax": "DOUBLE",
    "l_returnflag": "STRING", "l_linestatus": "STRING",
    "l_shipinstruct": "STRING", "l_shipmode": "STRING", "l_comment": "STRING",
    # orders
    "o_orderdate": "STRING", "o_orderpriority": "STRING", "o_orderstatus": "STRING",
    "o_orderkey": "BIGINT", "o_custkey": "BIGINT",
    # customer
    "c_mktsegment": "STRING", "c_phone": "STRING", "c_acctbal": "DOUBLE",
    "c_comment": "STRING",
    # part
    "p_name": "STRING", "p_type": "STRING", "p_brand": "STRING",
    "p_container": "STRING", "p_size": "BIGINT",
    # supplier / partsupp
    "ps_supplycost": "DOUBLE",
    # nation / region
    "r_name": "STRING", "n_name": "STRING",
}

# ---------------------------------------------------------------------------
# Type parsing (reused from runner.py)
# ---------------------------------------------------------------------------

def _parse_type(s):
    """Parse a Flink SQL type string to PyFlink DataType."""
    from pyflink.table import DataTypes
    s = s.strip()
    if s == "FLOAT":
        return DataTypes.FLOAT()
    if s == "INT":
        return DataTypes.INT()
    if s == "BIGINT":
        return DataTypes.BIGINT()
    if s == "STRING":
        return DataTypes.STRING()
    if s == "DOUBLE":
        return DataTypes.DOUBLE()
    if s.startswith("DECIMAL"):
        inner = s[s.index("(") + 1 : s.index(")")]
        p, sc = inner.split(",")
        return DataTypes.DECIMAL(int(p), int(sc))
    if s.startswith("ROW<"):
        inner = s[4:-1]
        fields = []
        for part in _split_row_fields(inner):
            name, ftype = part.strip().split(" ", 1)
            name = name.strip("`")
            fields.append(DataTypes.FIELD(name, _parse_type(ftype)))
        return DataTypes.ROW(*fields)
    raise ValueError(f"Unsupported type: {s}")


def _split_row_fields(s):
    parts, depth, current = [], 0, []
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


def _flatten_result_types(type_str):
    """Parse UDF_RESULT_TYPE → flat list of DataTypes (for UDTF result_types)."""
    from pyflink.table import DataTypes
    if type_str.startswith("ROW<"):
        inner = type_str[4:-1]
        fields = []
        for part in _split_row_fields(inner):
            _, ftype = part.strip().split(" ", 1)
            fields.append(_parse_type(ftype.strip()))
        return fields
    return [_parse_type(type_str)]


def _get_result_field_names(type_str):
    """Get field names from UDF_RESULT_TYPE for SQL alias generation."""
    if type_str.startswith("ROW<"):
        inner = type_str[4:-1]
        return [p.strip().split(" ")[0].strip("`") for p in _split_row_fields(inner)]
    return ["result"]


# ---------------------------------------------------------------------------
# TimedUDTF factory — wraps any UDF with timing, no UDF changes needed
# ---------------------------------------------------------------------------

def make_timed_udtf(udf_func, n_result_fields):
    """Create a generator function suitable for PyFlink udtf() registration.

    The returned function has signature: (java_start_time, *udf_args)
    and yields: (*result_fields, java_start_time, py_duration_ns)
    """

    def _timed_eval(java_start_time, *udf_args):
        t0 = time.perf_counter_ns()
        result = udf_func(*udf_args)
        t1 = time.perf_counter_ns()
        py_duration = t1 - t0
        if result is None:
            yield (None,) * n_result_fields + (java_start_time, py_duration)
        elif n_result_fields == 1:
            yield (result, java_start_time, py_duration)
        else:
            yield (*result, java_start_time, py_duration)

    return _timed_eval


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------

def generate_source_ddl(query_id, inputs, num_rows, rows_per_second):
    """Generate datagen source DDL with _dummy DOUBLE column for PreUDF."""
    col_defs = ["    _dummy DOUBLE"]
    datagen_opts = [
        f"    'connector' = 'datagen'",
        f"    'number-of-rows' = '{num_rows}'",
        f"    'rows-per-second' = '{rows_per_second}'",
        "    'fields._dummy.min' = '0.0'",
        "    'fields._dummy.max' = '1.0'",
    ]
    for col in inputs:
        col_type = COLUMN_TYPES.get(col, "STRING")
        col_defs.append(f"    {col} {col_type}")
        if col_type == "DOUBLE":
            datagen_opts.append(f"    'fields.{col}.min' = '1.0'")
            datagen_opts.append(f"    'fields.{col}.max' = '100000.0'")
        elif col_type == "BIGINT":
            datagen_opts.append(f"    'fields.{col}.min' = '1'")
            datagen_opts.append(f"    'fields.{col}.max' = '100000'")
        elif col_type == "STRING":
            datagen_opts.append(f"    'fields.{col}.length' = '10'")

    ddl = (
        f"CREATE TABLE source_{query_id} (\n"
        + ",\n".join(col_defs)
        + "\n) WITH (\n"
        + ",\n".join(datagen_opts)
        + "\n)"
    )
    return ddl


SINK_DDL = """CREATE TABLE sink (
    overhead BIGINT
) WITH (
    'connector' = 'blackhole'
)"""


def generate_benchmark_sql(query_id, inputs, result_type_str):
    """Generate the INSERT-SELECT chain: MarkStart → timed UDTF → CalcOverhead."""
    result_names = _get_result_field_names(result_type_str)
    all_alias = result_names + ["java_start_time", "py_duration"]
    alias_str = ", ".join(f"`{a}`" for a in all_alias)

    # UDTF args: marked._m.javaStartTime, then all source columns
    udtf_args = ["marked._m.javaStartTime"]
    for col in inputs:
        udtf_args.append(f"marked.{col}")
    udtf_args_str = ", ".join(udtf_args)

    sql = f"""INSERT INTO sink
    SELECT CalcOverhead(`java_start_time`, `py_duration`)
    FROM (
        WITH marked AS (
            SELECT *, MarkStart(_dummy) AS _m
            FROM source_{query_id}
        ),
        processed AS (
            SELECT TPY.*
            FROM marked,
            LATERAL TABLE(timed_{query_id}({udtf_args_str}))
            AS TPY({alias_str})
        )
        SELECT `java_start_time`, `py_duration`
        FROM processed
    )"""
    return sql


# ---------------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------------

def _create_remote_env(host, port, jar_path):
    """Create a remote StreamExecutionEnvironment via Java constructor.

    PyFlink 2.x removed create_remote_environment from the Python API,
    so we call RemoteStreamEnvironment's Java constructor directly via Py4J.
    """
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.java_gateway import get_gateway

    gateway = get_gateway()
    jvm = gateway.jvm
    jars = gateway.new_array(jvm.java.lang.String, 1)
    jars[0] = jar_path
    j_env = jvm.org.apache.flink.streaming.api.environment\
        .RemoteStreamEnvironment(host, port, jars)
    return StreamExecutionEnvironment(j_env)


def setup_env(args):
    """Create and configure the Flink execution environment."""
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment

    jar_uri = f"file://{os.path.abspath(args.jar)}"

    if args.cluster:
        host, port = args.cluster.split(":")
        env = _create_remote_env(host, int(port), os.path.abspath(args.jar))
    else:
        env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(args.parallelism)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    cfg = t_env.get_config().get_configuration()
    cfg.set_string("python.executable", sys.executable)
    cfg.set_string("python.client.executable", sys.executable)
    cfg.set_string("pipeline.jars", jar_uri)

    # Performance tuning
    t_env.get_config().set("parallelism.default", str(args.parallelism))
    t_env.get_config().set("python.fn-execution.arrow.batch.size", str(args.arrow_batch_size))
    t_env.get_config().set("python.fn-execution.bundle.size", str(args.bundle_size))
    t_env.get_config().set("python.fn-execution.bundle.time", str(args.bundle_time))
    t_env.get_config().set("python.execution-mode", args.execution_mode)

    return env, t_env


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_rows(s):
    """Parse row count with K/M/B suffixes: 10M → 10000000."""
    multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    if s[-1].upper() in multipliers:
        return int(float(s[:-1]) * multipliers[s[-1].upper()])
    return int(s)


def main():
    parser = argparse.ArgumentParser(
        description="TPC-H PyFlink UDF Benchmark — measure framework overhead"
    )
    parser.add_argument("--query", required=True, help="Query ID (q01,q03-q06,q09,q10,q12-q14,q18,q19,q22)")
    parser.add_argument("--cluster", help="Flink JM host:port. Omit for local mini-cluster.")
    parser.add_argument("--jar", default=None,
                        help="Path to JAR with MarkStart/CalcOverhead. "
                             "Default: java-udf/FlinkDemo-1.0-SNAPSHOT.jar next to this script")
    parser.add_argument("--rows", default="10M", help="Number of rows (supports K/M/B suffix)")
    parser.add_argument("--rows-per-second", type=int, default=100_000_000)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--arrow-batch-size", type=int, default=10000)
    parser.add_argument("--bundle-size", type=int, default=10000)
    parser.add_argument("--bundle-time", type=int, default=1000, help="Bundle time in ms")
    parser.add_argument("--execution-mode", choices=["process", "thread"], default="process")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument("--build-jar", action="store_true",
                        help="Build the Java UDF JAR before running (requires javac and FLINK_HOME)")
    args = parser.parse_args()

    # Resolve JAR path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if args.jar is None:
        args.jar = os.path.join(script_dir, "java-udf", "FlinkDemo-1.0-SNAPSHOT.jar")

    if args.build_jar:
        build_script = os.path.join(script_dir, "java-udf", "build.sh")
        if not os.path.isfile(build_script):
            print(f"ERROR: build script not found: {build_script}", file=sys.stderr)
            sys.exit(1)
        print(f"Building JAR: {build_script}")
        ret = os.system(f"bash {build_script}")
        if ret != 0:
            print(f"ERROR: JAR build failed (exit {ret >> 8})", file=sys.stderr)
            sys.exit(1)

    num_rows = parse_rows(args.rows)
    query_id = args.query

    # Load UDF module
    udf_dir = os.path.join(os.path.dirname(__file__), "udf")
    if udf_dir not in sys.path:
        sys.path.insert(0, udf_dir)

    mod = importlib.import_module(query_id)
    udf_func = getattr(mod, f"udf_{query_id}")
    inputs = getattr(mod, "UDF_INPUTS")
    result_type_str = getattr(mod, "UDF_RESULT_TYPE")

    # Compute timing metadata (string-only, no PyFlink dependency)
    result_names = _get_result_field_names(result_type_str)
    n_result_fields = len(result_names)

    # Generate SQL
    source_ddl = generate_source_ddl(query_id, inputs, num_rows, args.rows_per_second)
    benchmark_sql = generate_benchmark_sql(query_id, inputs, result_type_str)

    if args.dry_run:
        print("=" * 60)
        print(f"-- Source DDL for {query_id}")
        print("=" * 60)
        print(source_ddl)
        print()
        print("=" * 60)
        print("-- Sink DDL")
        print("=" * 60)
        print(SINK_DDL)
        print()
        print("=" * 60)
        print(f"-- Benchmark SQL for {query_id}")
        print(f"-- UDF inputs: {inputs}")
        print(f"-- UDF result: {result_type_str}")
        print(f"-- UDTF result_types: {result_names + ['java_start_time', 'py_duration']} (all BIGINT for timing)")
        print(f"-- Rows: {num_rows}")
        print("=" * 60)
        print(benchmark_sql)
        return

    # PyFlink imports — only needed for actual execution
    result_fields = _flatten_result_types(result_type_str)
    n_result_fields = len(result_fields)
    from pyflink.table import DataTypes
    udtf_result_types = result_fields + [DataTypes.BIGINT(), DataTypes.BIGINT()]

    # Setup environment
    env, t_env = setup_env(args)

    # Register source and sink
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(SINK_DDL)

    # Register Java UDFs
    t_env.create_java_temporary_function("MarkStart", "com.huawei.PreUDF")
    t_env.create_java_temporary_function("CalcOverhead", "com.huawei.PostUDF")

    # Create and register timed Python UDTF
    from pyflink.table.udf import udtf as register_udtf

    timed_func = make_timed_udtf(udf_func, n_result_fields)
    t_env.create_temporary_function(
        f"timed_{query_id}",
        register_udtf(timed_func, result_types=udtf_result_types),
    )

    # Distribute UDF file to TaskManagers
    udf_file = os.path.join(udf_dir, f"{query_id}.py")
    env.add_python_file(udf_file)
    # Also add __init__.py if it exists, for package imports
    init_file = os.path.join(udf_dir, "__init__.py")
    if os.path.exists(init_file):
        env.add_python_file(init_file)

    # Execute benchmark
    print(f"Starting benchmark: {query_id}")
    print(f"  rows: {num_rows}")
    print(f"  parallelism: {args.parallelism}")
    print(f"  bundle_size: {args.bundle_size}")
    print(f"  execution_mode: {args.execution_mode}")

    start = time.time()
    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(benchmark_sql)
    table_result = statement_set.execute()

    # Wait for completion
    job_client = table_result.get_job_client()
    if job_client:
        print(f"  Job ID: {job_client.get_job_id()}")
        job_client.get_job_execution_result().result()
    else:
        print("  Warning: no job client, waiting 30s...")
        time.sleep(30)

    elapsed = time.time() - start
    print(f"  Total time: {elapsed:.2f}s")
    print(f"  Throughput: {num_rows / elapsed:.0f} rows/s")

    # Structured output for pipeline consumption.
    result_data = {
        "type": "BENCHMARK_RESULT",
        "queryId": query_id,
        "rows": num_rows,
        "wallClockSeconds": round(elapsed, 3),
        "throughputRowsPerSec": round(num_rows / elapsed, 1),
    }

    print(json.dumps(result_data))


if __name__ == "__main__":
    main()
