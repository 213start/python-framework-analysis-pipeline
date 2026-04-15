"""Step 4: generate framework-specific test cases.

This step owns workload materialization, for example rewriting TPC-H SQL into
framework-native Python UDF cases without losing business logic.

Workload definitions live in ``workload/`` at the repository root, organized by
benchmark family and target framework:

    workload/tpch/sql/      — 22 original TPC-H SQL queries (framework-agnostic)
    workload/tpch/pyflink/  — PyFlink UDF implementations + runner
    workload/tpch/pyspark/  — (future) PySpark implementations

Each UDF module exports a pure-Python function and metadata (UDF_INPUTS,
UDF_RESULT_TYPE, SQL) so that a framework-specific runner can register the
function and execute the query against a live cluster.

This pipeline step currently serves as a documentation anchor.  Future work
may add CLI subcommands for listing available cases, validating UDF metadata,
or scaffolding new cases from SQL.
"""
