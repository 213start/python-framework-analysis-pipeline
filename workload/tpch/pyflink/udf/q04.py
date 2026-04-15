"""
TPC-H Q4: Order Priority Checking Query

Business question:
    Counts the number of orders ordered in a given quarter for which
    at least one lineitem was received after the committed date, grouped
    by order priority.

SQL semantics:
    SELECT o_orderpriority, count(*) as order_count
    FROM orders
    WHERE o_orderdate >= DATE '1993-07-01'
      AND o_orderdate < DATE '1993-10-01'
      AND EXISTS (SELECT * FROM lineitem
                  WHERE l_orderkey = o_orderkey
                    AND l_commitdate < l_receiptdate)
    GROUP BY o_orderpriority
    ORDER BY o_orderpriority;

UDF mapping:
    - Date range filter on o_orderdate → UDF returns None to discard
    - EXISTS subquery → handled by Flink SQL (native support)
    - GROUP BY / COUNT / ORDER BY → handled by Flink SQL
"""


def udf_q04(orderdate):
    """
    Pure-Python UDF for TPC-H Q4.

    Parameters
    ----------
    orderdate : str
        o_orderdate in 'YYYY-MM-DD' format.

    Returns
    -------
    int or None
        1 if row passes date filter; None otherwise.
    """
    if orderdate < '1993-07-01' or orderdate >= '1993-10-01':
        return None
    return 1


# --------------- Runner metadata ---------------

UDF_INPUTS = ['o_orderdate']

UDF_RESULT_TYPE = 'INT'

SQL = (
    "SELECT o_orderpriority, COUNT(*) AS order_count "
    "FROM orders "
    "WHERE udf_q04(o_orderdate) IS NOT NULL "
    "  AND EXISTS ("
    "    SELECT 1 FROM lineitem "
    "    WHERE l_orderkey = o_orderkey "
    "      AND l_commitdate < l_receiptdate"
    "  ) "
    "GROUP BY o_orderpriority "
    "ORDER BY o_orderpriority"
)
