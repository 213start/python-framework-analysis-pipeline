"""
TPC-H Q12: Shipping Modes and Order Priority Query

Business question:
    Determines whether selecting less expensive shipping modes leads to
    a loss of orders among urgent orders.

SQL semantics:
    SELECT l_shipmode,
           SUM(CASE WHEN o_orderpriority IN ('1-URGENT','2-HIGH')
                    THEN 1 ELSE 0 END) AS high_line_count,
           SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT','2-HIGH')
                    THEN 1 ELSE 0 END) AS low_line_count
    FROM orders, lineitem
    WHERE o_orderkey = l_orderkey
      AND l_shipmode IN ('MAIL', 'SHIP')
      AND l_commitdate < l_receiptdate
      AND l_shipdate < l_commitdate
      AND l_receiptdate >= DATE '1994-01-01'
      AND l_receiptdate <  DATE '1995-01-01'
    GROUP BY l_shipmode
    ORDER BY l_shipmode;

UDF mapping:
    - WHERE filters on lineitem columns → handled by Flink SQL in the query
    - JOIN condition (o_orderkey = l_orderkey) → handled by Flink SQL
    - CASE WHEN logic → UDF returns (high_line, low_line) per row
    - GROUP BY / SUM → handled by Flink SQL
"""


def udf_q12(orderpriority):
    """
    Pure-Python UDF for TPC-H Q12.

    Parameters
    ----------
    orderpriority : str
        o_orderpriority.

    Returns
    -------
    tuple[int, int]
        (high_line_count, low_line_count) — exactly one element is 1 and
        the other is 0, for the caller to SUM across rows.
    """
    if orderpriority in ('1-URGENT', '2-HIGH'):
        return (1, 0)
    else:
        return (0, 1)


# --------------- Runner metadata ---------------

UDF_INPUTS = ['o_orderpriority']

UDF_RESULT_TYPE = 'ROW<`high_line` INT, `low_line` INT>'

SQL = (
    "SELECT l_shipmode, "
    "  SUM(udf_q12(o_orderpriority).high_line) AS high_line_count, "
    "  SUM(udf_q12(o_orderpriority).low_line)  AS low_line_count "
    "FROM orders "
    "JOIN lineitem ON o_orderkey = l_orderkey "
    "WHERE l_shipmode IN ('MAIL', 'SHIP') "
    "  AND l_commitdate < l_receiptdate "
    "  AND l_shipdate  < l_commitdate "
    "  AND l_receiptdate >= DATE '1994-01-01' "
    "  AND l_receiptdate <  DATE '1995-01-01' "
    "GROUP BY l_shipmode "
    "ORDER BY l_shipmode"
)
