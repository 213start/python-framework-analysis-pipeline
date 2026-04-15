"""
TPC-H Q3: Shipping Priority Query

Business question:
    Retrieves the shipping priority and potential revenue of orders with
    the highest revenue for customers in the BUILDING market segment.

SQL semantics:
    SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,
           o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING'
      AND c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate < DATE '1995-03-15'
      AND l_shipdate > DATE '1995-03-15'
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate
    LIMIT 10;

UDF mapping:
    - WHERE filters (c_mktsegment, o_orderdate, l_shipdate) → UDF returns None to discard
    - Revenue calculation l_extendedprice*(1-l_discount) → return value
    - GROUP BY / ORDER BY / LIMIT → handled by Flink SQL
"""


def udf_q03(mktsegment, orderdate, shipdate, extendedprice, discount):
    """
    Pure-Python UDF for TPC-H Q3.

    Parameters
    ----------
    mktsegment : str
        c_mktsegment.
    orderdate : str
        o_orderdate in 'YYYY-MM-DD' format.
    shipdate : str
        l_shipdate in 'YYYY-MM-DD' format.
    extendedprice : float
        l_extendedprice.
    discount : float
        l_discount.

    Returns
    -------
    float or None
        Revenue contribution if row passes all filters; None otherwise.
    """
    if mktsegment != 'BUILDING':
        return None
    if orderdate >= '1995-03-15':
        return None
    if shipdate <= '1995-03-15':
        return None
    return float(extendedprice * (1 - discount))


# --------------- Runner metadata ---------------

UDF_INPUTS = [
    'c_mktsegment', 'o_orderdate', 'l_shipdate',
    'l_extendedprice', 'l_discount',
]

UDF_RESULT_TYPE = 'FLOAT'

SQL = (
    "SELECT l_orderkey, "
    "  SUM(udf_q03(c_mktsegment, o_orderdate, l_shipdate, "
    "              l_extendedprice, l_discount)) AS revenue, "
    "  o_orderdate, o_shippriority "
    "FROM customer "
    "JOIN orders ON c_custkey = o_custkey "
    "JOIN lineitem ON l_orderkey = o_orderkey "
    "GROUP BY l_orderkey, o_orderdate, o_shippriority "
    "ORDER BY revenue DESC, o_orderdate "
    "LIMIT 10"
)
