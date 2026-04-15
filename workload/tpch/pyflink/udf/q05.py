"""
TPC-H Q5: Local Supplier Volume Query

Business question:
    Lists the revenue volume by supplier nation for suppliers in the ASIA
    region for the year 1994.

SQL semantics:
    SELECT n_name, sum(l_extendedprice*(1-l_discount)) as revenue
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND l_suppkey = s_suppkey
      AND c_nationkey = s_nationkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'ASIA'
      AND o_orderdate >= DATE '1994-01-01'
      AND o_orderdate < DATE '1995-01-01'
    GROUP BY n_name
    ORDER BY revenue DESC;

UDF mapping:
    - WHERE filters (r_name, o_orderdate range) → UDF returns None to discard
    - Revenue calculation → return value
    - GROUP BY / ORDER BY → handled by Flink SQL
"""


def udf_q05(r_name, orderdate, extendedprice, discount):
    """
    Pure-Python UDF for TPC-H Q5.

    Parameters
    ----------
    r_name : str
        r_name.
    orderdate : str
        o_orderdate in 'YYYY-MM-DD' format.
    extendedprice : float
        l_extendedprice.
    discount : float
        l_discount.

    Returns
    -------
    float or None
        Revenue contribution if row passes all filters; None otherwise.
    """
    if r_name != 'ASIA':
        return None
    if orderdate < '1994-01-01' or orderdate >= '1995-01-01':
        return None
    return float(extendedprice * (1 - discount))


# --------------- Runner metadata ---------------

UDF_INPUTS = [
    'r_name', 'o_orderdate',
    'l_extendedprice', 'l_discount',
]

UDF_RESULT_TYPE = 'FLOAT'

SQL = (
    "SELECT n_name, "
    "  SUM(udf_q05(r_name, o_orderdate, l_extendedprice, l_discount)) "
    "    AS revenue "
    "FROM customer "
    "JOIN orders ON c_custkey = o_custkey "
    "JOIN lineitem ON l_orderkey = o_orderkey "
    "JOIN supplier ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey "
    "JOIN nation ON s_nationkey = n_nationkey "
    "JOIN region ON n_regionkey = r_regionkey "
    "GROUP BY n_name "
    "ORDER BY revenue DESC"
)
