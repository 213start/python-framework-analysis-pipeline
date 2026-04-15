"""
TPC-H Q10: Returned Item Reporting Query

Business question:
    Identifies customers who have returned items and reports the revenue
    loss from those returns for a given quarter.

SQL semantics:
    SELECT c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue,
           c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, orders, lineitem, nation
    WHERE c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate >= DATE '1993-10-01'
      AND o_orderdate < DATE '1994-01-01'
      AND l_returnflag = 'R'
      AND c_nationkey = n_nationkey
    GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name,
             c_address, c_comment
    ORDER BY revenue DESC
    LIMIT 20;

UDF mapping:
    - WHERE filters (o_orderdate range, l_returnflag) → UDF returns None to discard
    - Revenue calculation → return value
    - GROUP BY / ORDER BY / LIMIT → handled by Flink SQL
"""


def udf_q10(orderdate, returnflag, extendedprice, discount):
    """
    Pure-Python UDF for TPC-H Q10.

    Parameters
    ----------
    orderdate : str
        o_orderdate in 'YYYY-MM-DD' format.
    returnflag : str
        l_returnflag.
    extendedprice : float
        l_extendedprice.
    discount : float
        l_discount.

    Returns
    -------
    float or None
        Revenue contribution if row passes all filters; None otherwise.
    """
    if orderdate < '1993-10-01' or orderdate >= '1994-01-01':
        return None
    if returnflag != 'R':
        return None
    return float(extendedprice * (1 - discount))


# --------------- Runner metadata ---------------

UDF_INPUTS = [
    'o_orderdate', 'l_returnflag',
    'l_extendedprice', 'l_discount',
]

UDF_RESULT_TYPE = 'FLOAT'

SQL = (
    "SELECT c_custkey, c_name, "
    "  SUM(udf_q10(o_orderdate, l_returnflag, l_extendedprice, l_discount)) "
    "    AS revenue, "
    "  c_acctbal, n_name, c_address, c_phone, c_comment "
    "FROM customer "
    "JOIN orders ON c_custkey = o_custkey "
    "JOIN lineitem ON l_orderkey = o_orderkey "
    "JOIN nation ON c_nationkey = n_nationkey "
    "GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, "
    "  c_address, c_comment "
    "ORDER BY revenue DESC "
    "LIMIT 20"
)
