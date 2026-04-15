"""
TPC-H Q18: Large Volume Customer Query

Business question:
    Finds customers who have placed a large volume of orders (total
    quantity > 300) and lists their order details.

SQL semantics:
    SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
           sum(l_quantity)
    FROM customer, orders, lineitem
    WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem
                         GROUP BY l_orderkey
                         HAVING sum(l_quantity) > 300)
      AND c_custkey = o_custkey
      AND o_orderkey = l_orderkey
    GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
    ORDER BY o_totalprice DESC, o_orderdate
    LIMIT 100;

UDF mapping:
    - l_quantity pass-through → UDF returns quantity for SUM aggregation
    - HAVING subquery → handled by Flink SQL
    - GROUP BY / ORDER BY / LIMIT → handled by Flink SQL
"""


def udf_q18(quantity):
    """
    Pure-Python UDF for TPC-H Q18.

    Parameters
    ----------
    quantity : float
        l_quantity.

    Returns
    -------
    float
        Quantity value passed through for SUM aggregation.
    """
    return float(quantity)


# --------------- Runner metadata ---------------

UDF_INPUTS = ['l_quantity']

UDF_RESULT_TYPE = 'FLOAT'

SQL = (
    "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, "
    "  SUM(udf_q18(l_quantity)) AS sum_quantity "
    "FROM customer "
    "JOIN orders ON c_custkey = o_custkey "
    "JOIN lineitem ON o_orderkey = l_orderkey "
    "WHERE o_orderkey IN ("
    "  SELECT l_orderkey FROM lineitem "
    "  GROUP BY l_orderkey "
    "  HAVING SUM(l_quantity) > 300"
    ") "
    "GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice "
    "ORDER BY o_totalprice DESC, o_orderdate "
    "LIMIT 100"
)
