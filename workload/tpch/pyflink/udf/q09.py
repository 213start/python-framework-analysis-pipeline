"""
TPC-H Q9: Product Type Profit Measure Query

Business question:
    Determines the profit for each nation by year for parts that contain
    a given string in their name.

SQL semantics:
    SELECT nation, o_year, sum(amount) as sum_profit
    FROM (SELECT n_name as nation,
                 EXTRACT(YEAR FROM o_orderdate) as o_year,
                 l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity as amount
          FROM part, supplier, lineitem, partsupp, orders, nation
          WHERE s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%') as profit
    GROUP BY nation, o_year
    ORDER BY nation, o_year DESC;

UDF mapping:
    - p_name LIKE '%green%' filter → UDF returns None to discard
    - Profit calculation → UDF returns amount
    - EXTRACT / GROUP BY / ORDER BY → handled by Flink SQL
"""


def udf_q09(p_name, extendedprice, discount, supplycost, quantity):
    """
    Pure-Python UDF for TPC-H Q9.

    Parameters
    ----------
    p_name : str
        p_name.
    extendedprice : float
        l_extendedprice.
    discount : float
        l_discount.
    supplycost : float
        ps_supplycost.
    quantity : float
        l_quantity.

    Returns
    -------
    float or None
        Profit amount if p_name contains 'green'; None otherwise.
    """
    if 'green' not in p_name.lower():
        return None
    return float(extendedprice * (1 - discount) - supplycost * quantity)


# --------------- Runner metadata ---------------

UDF_INPUTS = [
    'p_name', 'l_extendedprice', 'l_discount',
    'ps_supplycost', 'l_quantity',
]

UDF_RESULT_TYPE = 'FLOAT'

SQL = (
    "SELECT n_name AS nation, "
    "  EXTRACT(YEAR FROM o_orderdate) AS o_year, "
    "  SUM(udf_q09(p_name, l_extendedprice, l_discount, "
    "              ps_supplycost, l_quantity)) AS sum_profit "
    "FROM part "
    "JOIN lineitem ON p_partkey = l_partkey "
    "JOIN supplier ON s_suppkey = l_suppkey "
    "JOIN partsupp ON ps_suppkey = l_suppkey AND ps_partkey = l_partkey "
    "JOIN orders ON o_orderkey = l_orderkey "
    "JOIN nation ON s_nationkey = n_nationkey "
    "GROUP BY n_name, EXTRACT(YEAR FROM o_orderdate) "
    "ORDER BY nation, o_year DESC"
)
