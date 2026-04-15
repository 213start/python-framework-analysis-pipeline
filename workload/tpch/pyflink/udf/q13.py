"""
TPC-H Q13: Customer Distribution Query

Business question:
    Observes the distribution of customers by the number of orders they
    have made, excluding orders whose comments contain 'special' and
    'requests'.

SQL semantics:
    SELECT c_count, count(*) as custdist
    FROM (SELECT c_custkey, count(o_orderkey) as c_count
          FROM customer LEFT OUTER JOIN orders
            ON c_custkey = o_custkey
            AND o_comment NOT LIKE '%special%requests%'
          GROUP BY c_custkey) AS c_orders
    GROUP BY c_count
    ORDER BY custdist DESC, c_count DESC;

UDF mapping:
    - o_comment NOT LIKE '%special%requests%' → UDF returns None to exclude
    - LEFT JOIN / GROUP BY / ORDER BY → handled by Flink SQL

Note: Since this is a LEFT JOIN, rows without matching orders have NULL
      o_comment. The UDF should pass through NULLs (treat as valid = no
      comment to filter). Only filter when o_comment actually contains
      both 'special' and 'requests'.
"""


def udf_q13(o_comment):
    """
    Pure-Python UDF for TPC-H Q13.

    Parameters
    ----------
    o_comment : str or None
        o_comment. None for LEFT JOIN non-matches.

    Returns
    -------
    int or None
        1 if the order passes the NOT LIKE filter; None if excluded.
        NULL o_comment (LEFT JOIN miss) returns 1 (valid — no order).
    """
    if o_comment is None:
        return 1
    if 'special' in o_comment and 'requests' in o_comment:
        return None
    return 1


# --------------- Runner metadata ---------------

UDF_INPUTS = ['o_comment']

UDF_RESULT_TYPE = 'INT'

SQL = (
    "SELECT c_count, COUNT(*) AS custdist "
    "FROM ("
    "  SELECT c_custkey, "
    "    SUM(udf_q13(o_comment)) AS c_count "
    "  FROM customer "
    "  LEFT JOIN orders ON c_custkey = o_custkey "
    "  GROUP BY c_custkey"
    ") c_orders "
    "GROUP BY c_count "
    "ORDER BY custdist DESC, c_count DESC"
)
