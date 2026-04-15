"""
TPC-H Q22: Global Sales Opportunity Query

Business question:
    Identifies customers from specific countries by their phone area codes
    who have not placed orders but have a positive account balance, and
    counts them by country code.

SQL semantics:
    SELECT cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
    FROM (SELECT substring(c_phone, 1, 2) as cntrycode, c_acctbal
          FROM customer
          WHERE substring(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
            AND c_acctbal > (SELECT avg(c_acctbal) FROM customer
                             WHERE c_acctbal > 0.00
                               AND substring(c_phone, 1, 2)
                                   IN ('13','31','23','29','30','18','17'))
            AND NOT EXISTS (SELECT * FROM orders
                            WHERE o_custkey = c_custkey)) AS custsale
    GROUP BY cntrycode
    ORDER BY cntrycode;

UDF mapping:
    - Phone country code extraction + target set filter → UDF
    - Returns (cntrycode, acctbal) for matching rows
    - c_acctbal > avg subquery + NOT EXISTS → handled by Flink SQL WHERE
    - GROUP BY / SUM / COUNT → handled by Flink SQL
"""

_TARGET_CODES = frozenset(('13', '31', '23', '29', '30', '18', '17'))


def udf_q22(phone, acctbal):
    """
    Pure-Python UDF for TPC-H Q22.

    Parameters
    ----------
    phone : str
        c_phone.
    acctbal : float
        c_acctbal.

    Returns
    -------
    tuple or None
        (cntrycode, acctbal) if phone prefix matches target codes;
        None otherwise.
    """
    cntrycode = phone[:2]
    if cntrycode not in _TARGET_CODES:
        return None
    return (cntrycode, float(acctbal))


# --------------- Runner metadata ---------------

UDF_INPUTS = ['c_phone', 'c_acctbal']

UDF_RESULT_TYPE = 'ROW<`cntrycode` STRING, `acctbal` FLOAT>'

SQL = (
    "SELECT r.cntrycode, "
    "  COUNT(*) AS numcust, "
    "  SUM(r.acctbal) AS totacctbal "
    "FROM ("
    "  SELECT udf_q22(c_phone, c_acctbal) AS (cntrycode, acctbal) "
    "  FROM customer "
    "  WHERE NOT EXISTS ("
    "    SELECT 1 FROM orders WHERE o_custkey = c_custkey"
    "  )"
    ") r "
    "WHERE r.cntrycode IS NOT NULL "
    "  AND r.acctbal > ("
    "    SELECT AVG(c_acctbal) FROM customer "
    "    WHERE c_acctbal > 0.00 "
    "      AND SUBSTRING(c_phone, 1, 2) IN "
    "        ('13','31','23','29','30','18','17')"
    "  ) "
    "GROUP BY r.cntrycode "
    "ORDER BY r.cntrycode"
)
