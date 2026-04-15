"""
TPC-H Q6: Forecasting Revenue Change Query

Business question:
    Quantifies the amount of revenue increase that would have resulted from
    eliminating a specific range of discounts in a given year.

SQL semantics:
    SELECT sum(l_extendedprice * l_discount) AS revenue
    FROM lineitem
    WHERE l_shipdate >= DATE '1994-01-01'
      AND l_shipdate <  DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity < 24;

UDF mapping:
    - WHERE clause  → filter (return None to discard row)
    - l_extendedprice * l_discount → return value (contribution to SUM)

    Aggregation (SUM) and table scan are handled by the framework runner.
"""


def udf_q06(shipdate, discount, quantity, extendedprice):
    """
    Pure-Python UDF for TPC-H Q6.

    Parameters
    ----------
    shipdate : str
        l_shipdate in 'YYYY-MM-DD' format.
    discount : float
        l_discount.
    quantity : float
        l_quantity.
    extendedprice : float
        l_extendedprice.

    Returns
    -------
    float or None
        Revenue contribution (extendedprice * discount) if the row passes
        all filters; None otherwise (row excluded from aggregation).
    """
    # WHERE: l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
    if shipdate < '1994-01-01' or shipdate >= '1995-01-01':
        return None

    # WHERE: l_discount BETWEEN 0.05 AND 0.07
    if discount < 0.05 or discount > 0.07:
        return None

    # WHERE: l_quantity < 24
    if quantity >= 24:
        return None

    # SELECT: l_extendedprice * l_discount
    return float(extendedprice * discount)


# --------------- Runner metadata ---------------

# Columns passed to the UDF (in order of function parameters).
UDF_INPUTS = ['l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice']

# Flink SQL result type for the UDF return value.
UDF_RESULT_TYPE = 'FLOAT'

# Flink SQL that uses this UDF. The runner registers the UDF under the name
# "udf_q06" and executes this statement.
SQL = (
    "SELECT SUM(udf_q06(l_shipdate, l_discount, l_quantity, l_extendedprice)) "
    "AS revenue FROM lineitem"
)
