"""
TPC-H Q1: Pricing Summary Report Query

Business question:
    Provides a summary pricing report for all lineitems shipped before a
    given date, grouped by return flag and line status.

SQL semantics:
    SELECT l_returnflag, l_linestatus,
           SUM(l_quantity) AS sum_qty,
           SUM(l_extendedprice) AS sum_base_price,
           SUM(l_extendedprice*(1-l_discount)) AS sum_disc_price,
           SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge,
           AVG(l_quantity) AS avg_qty,
           AVG(l_extendedprice) AS avg_price,
           AVG(l_discount) AS avg_disc,
           COUNT(*) AS count_order
    FROM lineitem
    WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus;

UDF mapping:
    - WHERE filter → return None to discard rows outside date range
    - Per-row calculated fields (disc_price, charge) → UDF returns a struct
    - SUM / AVG / COUNT → handled by Flink SQL GROUP BY
    - ORDER BY → handled by Flink SQL
"""

from datetime import date, timedelta

# Pre-compute the cutoff date: 1998-12-01 minus 90 days = 1998-09-02
_CUTOFF = date(1998, 12, 1) - timedelta(days=90)  # 1998-09-02


def udf_q01(shipdate, quantity, extendedprice, discount, tax,
            returnflag, linestatus):
    """
    Pure-Python UDF for TPC-H Q1.

    Parameters
    ----------
    shipdate : str
        l_shipdate in 'YYYY-MM-DD' format.
    quantity : float
        l_quantity.
    extendedprice : float
        l_extendedprice.
    discount : float
        l_discount.
    tax : float
        l_tax.
    returnflag : str
        l_returnflag ('A', 'N', 'R').
    linestatus : str
        l_linestatus ('F', 'O').

    Returns
    -------
    tuple or None
        (quantity, extendedprice, disc_price, charge, discount) if row passes
        the date filter; None otherwise.
    """
    # WHERE: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
    if shipdate > _CUTOFF.strftime('%Y-%m-%d'):
        return None

    disc_price = extendedprice * (1 - discount)
    charge = disc_price * (1 + tax)

    return (float(quantity), float(extendedprice),
            float(disc_price), float(charge), float(discount))


# --------------- Runner metadata ---------------

UDF_INPUTS = [
    'l_shipdate', 'l_quantity', 'l_extendedprice',
    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
]

UDF_RESULT_TYPE = (
    'ROW<'
    '`quantity`      FLOAT, '
    '`extendedprice`  FLOAT, '
    '`disc_price`     FLOAT, '
    '`charge`         FLOAT, '
    '`discount`       FLOAT'
    '>'
)

SQL = (
    "SELECT t.l_returnflag, t.l_linestatus, "
    "  SUM(t.quantity)      AS sum_qty, "
    "  SUM(t.extendedprice) AS sum_base_price, "
    "  SUM(t.disc_price)    AS sum_disc_price, "
    "  SUM(t.charge)        AS sum_charge, "
    "  AVG(t.quantity)      AS avg_qty, "
    "  AVG(t.extendedprice) AS avg_price, "
    "  AVG(t.discount)      AS avg_disc, "
    "  COUNT(*)             AS count_order "
    "FROM ("
    "  SELECT l_returnflag, l_linestatus, "
    "    udf_q01(l_shipdate, l_quantity, l_extendedprice, "
    "            l_discount, l_tax, l_returnflag, l_linestatus) "
    "    AS (quantity, extendedprice, disc_price, charge, discount) "
    "  FROM lineitem"
    ") t "
    "WHERE t.quantity IS NOT NULL "
    "GROUP BY t.l_returnflag, t.l_linestatus "
    "ORDER BY t.l_returnflag, t.l_linestatus"
)
