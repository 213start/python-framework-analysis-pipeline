"""
TPC-H Q19: Discounted Revenue Query

Business question:
    Reports the discounted revenue for three groups of parts and shipping
    modes, filtered by brand, container, size, quantity, and delivery method.

SQL semantics:
    SELECT sum(l_extendedprice*(1-l_discount)) as revenue
    FROM lineitem, part
    WHERE (p_partkey = l_partkey
           AND p_brand = 'Brand#31'
           AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
           AND l_quantity >= 1 AND l_quantity <= 11
           AND p_size BETWEEN 1 AND 5
           AND l_shipmode IN ('AIR','AIR REG')
           AND l_shipinstruct = 'DELIVER IN PERSON')
       OR (p_partkey = l_partkey
           AND p_brand = 'Brand#32'
           AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK')
           AND l_quantity >= 10 AND l_quantity <= 20
           AND p_size BETWEEN 1 AND 10
           AND l_shipmode IN ('AIR','AIR REG')
           AND l_shipinstruct = 'DELIVER IN PERSON')
       OR (p_partkey = l_partkey
           AND p_brand = 'Brand#33'
           AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG')
           AND l_quantity >= 20 AND l_quantity <= 30
           AND p_size BETWEEN 1 AND 15
           AND l_shipmode IN ('AIR','AIR REG')
           AND l_shipinstruct = 'DELIVER IN PERSON');

UDF mapping:
    - All WHERE conditions (3 OR branches + common filters) → UDF
    - Revenue calculation → return value
    - SUM → handled by Flink SQL
"""

_SM_CONTAINERS = frozenset(('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))
_MED_CONTAINERS = frozenset(('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))
_LG_CONTAINERS = frozenset(('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))
_AIR_MODES = frozenset(('AIR', 'AIR REG'))


def udf_q19(brand, container, p_size, quantity, shipmode, shipinstruct,
            extendedprice, discount):
    """
    Pure-Python UDF for TPC-H Q19.

    Parameters
    ----------
    brand : str
        p_brand.
    container : str
        p_container.
    p_size : float
        p_size.
    quantity : float
        l_quantity.
    shipmode : str
        l_shipmode.
    shipinstruct : str
        l_shipinstruct.
    extendedprice : float
        l_extendedprice.
    discount : float
        l_discount.

    Returns
    -------
    float or None
        Revenue contribution if row matches any of the 3 branches; None otherwise.
    """
    # Common conditions for all branches
    if shipmode not in _AIR_MODES:
        return None
    if shipinstruct != 'DELIVER IN PERSON':
        return None

    size = float(p_size)
    qty = float(quantity)

    # Branch 1: Brand#31 + SM containers + qty 1-11 + size 1-5
    if (brand == 'Brand#31'
            and container in _SM_CONTAINERS
            and 1 <= qty <= 11
            and 1 <= size <= 5):
        return float(extendedprice * (1 - discount))

    # Branch 2: Brand#32 + MED containers + qty 10-20 + size 1-10
    if (brand == 'Brand#32'
            and container in _MED_CONTAINERS
            and 10 <= qty <= 20
            and 1 <= size <= 10):
        return float(extendedprice * (1 - discount))

    # Branch 3: Brand#33 + LG containers + qty 20-30 + size 1-15
    if (brand == 'Brand#33'
            and container in _LG_CONTAINERS
            and 20 <= qty <= 30
            and 1 <= size <= 15):
        return float(extendedprice * (1 - discount))

    return None


# --------------- Runner metadata ---------------

UDF_INPUTS = [
    'p_brand', 'p_container', 'p_size',
    'l_quantity', 'l_shipmode', 'l_shipinstruct',
    'l_extendedprice', 'l_discount',
]

UDF_RESULT_TYPE = 'FLOAT'

SQL = (
    "SELECT "
    "  SUM(udf_q19(p_brand, p_container, p_size, "
    "              l_quantity, l_shipmode, l_shipinstruct, "
    "              l_extendedprice, l_discount)) AS revenue "
    "FROM lineitem "
    "JOIN part ON p_partkey = l_partkey"
)
