def udf_q06(row):
    if not ("1994-01-01" <= row.shipdate < "1995-01-01"):
        return None
    if not (0.05 <= row.discount <= 0.07):
        return None
    if row.quantity >= 24:
        return None
    return row.extendedprice * row.discount
