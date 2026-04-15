def udf_q12(order, line):
    if order.orderkey != line.orderkey:
        return None
    priority = 1 if order.orderpriority in {"1-URGENT", "2-HIGH"} else 0
    return (line.shipmode, priority)
