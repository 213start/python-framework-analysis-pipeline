select
  l_shipmode,
  sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count
from orders, lineitem
where o_orderkey = l_orderkey
group by l_shipmode;
