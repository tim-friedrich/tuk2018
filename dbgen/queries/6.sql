-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= TO_DATE(':1', 'yyyy-mm-dd')
	and l_shipdate < ADD_YEARS(TO_DATE(':1', 'yyyy-mm-dd'), 1)
	and l_discount between :2 - 0.01 and :2 + 0.01
	and l_quantity < :3;
