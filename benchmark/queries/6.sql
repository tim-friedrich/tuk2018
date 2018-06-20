-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998


select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= TO_DATE('1994-01-01', 'yyyy-mm-dd')
	and l_shipdate < ADD_YEARS(TO_DATE('1994-01-01', 'yyyy-mm-dd'), 1)
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
