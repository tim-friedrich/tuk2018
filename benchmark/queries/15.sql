-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

create view REVENUE_VIEW (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= TO_DATE('1996-01-01', 'yyyy-mm-dd')
		and l_shipdate < ADD_MONTHS(TO_DATE('1996-01-01', 'yyyy-mm-dd'), 3)
	group by
		l_suppkey;


select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	REVENUE_VIEW
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			REVENUE_VIEW
	)
order by
	s_suppkey;

drop view REVENUE_VIEW;