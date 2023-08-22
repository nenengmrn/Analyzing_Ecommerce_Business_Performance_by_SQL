--Create Table
create table products (
	product_id varchar primary key,
	product_category_name varchar,
	product_name_lenght int,
	product_description_lenght int,
	product_photos_qty int,
	product_weight_g int,
	product_length_cm int,
	product_height_cm int,
	product_width_cm int
);

create table geolocation (
	geolocation_zip_code_prefix int,
	geolocation_lat float8,
	geolocation_lng float8,
	geolocation_city varchar,
	geolocation_state varchar
);

create table sellers (
	seller_id varchar primary key,
	seller_zip_code_prefix int,
	seller_city varchar,
	seller_state varchar
);

create table customers (
	customer_id varchar primary key,
	customer_unique_id varchar,
	customer_zip_code_prefix int,
	customer_city varchar,
	customer_state varchar
);

create table orders (
	order_id varchar primary key,
	customer_id varchar,
	order_status varchar,
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp,
	foreign key (customer_id) references customers(customer_id) on delete set null
);

create table order_items (
	order_id varchar,
	order_item_id int,
	product_id varchar,
	seller_id varchar,
	shipping_limit_date timestamp,
	price float8,
	freight_value float8,
	foreign key (product_id) references products(product_id) on delete set null,
	foreign key (order_id) references orders(order_id) on delete set null,
	foreign key (seller_id) references sellers(seller_id) on delete set null
);

create table order_payments (
	order_id varchar,
	payment_sequential int,
	payment_type varchar,
	payment_installments int,
	payment_value float8,
	foreign key (order_id) references orders(order_id) on delete set null
);

create table order_reviews (
	review_id varchar,
	order_id varchar,
	review_score int,
	review_comment_title varchar,
	review_comment_message varchar,
	review_creation_date timestamp,
	review_answer_timestamp timestamp,
	foreign key (order_id) references orders(order_id) on delete set null
);


--Annual Customer Activity Growth Analysis

with
monthly_active_users as (
	select 
		year,
		round(avg(total),0) as average_mau
	from (
		select 
			date_part('year', o.order_purchase_timestamp) as year,
			date_part('month', o.order_purchase_timestamp) as month,
			count(distinct c.customer_unique_id) as total
		from 
			orders as o 
			join customers as c
			on o.customer_id = c.customer_id
		group by 1, 2
	) as subq1
	group by 1
	order by 1
),

new_customer as (
	select 
		date_part('year', first_purchase) as year,
		count (first_purchase) as total_new_customer
	from (
		select
			c.customer_unique_id,
			min (o.order_purchase_timestamp) as first_purchase
		from 
			customers as c
			join orders as o
			on c.customer_id = o.customer_id
		group by 1
	) as subq2
	group by 1
	order by 1
),

repeat_order as (
	select
		year,
		count(1) as total_customer_repeat_order
	from (
		select
			date_part('year', o.order_purchase_timestamp) as year,
			c.customer_unique_id,
			count(1) as total
		from 
			orders as o
			join customers as c
			on o.customer_id = c.customer_id
		group by 1, 2
		having count(1) > 1
	) as subq3
	group by 1
	order by 1
),

frequency_order as (
	select 
		year,
		round(avg(total),2) as average_frequency_order
	from(
		select 
			date_part('year', o.order_purchase_timestamp) as year,
			customer_unique_id,
			count(1) as total
		from
			orders as o
			join customers as c
			on o.customer_id = c.customer_id
		group by 1, 2
	) as subq4
	group by 1
	order by 1
)

select
	mau.year,
	mau.average_mau,
	nc.total_new_customer,
	ro.total_customer_repeat_order,
	fo.average_frequency_order
from
	monthly_active_users as mau
	join new_customer as nc
	on mau.year = nc.year
	join repeat_order as ro
	on mau.year = ro.year
	join frequency_order as fo
	on mau.year = fo.year



--Annual Product Category Quality Analysis

create table annual_product_category as (
	with
		revenue as (
			select 
				extract(year from o.order_purchase_timestamp) as year,
				round(cast(sum(oi.price + oi.freight_value) as numeric),2) as total_revenue
			from 
				orders as o
				join order_items as oi
				on o.order_id = oi.order_id
				where o.order_status ='delivered'
			group by 1
			order by 1
			),
		
		canceled_order as (
			select 
				extract(year from order_purchase_timestamp) as year,
				count(order_id) as total_canceled_order
			from 
				orders
			where order_status = 'canceled'
			group by 1
			order by 1
			),

		top_kategori as (
			select 
				year,
				top_product_name,
				total_revenue_top
			from (
			select
				extract(year from o.order_purchase_timestamp) as year,
				p.product_category_name as top_product_name,
				round(cast(sum(oi.price + oi.freight_value) as numeric),2) as total_revenue_top,
				rank () over (partition by extract(year from o.order_purchase_timestamp) 
							  order by round(cast(sum(oi.price + oi.freight_value) as numeric),2) desc) as rank_product
			from 
				orders as o
				join order_items as oi
				on o.order_id = oi.order_id
				join products as p
				on oi.product_id = p.product_id
			where o.order_status = 'delivered'
			group by 1, 2
			) as subq1
			where rank_product = 1
			),

		top_cancel as (
			select
				year,
				canceled_product_name,
				total_cancel
			from (
			select 
				extract(year from o.order_purchase_timestamp) as year,
				p.product_category_name as canceled_product_name,
				count(o.order_id) as total_cancel,
				rank () over (partition by extract(year from o.order_purchase_timestamp) 
							  order by count(o.order_id) desc) as rank_product
			from
				orders as o
				join order_items as oi
				on o.order_id = oi.order_id
				join products as p
				on oi.product_id = p.product_id
			where o.order_status = 'canceled'
			group by 1, 2
			) as subq2
			where rank_product = 1
			)
	
select
	r.year,
	r.total_revenue,
	co.total_canceled_order,
	tk.top_product_name,
	tk.total_revenue_top,
	tc.canceled_product_name,
	tc.total_cancel
from
	revenue as r
	join canceled_order as co
	on r.year = co.year
	join top_kategori as tk
	on r.year = tk.year
	join top_cancel as tc
	on r.year = tc.year
)
	



--Annual Payment Type Usage Analysis
with
total_users as (
	select 
		op.payment_type,
		count(1) as total,
		round(100.00 * count(1)/(select count(1) from order_payments),2) as persentase
	from 
		order_payments as op
		join orders as o
		on op.order_id = o.order_id
	group by 1
	order by 2 desc
),

table_payments as (
	select 
		extract(year from o.order_purchase_timestamp) as year,
		op.payment_type,
		count (1) as total
	from 
		orders as o
		join order_payments as op
		on o.order_id = op.order_id
	group by 1, 2
	order by 1, 3 desc
)

select 
	tu.payment_type,
	tu.total,
	tu.persentase,
	sum(case when tp.year=2016 then tp.total else 0 end) as year_2016,
	sum(case when tp.year=2017 then tp.total else 0 end) as year_2017,
	sum(case when tp.year=2018 then tp.total else 0 end) as year_2018
from 
	total_users as tu
	join table_payments as tp
	on tu.payment_type = tp.payment_type
group by 1, 2, 3
order by 2 desc
