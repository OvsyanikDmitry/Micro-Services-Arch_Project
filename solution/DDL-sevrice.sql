

--CDM
create schema CDM;
--dm.user_product_counters
drop table if exists cdm.user_product_counters;
create table cdm.user_product_counters (
	id serial not null primary key,
	user_id uuid not null,
	product_id uuid not null,
	product_name varchar not null,
	order_cnt int not null
	);
	
ALTER TABLE cdm.user_product_counters ADD CONSTRAINT positive_order_cnt CHECK (order_cnt >= 0);

CREATE UNIQUE INDEX purchases_user_id_product_id_idx
ON cdm.user_product_counters (user_id, product_id);

--cdm.user_category_counters

drop table if exists cdm.user_category_counters;
create table cdm.user_category_counters (
	id serial not null primary key,
	user_id uuid not null,
	category_id uuid not null,
	category_name varchar not null,
	order_cnt int not null
	);
	
ALTER TABLE cdm.user_category_counters ADD CONSTRAINT ucc_positive_order_cnt CHECK (order_cnt >= 0);

CREATE UNIQUE INDEX ucc_purchases_user_id_product_id_idx
ON cdm.user_category_counters (user_id, category_id);


--STG
create schema stg;

drop table if exists stg.order_events;

create table stg.order_events (
	object_id int not null unique,
	object_type varchar not null,
	sent_dttm timestamp not null,
	payload json not null
);


--DDS

create schema dds;

--dds.h_user
drop table if exists dds.h_user;

create table dds.h_user(
	h_user_pk uuid not null primary key,
	user_id varchar not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
);

select * from dds.h_user;

--dds.h_product

drop table if exists dds.h_product;

create table dds.h_product (
	h_product_pk uuid not null primary key,
	product_id varchar not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
	);

select * from dds.h_product;

--dds.h_category
drop table if exists dds.h_category;

create table dds.h_category (
	h_category_pk uuid not null primary key,
	category_name varchar not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
);

select * from dds.h_category;

--dds.h_restaurant
drop table if exists dds.h_restaurant;

create table dds.h_restaurant (
	h_restaurant_pk uuid not null primary key,
	restaurant_id varchar not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null);

select * from dds.h_restaurant;


--dds.h_order
drop table if exists dds.h_order;

create table dds.h_order (
	h_order_pk uuid not null primary key,
	order_id int not null,
	order_dt timestamp not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
	);

select * from dds.h_order;



--LINKS
--dds.l_order_product
drop table if exists dds.l_order_product;

create table dds.l_order_product (
	hk_order_product_pk uuid not null,
	h_order_pk uuid not null,
	h_product_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
	);

--fk_hk_order_product_pk fk_hk_product_order_pk
alter table dds.l_order_product 
add constraint fk_hk_order_product_pk
foreign key (h_product_pk) references dds.h_product(h_product_pk);

alter table dds.l_order_product 
add constraint fk_hk_product_order_pk
foreign key (h_order_pk) references dds.h_order(h_order_pk);

select * from dds.l_order_product;



--dds.l_product_restaurant
drop table if exists dds.l_product_restaurant;

create table dds.l_product_restaurant (
	hk_product_restaurant_pk uuid not null,
	h_restaurant_pk uuid not null,
	h_product_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
	);

--fk_hk_restaurant_product_pk fk_hk_product_restaurant_pk
alter table dds.l_product_restaurant 
add constraint fk_hk_restaurant_product_pk
foreign key (h_product_pk) references dds.h_product(h_product_pk);

alter table dds.l_product_restaurant 
add constraint fk_hk_product_restaurant_pk
foreign key (h_restaurant_pk) references dds.h_restaurant(h_restaurant_pk);

select * from dds.l_product_restaurant;



--dds.l_product_restaurant
drop table if exists dds.l_product_category;

create table dds.l_product_category (
	hk_product_category_pk uuid not null,
	h_category_pk uuid not null,
	h_product_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
	);

--fk_hk_product_category_pk fk_hk_category_product_pk
alter table dds.l_product_category 
add constraint fk_hk_product_category_pk
foreign key (h_product_pk) references dds.h_product(h_product_pk);

alter table dds.l_product_category 
add constraint fk_hk_category_product_pk
foreign key (h_category_pk) references dds.h_category(h_category_pk);

select * from dds.l_product_category;



--dds.l_order_user
drop table if exists dds.l_order_user;

create table dds.l_order_user (
	hk_order_user_pk uuid not null,
	h_user_pk uuid not null,
	h_order_pk uuid not null,
	load_dt timestamp not null,
	load_src varchar default 'orders-system-kafka' not null
	);

--fk_hk_user_order_pk fk_hk_order_user_pk
alter table dds.l_order_user 
add constraint fk_hk_user_order_pk
foreign key (h_user_pk) references dds.h_user(h_user_pk);

alter table dds.l_order_user 
add constraint fk_hk_order_user_pk
foreign key (h_order_pk) references dds.h_order(h_order_pk);

select * from dds.l_order_user;

--SATTELITS
--dds.s_user_names
drop table if exists dds.s_user_names;

CREATE TABLE IF NOT EXISTS dds.s_user_names(
h_user_pk UUID NOT NULL REFERENCES dds.h_user(h_user_pk), 
username VARCHAR NOT NULL, 
userlogin VARCHAR NOT NULL, 
load_dt TIMESTAMP NOT NULL, 
load_src VARCHAR NOT NULL,
hk_user_names_hashdiff UUID NOT NULL UNIQUE, 
PRIMARY KEY(h_user_pk, load_dt) );


--dds.s_product_names
drop table if exists dds.s_product_names;

CREATE TABLE IF NOT EXISTS dds.s_product_names(
h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk), 
name VARCHAR NOT NULL, 
load_dt TIMESTAMP NOT NULL, 
load_src VARCHAR NOT NULL,
hk_product_names_hashdiff UUID NOT NULL UNIQUE, 
PRIMARY KEY(h_product_pk, load_dt) );


--dds.s_product_names
drop table if exists dds.s_restaurant_names;

CREATE TABLE IF NOT EXISTS dds.s_restaurant_names(
h_restaurant_pk UUID NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk), 
name VARCHAR NOT NULL, 
load_dt TIMESTAMP NOT NULL, 
load_src VARCHAR NOT NULL,
hk_restaurant_names_hashdiff UUID NOT NULL UNIQUE, 
PRIMARY KEY(h_restaurant_pk, load_dt) );


--dds.s_order_cost
drop table if exists dds.s_order_cost;

CREATE TABLE IF NOT EXISTS dds.s_order_cost(
h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk), 
cost decimal(19, 5) NOT NULL, 
payment decimal(19, 5) NOT NULL, 
load_dt TIMESTAMP NOT NULL, 
load_src VARCHAR NOT NULL,
hk_order_cost_hashdiff UUID NOT NULL UNIQUE, 
PRIMARY KEY(h_order_pk, load_dt) );

ALTER TABLE dds.s_order_cost ADD CONSTRAINT ucc_cosrt_order_cnt CHECK (cost >= 0);
ALTER TABLE dds.s_order_cost ADD CONSTRAINT ucc_payment_order_cnt CHECK (payment >= 0);

--dds.s_order_status
drop table if exists dds.s_order_status;

CREATE TABLE IF NOT EXISTS dds.s_order_status(
h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk), 
status VARCHAR NOT NULL, 
load_dt TIMESTAMP NOT NULL, 
load_src VARCHAR NOT NULL,
hk_order_status_hashdiff UUID NOT NULL UNIQUE, 
PRIMARY KEY(h_order_pk, load_dt) );



select payload from stg.order_events
limit 10;

select count(*) from stg.order_events;

select max(sent_dttm) from stg.order_events;
