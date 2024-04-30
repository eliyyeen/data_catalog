CREATE OR REPLACE STAGE csv_stage
  file_format = (type = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

CREATE OR REPLACE TABLE users(
    id  number,
    first_name  varchar(40),
    last_name varchar(40),
    email   varchar(50),
    age   number,
    gender  varchar(10),
    state varchar(50),
    street_address  varchar(100),
    postal_code   varchar(50),
    city  varchar(50),
    country  varchar(50),
    latitude  float,
    longitude float,
    traffic_source  varchar(40),
    created_at  varchar(50)
)
PUT file:///Users/amd/Desktop/Ecom_Dataset/user.csv @csv_stage;
copy into users from @csv_stage files = ('user.csv.gz');


CREATE OR REPLACE TABLE distributions(
id   INTEGER,
name    STRING,
latitude    FLOAT,
longitude   FLOAT
)
PUT file:///Users/amd/Desktop/Ecom_Dataset/distribution.csv @csv_stage;
copy into distributions from @csv_stage files = ('distribution.csv.gz');




CREATE OR REPLACE TABLE inventory_items (
id   INTEGER,
product_id   INTEGER,
created_at   STRING,
sold_at   STRING,
cost    FLOAT,
product_category    STRING,
product_name    STRING,
product_brand   STRING,
product_retail_price    FLOAT,
product_department    STRING,
product_sku   STRING,
product_distribution_center_id    INTEGER
);

PUT file:///Users/amd/Desktop/Ecom_Dataset/inventory.csv @csv_stage;
copy into inventory_items from @csv_stage files = ('inventory.csv.gz');




CREATE OR REPLACE TABLE order_items (
id    INTEGER,
order_id    INTEGER,
user_id   INTEGER,
product_id    INTEGER,
inventory_item_id   INTEGER,
status    STRING,
created_at    STRING,
shipped_at    STRING,
delivered_at  STRING,
returned_at   STRING,
sale_price    FLOAT
)

PUT file:///Users/amd/Desktop/Ecom_Dataset/order_items.csv @csv_stage;
copy into order_items from @csv_stage files = ('order_items.csv.gz');




id,
order_id,
user_id,
product_id,
inventory_item_id,
status,
created_at,
shipped_at,
delivered_at,
returned_at,
sale_price

CREATE OR REPLACE TABLE orders(
id   number,
order_id    INTEGER,
user_id   INTEGER ,
product_id   INTEGER,
inventory_item_id   INTEGER,
status    STRING ,
created_at    STRING,
shipped_at    STRING,
delivered_at    STRING,
returned_at   STRING,
sale_price   INTEGER
)

PUT file:///Users/amd/Desktop/Ecom_Dataset/orders.csv @csv_stage;
copy into orders from @csv_stage files = ('orders.csv.gz');





CREATE OR REPLACE TABLE products(
id    INTEGER,
cost    FLOAT,
category    STRING,
name    STRING,
brand   STRING,
retail_price    FLOAT,
department    STRING,
sku   STRING,
distribution_center_id    INTEGER
)

PUT file:///Users/amd/Desktop/Ecom_Dataset/products.csv @csv_stage;
copy into products from @csv_stage files = ('products.csv.gz');


CREATE OR REPLACE TABLE events(
id    INTEGER,
user_id   INTEGER,
sequence_number   INTEGER,
session_id    STRING,
created_at    STRING,
ip_address    STRING,
city    STRING,
state   STRING,
postal_code   STRING,
browser   STRING,
traffic_source    STRING,
uri   STRING ,
event_type    STRING
)

PUT file:///Users/amd/Desktop/Ecom_Dataset/events.csv @csv_stage;
copy into events from @csv_stage files = ('events.csv.gz');




















