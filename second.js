CREATE TABLE sales_order (
order_id INT,
customer_id INT,
product_id INT,
order_date TIMESTAMP,
order_amount DOUBLE,
 quantity INT,
 discount DOUBLE,
 tax DOUBLE,
total_amount DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE product (
product_id INT,
product_name STRING,
 category STRING,
 price DOUBLE,
 manufacturer STRING,
date_added TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE customer (
customer_id INT,
customer_name STRING,
 email STRING,
 phone STRING,
address STRUCT<street: STRING, city: STRING, state: STRING, zip: INT>,
date_joined TIMESTAMP
)
PARTITIONED BY (country STRING, state STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;


INSERT INTO TABLE product VALUES
(101, 'Laptop Pro 15', 'Electronics', 1500.00, 'TechBrand', '2024-01-10 10:00:00'),
(102, 'Smartphone X', 'Electronics', 800.00, 'PhoneCorp', '2024-02-05 12:30:00'),
(103, 'Wireless Headphones', 'Electronics', 200.00, 'SoundMax', '2024-03-12 09:15:00'),
(104, 'Office Chair', 'Furniture', 250.00, 'FurniCo', '2024-01-20 14:00:00'),
(105, 'Coffee Table', 'Furniture', 150.00, 'HomeStyle', '2024-02-18 11:00:00');


INSERT INTO TABLE sales_order VALUES
(301, 201, 101, '2024-03-01 10:00:00', 1500.00, 1, 100.00, 50.00, 1450.00),
(302, 201, 103, '2024-03-05 12:30:00', 200.00, 2, 0.00, 20.00, 220.00),
(303, 202, 102, '2024-03-07 14:00:00', 800.00, 1, 50.00, 40.00, 790.00),
(304, 203, 104, '2024-03-10 16:15:00', 250.00, 1, 0.00, 25.00, 275.00),
(305, 203, 105, '2024-03-12 11:00:00', 150.00, 3, 10.00, 15.00, 455.00);


201,Alice Johnson,alice@example.com,555-1234,123 Maple St|Los Angeles|CA|90001,2023-12-01 10:00:00
202,Bob Smith,bob@example.com,555-5678,456 Oak Ave|San Francisco|CA|94102,2023-12-15 09:30:00
203,Charlie Lee,charlie@example.com,555-8765,789 Pine Rd|San Diego|CA|92103,2024-01-05 11:15:00


in csv in cloudera directly


ALTER TABLE customer ADD PARTITION (country='USA', state='California');

LOAD DATA LOCAL INPATH '/home/cloudera/customer_data.txt'
INTO TABLE customer
PARTITION (country='USA', state='California');


1)SELECT SUM(total_amount) AS total_sales FROM sales_order;
2)SELECT p.product_name, SUM(s.total_amount) AS total_sales
FROM sales_order s
JOIN product p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT N;
3)SELECT c.customer_name, SUM(s.total_amount) AS total_sales
FROM sales_order s
JOIN customer c ON s.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_sales DESC;
4)SELECT YEAR(order_date) AS year, MONTH(order_date) AS month, SUM(total_amount) AS
total_sales
FROM sales_order
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;
5)SELECT p.product_name, SUM(s.quantity) AS total_quantity
FROM sales_order s
JOIN product p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_quantity DESC;
6)SELECT category, product_name, price
FROM (
 SELECT category, product_name, price,
 ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rank
 FROM product
) ranked_products
WHERE rank = 1;

create database if not exists second;
use second;
select current_database();
show databases();
drop database if exists second;
