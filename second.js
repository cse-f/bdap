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
(101, 'Laptop Pro 15', 'Electronics', 85000.00, 'TechNova', '2024-01-10 10:00:00'),
(102, 'Smartphone X2', 'Electronics', 45000.00, 'InnovaMobiles', '2024-02-05 12:30:00'),
(103, 'Wireless Earbuds', 'Electronics', 5000.00, 'SoundBliss', '2024-03-12 09:15:00'),
(104, 'Ergo Office Chair', 'Furniture', 9000.00, 'FurniCraft', '2024-01-20 14:00:00'),
(105, 'Teak Coffee Table', 'Furniture', 6500.00, 'WoodArt', '2024-02-18 11:00:00');


INSERT INTO TABLE sales_order VALUES
(301, 201, 101, '2024-03-01 10:00:00', 85000.00, 1, 5000.00, 1500.00, 81500.00),
(302, 201, 103, '2024-03-05 12:30:00', 5000.00, 2, 0.00, 500.00, 10500.00),
(303, 202, 102, '2024-03-07 14:00:00', 45000.00, 1, 2500.00, 900.00, 43400.00),
(304, 203, 104, '2024-03-10 16:15:00', 9000.00, 1, 0.00, 200.00, 9200.00),
(305, 203, 105, '2024-03-12 11:00:00', 6500.00, 3, 500.00, 300.00, 19400.00);


201,Yashwanth Reddy,yashwanth@example.com,9876543210,12-3-45 Banjara Hills|Hyderabad|Telangana|500034,2023-12-01 10:00:00
202,Ananya Sharma,ananya@example.com,9123456780,8-2-123 Jubilee Hills|Hyderabad|Telangana|500033,2023-12-15 09:30:00
203,Arjun Kumar,arjun@example.com,9988776655,5-6-789 Hanamkonda|Warangal|Telangana|506001,2024-01-05 11:15:00



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
