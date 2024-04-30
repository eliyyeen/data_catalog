
create or replace table best_selling_item as
SELECT oi.product_id as product_id, p.name as product_name, p.category as product_category, count(*) as num_of_orders
FROM BST.DEMO.products as p
JOIN BST.DEMO.order_items as oi
ON p.id = oi.product_id
GROUP BY oi.product_id,p.name,p.category
ORDER BY num_of_orders DESC


create or replace table top_10_spenders as
SELECT u.id as user_id, u.first_name, u.last_name, avg(oi.sale_price) as avg_sale_price
FROM BST.DEMO.users as u
JOIN BST.DEMO.order_items as oi
ON u.id = oi.user_id
GROUP BY 1,2,3
ORDER BY avg_sale_price DESC
LIMIT 10