### Q
How many customers are there in total?

### SQL
SELECT COUNT(*) FROM customers

### Q
List the names and emails of customers who have placed orders with total amount greater than $5000.

### SQL
SELECT DISTINCT c.name, c.email
FROM customers AS c
INNER JOIN orders AS o ON c.customer_id = o.customer_id
WHERE o.total_amount > 5000

### Q
What is the total quantity sold for each product? Show the top 5 products by quantity sold.

### SQL
SELECT p.name, SUM(oi.quantity) AS total_sold
FROM products AS p
INNER JOIN order_items AS oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.name
ORDER BY total_sold DESC
LIMIT 5

### Q
How many orders were placed in March 2024?

### SQL
SELECT COUNT(*)
FROM orders
WHERE order_date >= '2024-03-01' AND order_date < '2024-04-01'

### Q
List all products that have never received a review, showing product name and category.

### SQL
SELECT p.name, c.name AS category_name
FROM products AS p
INNER JOIN categories AS c ON p.category_id = c.category_id
LEFT JOIN reviews AS r ON p.product_id = r.product_id
WHERE r.review_id IS NULL

### Q
Find the average rating for each product. Use COALESCE to show 0 for products with no reviews.

### SQL
SELECT p.name, COALESCE(AVG(r.rating), 0) AS avg_rating
FROM products AS p
LEFT JOIN reviews AS r ON p.product_id = r.product_id
GROUP BY p.product_id, p.name
ORDER BY avg_rating DESC

### Q
Which customers registered in 2023? Show name and registration month truncated to month.

### SQL
SELECT name, TO_CHAR(registration_date, 'YYYY-MM') AS reg_month
FROM customers
WHERE EXTRACT(YEAR FROM registration_date) = 2023
ORDER BY registration_date
