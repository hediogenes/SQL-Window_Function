SELECT customer_unique_id AS customer,
       DATE(order_purchase_timestamp) AS purchase_date,
       price AS income_per_customer,
       SUM(price) OVER w AS full_income_per_customer
FROM e_shop.olist_order_items_dataset a LEFT JOIN e_shop.olist_orders_dataset b ON (a.order_id = b.order_id)
                                        LEFT JOIN e_shop.olist_customers_dataset c ON (b.customer_id = c.customer_id)
WHERE customer_unique_id IN 
       (
       SELECT customer_unique_id
       FROM e_shop.olist_orders_dataset a LEFT JOIN e_shop.olist_customers_dataset b ON (a.customer_id = b.customer_id)
       WHERE order_delivered_customer_date IS NOT NULL
       GROUP BY customer_unique_id
       HAVING COUNT(DISTINCT order_id) >= 2
       )
WINDOW w AS
       (
       PARTITION BY customer_unique_id
       ORDER BY order_purchase_timestamp ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
ORDER BY customer_unique_id ASC
LIMIT 49
