SELECT customer_unique_id AS customer,
       DATE(order_purchase_timestamp) AS purchase_date,
       price AS income_per_customer,
       SUM(price) OVER w AS full_income_per_customer
/* В SELECT я вложил следующие колонки:
*  customer_unique_id содержит уникальные ID всех пользователей. Я присвоил ей название customer.
*  order_purchase_timestamp содержит дату и время покупки. Я оставил от нее только дату (с помощью DATE()) и присвоил ей название purchase_date.
*  price содержит цену каждого заказа. Я присвоил ей название income_per_customer.
*  Я вновь вложил price, но уже для реализации оконной функции, которая будет считать сумму принесенной прибыли. Я присвоил ей название full_income_per_customer.
*/

FROM e_shop.olist_order_items_dataset a LEFT JOIN e_shop.olist_orders_dataset b ON (a.order_id = b.order_id)
                                        LEFT JOIN e_shop.olist_customers_dataset c ON (b.customer_id = c.customer_id)
/* Я заджойнил все имеющиеся датасеты без потери данных. */

WHERE customer_unique_id IN 
       (
       SELECT customer_unique_id
       FROM e_shop.olist_orders_dataset a LEFT JOIN e_shop.olist_customers_dataset b ON (a.customer_id = b.customer_id)
       WHERE order_delivered_customer_date IS NOT NULL
       GROUP BY customer_unique_id
       HAVING COUNT(DISTINCT order_id) >= 2
       )
/* В WHERE я составил подзапрос, который оставит только тех пользователей, которые:
       - Сделали больше 2 заказов (HAVING COUNT(DISTINCT order_id) >= 2).
       - Получили заказ (WHERE order_delivered_customer_date IS NOT NULL).
*/

WINDOW w AS
       (
       PARTITION BY customer_unique_id
       ORDER BY order_purchase_timestamp ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
/* Реализую оконную функцию.
   Я сделал разбивку по customer_unique_id, чтобы считать накопленную прибыль по пользователям (PARTITION BY customer_unique_id).
   Я отсортировал прибыль по возрастанию даты (ORDER BY order_purchase_timestamp ASC).
   Я поставил границы окна так, чтобы сумма изменялась (в моем случае увеличивалась) с каждым новым значением (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW).
*/

ORDER BY customer_unique_id ASC -- Сделаю сортировку по присвоенным ID пользователей.
LIMIT 49 -- Поставлю ограничение в 49 строк результата для экономии веса таблицы.
