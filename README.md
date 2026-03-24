# BI_Agent

🌟 RAG + Few-Shot Performance Comparison
To demonstrate the power of our core RAG (Retrieval-Augmented Generation) and Few-Shot (Knowledge Base) mechanisms, we tested the BI Agent against a classic 4-Table JOIN e-commerce scenario.

📝 Test Case
Question: "Calculate the total items and total revenue of 'Accessories' purchased by non-VIP users in each country. Only include paid orders, and sort by total revenue in descending order."

❌ Before Few-Shot: Hallucination & Query Failure
With only Schema RAG enabled but lacking business context, the 7B local LLM took shortcuts. It hallucinated columns that don't exist in the joined tables, resulting in an invalid SQL query.

SQL
-- ❌ Failed SQL Generation
SELECT 
    country, 
    SUM(CASE WHEN is_vip = 0 AND category = 'Accessories' THEN 1 ELSE 0 END) AS total_items,
    SUM(CASE WHEN is_vip = 0 AND category = 'Accessories' THEN price ELSE 0 END) AS total_amount
FROM orders
JOIN users ON orders.user_id = users.id
WHERE paid_at IS NOT NULL AND is_vip = 0 AND category = 'Accessories'
GROUP BY country
ORDER BY total_amount DESC;
🚨 Failure Analysis: The model completely missed joining the products and order_items tables, falsely assuming the category and price fields existed directly within the orders or users tables.

✅ After Few-Shot: Perfect Generation & Reasoning
After injecting a correct query pattern into the pgvector knowledge base via the /system/add-example endpoint, the LLM not only learned the correct 4-table JOIN logic but also generalized the reasoning to calculate the total revenue correctly.

SQL
-- ✅ Successful SQL Generation
SELECT 
    u.country, 
    SUM(oi.quantity) AS total_items, 
    SUM(p.price * oi.quantity) AS total_amount 
FROM users u 
JOIN orders o ON u.id = o.user_id 
JOIN order_items oi ON o.id = oi.order_id 
JOIN products p ON oi.product_id = p.id 
WHERE u.is_vip = FALSE 
  AND o.status = 'paid' 
  AND p.category = 'Accessories' 
GROUP BY u.country 
ORDER BY total_amount DESC;
🎯 Key Highlights:

Perfect JOINs: Accurately identified the schema relationships and connected users -> orders -> order_items -> products.

Smart Aggregation: Instead of rigidly copying the exact text from the few-shot example, the model dynamically applied SUM(p.price * oi.quantity) to calculate the precise revenue, demonstrating genuine reasoning over simple memorization.