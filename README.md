# 🧠 Explainable BI Agent (InsightSQL)

Turn natural language into production-ready SQL — with reasoning.

## 🚀 Evolution of the BI Agent

We progressively enhanced the BI Agent across three stages:

1. **Raw LLM (Schema-only RAG)**
2. **Few-shot Learning (Knowledge Base Injection)**
3. **Chain-of-Thought (CoT) + Explainability**

---

## ❌ V1: Raw LLM (Schema RAG Only)

**Setup**

* Only schema-level RAG (table + column names)
* No business context
* No examples

---

### 🧪 Result

```sql
-- ❌ Incorrect SQL
SELECT 
    country,
    SUM(CASE WHEN is_vip = 0 AND category = 'Accessories' THEN 1 ELSE 0 END) AS total_items,
    SUM(CASE WHEN is_vip = 0 AND category = 'Accessories' THEN price ELSE 0 END) AS total_amount
FROM orders
JOIN users ON orders.user_id = users.id
WHERE paid_at IS NOT NULL 
  AND is_vip = 0 
  AND category = 'Accessories'
GROUP BY country
ORDER BY total_amount DESC;
```

---

### 🚨 Problems

* ❌ Hallucinated columns (`category`, `price`)
* ❌ Missing joins (`order_items`, `products`)
* ❌ Incorrect aggregation logic

> Schema awareness ≠ relational understanding

---

## ✅ V2: Few-shot Learning (Knowledge Base)

**Improvement**

* Injected correct SQL patterns into **pgvector**
* Provided multi-table JOIN examples
* Enabled semantic retrieval of query templates

---

### 🧪 Result

```sql
-- ✅ Correct SQL
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
```

---

### 🎯 Improvements

* ✅ Correct JOIN path: `users → orders → order_items → products`
* ✅ Accurate revenue calculation: `price × quantity`
* ✅ No hallucinated fields

> Few-shot transforms the model from guessing → pattern reasoning

---

## 🧠 V3: CoT + Explainability (Debug Mode)

**Improvement**

* Exposed **Chain-of-Thought (query_plan)**
* Added SQL validation layer
* Enabled full transparency (reasoning → SQL → execution)

---

### 🧪 Debug Output

```json
{
  "query_plan": "Step 1: Join users and orders (paid). Step 2: Join order_items and products (Accessories). Step 3: Filter non-VIP. Step 4: Aggregate and sort.",
  "generated_sql": "...",
  "validated_sql": "..."
}
```

---

### 🎯 Improvements

* 🧠 Explainable reasoning (not a black box)
* 🔍 Easier debugging of SQL errors
* 🛡️ Safer execution via validation layer

> CoT upgrades the system from correct → trustworthy

---

## 📊 Summary

| Version     | Capability               | Result          |
| ----------- | ------------------------ | --------------- |
| V1 Raw      | Schema RAG only          | ❌ Hallucination |
| V2 Few-shot | Pattern learning         | ✅ Correct SQL   |
| V3 CoT      | Reasoning + transparency | ✅ + Explainable |

---

## 🧩 Key Insight

This evolution shows a clear progression:

```text
Schema Awareness → Pattern Learning → Reasoned Execution
```

* **RAG alone is not enough**
* **Few-shot enables structure understanding**
* **CoT enables trust and debuggability**
