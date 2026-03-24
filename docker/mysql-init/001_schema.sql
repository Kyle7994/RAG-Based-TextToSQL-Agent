-- ======================================================
-- 1. 初始化结构 (清理并建表)
-- ======================================================

-- 清理旧表（注意删除顺序，需先删从表再删主表）
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;

-- 创建用户表
CREATE TABLE users (
  id BIGINT PRIMARY KEY,
  email VARCHAR(255),
  country VARCHAR(64),
  signup_at DATETIME,
  channel VARCHAR(64),
  is_vip BOOLEAN
);

-- 创建产品表
CREATE TABLE products (
  id BIGINT PRIMARY KEY,
  name VARCHAR(255),
  category VARCHAR(128),
  price DECIMAL(10,2),
  created_at DATETIME
);

-- 创建订单表
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  user_id BIGINT,
  status VARCHAR(32),
  total_amount DECIMAL(10,2),
  created_at DATETIME,
  paid_at DATETIME,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- 创建订单明细表
CREATE TABLE order_items (
  id BIGINT PRIMARY KEY,
  order_id BIGINT,
  product_id BIGINT,
  quantity INT,
  unit_price DECIMAL(10,2),
  subtotal DECIMAL(10,2),
  FOREIGN KEY (order_id) REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- ======================================================
-- 2. 插入所有用户数据 (1-20)
-- ======================================================
INSERT INTO users VALUES
(1, 'alice@example.com', 'US', '2026-03-01 10:00:00', 'ads', true),
(2, 'bob@example.com', 'CA', '2026-03-02 11:00:00', 'organic', false),
(3, 'carol@example.com', 'US', '2026-03-03 12:00:00', 'referral', false),
(4, 'david@example.com', 'UK', '2026-03-04 09:30:00', 'ads', false),
(5, 'emma@example.com', 'US', '2026-03-05 14:20:00', 'organic', true),
(6, 'frank@example.com', 'DE', '2026-03-06 16:45:00', 'referral', false),
(7, 'grace@example.com', 'CA', '2026-03-07 08:10:00', 'ads', false),
(8, 'henry@example.com', 'US', '2026-03-08 19:05:00', 'organic', true),
(9, 'irene@example.com', 'FR', '2026-03-09 13:15:00', 'referral', false),
(10, 'jack@example.com', 'JP', '2026-03-10 21:40:00', 'ads', false),
(11, 'kevin@example.com', 'SG', '2026-03-11 09:00:00', 'organic', true),
(12, 'lisa@example.com', 'AU', '2026-03-12 10:30:00', 'ads', false),
(13, 'mike@example.com', 'US', '2026-03-13 11:15:00', 'referral', true),
(14, 'nina@example.com', 'BR', '2026-03-14 14:50:00', 'organic', false),
(15, 'oscar@example.com', 'KR', '2026-03-15 16:20:00', 'ads', false),
(16, 'paul@example.com', 'FR', '2026-03-16 08:45:00', 'referral', true),
(17, 'quinn@example.com', 'US', '2026-03-17 12:10:00', 'organic', false),
(18, 'rose@example.com', 'UK', '2026-03-18 17:30:00', 'ads', false),
(19, 'sam@example.com', 'IN', '2026-03-19 13:00:00', 'referral', false),
(20, 'tina@example.com', 'JP', '2026-03-20 20:15:00', 'organic', true);

-- ======================================================
-- 3. 插入所有产品数据 (1001-1020)
-- ======================================================
INSERT INTO products VALUES
(1001, 'Phone Case', 'Accessories', 19.99, '2026-02-01 00:00:00'),
(1002, 'USB-C Cable', 'Accessories', 9.99, '2026-02-05 00:00:00'),
(1003, 'Bluetooth Speaker', 'Electronics', 49.99, '2026-02-10 00:00:00'),
(1004, 'Wireless Mouse', 'Electronics', 25.99, '2026-02-12 00:00:00'),
(1005, 'Laptop Stand', 'Accessories', 35.50, '2026-02-14 00:00:00'),
(1006, 'Mechanical Keyboard', 'Electronics', 89.00, '2026-02-18 00:00:00'),
(1007, 'Desk Lamp', 'Home Office', 42.75, '2026-02-20 00:00:00'),
(1008, 'Notebook Pack', 'Stationery', 12.49, '2026-02-22 00:00:00'),
(1009, 'Water Bottle', 'Lifestyle', 18.20, '2026-02-25 00:00:00'),
(1010, 'Webcam', 'Electronics', 64.90, '2026-02-28 00:00:00'),
(1011, 'Gaming Chair', 'Furniture', 199.00, '2026-03-01 00:00:00'),
(1012, 'Monitor Stand', 'Accessories', 45.00, '2026-03-02 00:00:00'),
(1013, 'Noise Cancelling Headphones', 'Electronics', 299.99, '2026-03-03 00:00:00'),
(1014, 'Wireless Charger', 'Electronics', 29.50, '2026-03-04 00:00:00'),
(1015, 'Standing Desk', 'Furniture', 450.00, '2026-03-05 00:00:00'),
(1016, 'USB Hub', 'Accessories', 24.99, '2026-03-06 00:00:00'),
(1017, 'Ergonomic Mouse', 'Electronics', 55.00, '2026-03-07 00:00:00'),
(1018, 'Leather Journal', 'Stationery', 32.00, '2026-03-08 00:00:00'),
(1019, 'Coffee Mug', 'Lifestyle', 15.00, '2026-03-09 00:00:00'),
(1020, 'Screen Cleaner Kit', 'Accessories', 12.00, '2026-03-10 00:00:00');

-- ======================================================
-- 4. 插入所有订单数据 (101-121)
-- ======================================================
INSERT INTO orders VALUES
(101, 1, 'paid', 120.50, '2026-03-10 09:00:00', '2026-03-10 09:05:00'),
(102, 2, 'paid', 80.00, '2026-03-11 10:00:00', '2026-03-11 10:02:00'),
(103, 1, 'refunded', 50.00, '2026-03-12 11:00:00', '2026-03-12 11:03:00'),
(104, 3, 'paid', 200.00, '2026-03-15 13:00:00', '2026-03-15 13:04:00'),
(105, 4, 'paid', 51.98, '2026-03-16 09:15:00', '2026-03-16 09:17:00'),
(106, 5, 'paid', 35.50, '2026-03-17 10:40:00', '2026-03-17 10:42:00'),
(107, 6, 'pending', 89.00, '2026-03-18 11:25:00', NULL),
(108, 7, 'paid', 85.50, '2026-03-19 15:10:00', '2026-03-19 15:13:00'),
(109, 8, 'refunded', 24.98, '2026-03-20 16:30:00', '2026-03-20 16:35:00'),
(110, 9, 'paid', 18.20, '2026-03-21 12:05:00', '2026-03-21 12:07:00'),
(111, 10, 'cancelled', 64.90, '2026-03-22 18:45:00', NULL),
(112, 11, 'paid', 199.00, '2026-03-23 10:00:00', '2026-03-23 10:05:00'),
(113, 12, 'paid', 45.00, '2026-03-23 11:30:00', '2026-03-23 11:32:00'),
(114, 13, 'paid', 329.49, '2026-03-24 09:15:00', '2026-03-24 09:20:00'),
(115, 14, 'pending', 29.50, '2026-03-24 14:00:00', NULL),
(116, 15, 'paid', 450.00, '2026-03-25 10:45:00', '2026-03-25 10:50:00'),
(117, 16, 'paid', 79.99, '2026-03-25 16:20:00', '2026-03-25 16:22:00'),
(118, 17, 'refunded', 55.00, '2026-03-26 11:10:00', '2026-03-26 11:15:00'),
(119, 18, 'paid', 32.00, '2026-03-26 15:30:00', '2026-03-26 15:33:00'),
(120, 19, 'cancelled', 15.00, '2026-03-27 12:00:00', NULL),
(121, 20, 'paid', 12.00, '2026-03-27 18:00:00', '2026-03-27 18:02:00');

-- ======================================================
-- 5. 插入所有订单项数据 (1-21)
-- ======================================================
INSERT INTO order_items VALUES
(1, 101, 1001, 2, 19.99, 39.98),
(2, 101, 1002, 1, 9.99, 9.99),
(3, 102, 1002, 3, 9.99, 29.97),
(4, 104, 1003, 2, 49.99, 99.98),
(5, 105, 1004, 2, 25.99, 51.98),
(6, 106, 1005, 1, 35.50, 35.50),
(7, 107, 1006, 1, 89.00, 89.00),
(8, 108, 1007, 2, 42.75, 85.50),
(9, 109, 1008, 2, 12.49, 24.98),
(10, 110, 1009, 1, 18.20, 18.20),
(11, 111, 1010, 1, 64.90, 64.90),
(12, 112, 1011, 1, 199.00, 199.00),
(13, 113, 1012, 1, 45.00, 45.00),
(14, 114, 1013, 1, 299.99, 299.99),
(15, 114, 1014, 1, 29.50, 29.50),
(16, 115, 1014, 1, 29.50, 29.50),
(17, 116, 1015, 1, 450.00, 450.00),
(18, 117, 1016, 1, 24.99, 24.99),
(19, 117, 1017, 1, 55.00, 55.00),
(20, 118, 1017, 1, 55.00, 55.00),
(21, 119, 1018, 1, 32.00, 32.00);