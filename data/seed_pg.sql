-- NL2SQL Mini Agent — 电商 Demo 数据库种子脚本
-- 方言：PostgreSQL
-- 用法：psql -d <database> -f data/seed_pg.sql

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT UNIQUE NOT NULL,
    phone       TEXT,
    city        TEXT NOT NULL,
    registration_date TEXT NOT NULL
);

CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT
);

CREATE TABLE products (
    product_id     SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    description    TEXT,
    price          NUMERIC(10,2) NOT NULL CHECK(price > 0),
    category_id    INTEGER REFERENCES categories(category_id),
    stock_quantity INTEGER DEFAULT 0,
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TEXT NOT NULL
);

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INTEGER REFERENCES customers(customer_id),
    order_date   TEXT NOT NULL,
    status       TEXT NOT NULL CHECK(status IN ('pending','confirmed','shipped','delivered','cancelled','refunded')),
    total_amount NUMERIC(10,2) NOT NULL
);

CREATE TABLE order_items (
    item_id    SERIAL PRIMARY KEY,
    order_id   INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity   INTEGER NOT NULL CHECK(quantity > 0),
    unit_price NUMERIC(10,2) NOT NULL
);

CREATE TABLE reviews (
    review_id   SERIAL PRIMARY KEY,
    product_id  INTEGER REFERENCES products(product_id),
    customer_id INTEGER REFERENCES customers(customer_id),
    rating      INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
    comment     TEXT,
    created_at  TEXT NOT NULL
);

-- ============================================================
-- 种子数据
-- ============================================================

INSERT INTO customers VALUES
(1,  '张三',  'zhangsan@example.com',   '13800001001', '北京', '2025-09-15'),
(2,  '李四',  'lisi@example.com',       '13800001002', '上海', '2025-11-01'),
(3,  '王五',  'wangwu@example.com',     '13800001003', '广州', '2025-12-10'),
(4,  '赵六',  'zhaoliu@example.com',    '13800001004', '深圳', '2026-01-20'),
(5,  '孙七',  'sunqi@example.com',      '13800001005', '北京', '2026-02-14'),
(6,  '周八',  'zhouba@example.com',     '13800001006', '上海', '2026-03-01'),
(7,  '吴九',  'wujiu@example.com',      '13800001007', '杭州', '2026-03-15'),
(8,  '郑十',  'zhengshi@example.com',   '13800001008', '广州', '2026-04-01'),
(9,  '陈一',  'chenyi@example.com',     '13800001009', '北京', '2026-04-20'),
(10, '林二',  'liner@example.com',      '13800001010', '深圳', '2026-05-01');

INSERT INTO categories VALUES
(1, '手机数码', '手机、平板、智能穿戴设备'),
(2, '电脑办公', '笔记本、台式机、办公外设'),
(3, '家用电器', '空调、冰箱、洗衣机、厨房电器'),
(4, '服饰鞋包', '服装、鞋靴、箱包配饰'),
(5, '食品生鲜', '零食、饮料、生鲜水果'),
(6, '图书文娱', '图书、电子书、音乐影视');

INSERT INTO products VALUES
(1,  'iPhone 16 Pro',        '苹果旗舰手机 256GB',           8999.00,  1, 120, TRUE,  '2025-10-01'),
(2,  '华为 Mate 70',          '华为旗舰手机 512GB',           6999.00,  1, 85,  TRUE,  '2025-11-15'),
(3,  'MacBook Air M4',       '苹果轻薄本 13寸 16GB+512GB',  9499.00,  2, 40,  TRUE,  '2025-09-20'),
(4,  'ThinkPad X1 Carbon',   '联想商务本 14寸 32GB+1TB',    10999.00, 2, 25,  TRUE,  '2025-12-01'),
(5,  '机械键盘 K8 Pro',       '无线机械键盘 87键 红轴',       499.00,   2, 200, TRUE,  '2026-01-10'),
(6,  '格力空调 1.5匹',        '变频冷暖挂式空调',             3299.00,  3, 60,  TRUE,  '2025-08-01'),
(7,  '海尔冰箱 500L',         '对开门风冷无霜冰箱',            4299.00,  3, 30,  TRUE,  '2025-10-15'),
(8,  'Nike Air Max 270',     '男士运动鞋 透气缓震',           899.00,   4, 150, TRUE,  '2026-02-01'),
(9,  'Levi''s 501 牛仔裤',    '经典直筒牛仔裤 中蓝色',         599.00,   4, 100, TRUE,  '2026-03-01'),
(10, '三只松鼠坚果礼盒',      '每日坚果 30袋装 750g',          129.00,   5, 500, TRUE,  '2026-04-01'),
(11, '伊利纯牛奶 250ml*24',   '整箱装 纯牛奶',                 69.90,   5, 300, TRUE,  '2026-04-15'),
(12, '《三体》全集',          '刘慈欣科幻小说三部曲 精装版',    99.00,   6, 80,  TRUE,  '2025-06-01'),
(13, 'AirPods Pro 3',        '苹果主动降噪耳机 USB-C',        1799.00,  1, 90,  TRUE,  '2025-11-01'),
(14, '戴森吸尘器 V15',        '无线手持吸尘器 智能激光探测',    4990.00,  3, 20,  TRUE,  '2025-09-01'),
(15, '小米手环 9 Pro',        '智能手环 AMOLED屏 NFC版',       399.00,   1, 250, TRUE,  '2026-01-15');

INSERT INTO orders VALUES
(1,  1,  '2026-05-08', 'delivered', 8999.00),
(2,  2,  '2026-05-07', 'shipped',   9499.00),
(3,  3,  '2026-05-06', 'confirmed', 499.00),
(4,  4,  '2026-05-05', 'delivered', 4299.00),
(5,  5,  '2026-05-04', 'delivered', 899.00),
(6,  1,  '2026-05-03', 'delivered', 198.90),
(7,  6,  '2026-05-02', 'shipped',   6999.00),
(8,  7,  '2026-05-01', 'confirmed', 3299.00),
(9,  8,  '2026-04-28', 'delivered', 10999.00),
(10, 9,  '2026-04-25', 'delivered', 1799.00),
(11, 10,'2026-04-20', 'delivered', 599.00),
(12, 2,  '2026-04-18', 'delivered', 69.90),
(13, 3,  '2026-04-15', 'cancelled',  399.00),
(14, 4,  '2026-04-10', 'delivered', 4990.00),
(15, 5,  '2026-04-05', 'refunded',  3299.00),
(16, 1,  '2026-03-28', 'delivered', 449.00),
(17, 7,  '2026-03-20', 'delivered', 99.00),
(18, 8,  '2026-03-15', 'delivered', 899.00),
(19, 9,  '2026-03-01', 'delivered', 8999.00),
(20, 10,'2026-02-20', 'delivered', 129.00),
(21, 6,  '2026-05-08', 'pending',   69.90),
(22, 3,  '2026-05-08', 'pending',   1799.00),
(23, 4,  '2026-05-07', 'confirmed', 899.00),
(24, 2,  '2026-05-06', 'shipped',   499.00),
(25, 1,  '2026-05-09', 'pending',   99.00);

INSERT INTO order_items VALUES
(1,  1,  1,  1, 8999.00),
(2,  2,  3,  1, 9499.00),
(3,  3,  5,  1, 499.00),
(4,  4,  7,  1, 4299.00),
(5,  5,  8,  1, 899.00),
(6,  6,  10, 1, 129.00),
(7,  6,  11, 1, 69.90),
(8,  7,  2,  1, 6999.00),
(9,  8,  6,  1, 3299.00),
(10, 9,  4,  1, 10999.00),
(11, 10, 13, 1, 1799.00),
(12, 11, 9,  1, 599.00),
(13, 12, 11, 1, 69.90),
(14, 13, 15, 1, 399.00),
(15, 14, 14, 1, 4990.00),
(16, 15, 6,  1, 3299.00),
(17, 16, 15, 1, 399.00),
(18, 16, 10, 1, 50.00),
(19, 17, 12, 1, 99.00),
(20, 18, 8,  1, 899.00),
(21, 19, 1,  1, 8999.00),
(22, 20, 10, 1, 129.00),
(23, 21, 11, 1, 69.90),
(24, 22, 13, 1, 1799.00),
(25, 23, 8,  1, 899.00),
(26, 24, 5,  1, 499.00),
(27, 25, 12, 1, 99.00);

INSERT INTO reviews VALUES
(1,  1,  1,  5, '拍照效果惊艳，系统流畅度满分', '2026-04-01'),
(2,  1,  5,  4, '续航比上一代有提升',           '2026-04-05'),
(3,  2,  6,  5, '国产机皇名副其实',             '2026-03-20'),
(4,  3,  2,  5, '轻薄便携，M4芯片性能强劲',     '2026-03-15'),
(5,  3,  7,  4, '散热一般，高强度使用会降频',   '2026-04-10'),
(6,  5,  3,  5, '码字手感极佳，桌面利器',       '2026-02-20'),
(7,  8,  4,  4, '穿着舒适，尺码偏小建议买大一码','2026-03-05'),
(8,  8,  9,  2, '颜色和图片有差异',             '2026-04-01'),
(9,  10, 1,  3, '坚果不够脆，包装有破损',       '2026-02-15'),
(10, 12, 8,  5, '科幻迷必入，装帧精美',         '2026-01-10'),
(11, 12, 6,  5, '三体人表示很赞',               '2026-02-01'),
(12, 13, 4,  4, '降噪效果不错，佩戴舒适',       '2026-03-10'),
(13, 14, 7,  5, '吸力超强，激光探测很有用',     '2026-01-20'),
(14, 6,  8,  3, '制冷效果还行，噪音偏大',       '2025-10-15'),
(15, 15, 5,  5, '性价比极高，功能齐全',         '2026-02-28');

-- Reset sequences to after the inserted IDs
SELECT setval('customers_customer_id_seq', 10);
SELECT setval('categories_category_id_seq', 6);
SELECT setval('products_product_id_seq', 15);
SELECT setval('orders_order_id_seq', 25);
SELECT setval('order_items_item_id_seq', 27);
SELECT setval('reviews_review_id_seq', 15);
