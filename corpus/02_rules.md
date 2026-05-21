# 指标口径与查询规则

## 有效订单
- 定义：status 不等于 'cancelled' 且不等于 'refunded' 的订单
- SQL 写法：WHERE status NOT IN ('cancelled', 'refunded') 或 WHERE status NOT IN ('cancelled','refunded')
- 几乎所有业务统计（销售额、销量）都应当只算有效订单

## 销售额（GMV）
- 定义：有效订单的 total_amount 之和
- SQL 写法：SELECT SUM(total_amount) FROM orders WHERE status NOT IN ('cancelled', 'refunded')

## 商品销量
- 定义：有效订单中该商品的购买数量之和
- 需要 JOIN order_items + orders，筛选有效订单后对 quantity 求和
- SQL 写法：SUM(oi.quantity) ... JOIN orders o ON ... WHERE o.status NOT IN ('cancelled','refunded')

## 时间表达
- "过去7天" 或 "最近7天"：WHERE order_date >= date('now', '-7 days')
- "过去30天" 或 "最近一个月"：WHERE order_date >= date('now', '-30 days')
- "本月"：WHERE order_date >= date('now', 'start of month')
- "上个月"：WHERE order_date >= date('now', 'start of month', '-1 month') AND order_date < date('now', 'start of month')
- SQLite 使用 date() 函数，不区分大小写

## 评分
- rating 是 1-5 的整数
- 平均评分用 AVG(rating)，结果为小数
- "高评分"、"评分大于等于4"：HAVING AVG(rating) >= 4

## 价格比较
- products.price 单位是元，直接数值比较
- "大于1000元"：WHERE price > 1000
- "价格区间"：WHERE price BETWEEN 100 AND 500

## 数量统计
- "每个品类有多少商品"：COUNT(p.product_id) GROUP BY c.name
- "每个城市有多少客户"：COUNT(*) GROUP BY city
- 注意：COUNT 对象用具体列名（如 product_id）而非 *，避免把不存在的行也算进去

## 默认 LIMIT
- 除非用户明确要求全部数据，否则自动加 LIMIT 200
- 用户说"前N个"、"TOP N"时 LIMIT 使用用户指定的 N
