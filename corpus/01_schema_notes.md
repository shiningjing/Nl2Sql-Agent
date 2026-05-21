# 表级说明

## customers 客户表
- 共 10 条记录
- city 为中文存储（如"北京"），查询时需用中文值
- registration_date 格式为 YYYY-MM-DD，如 '2025-09-15'

## categories 品类表
- 共 6 条记录
- name 为中文：手机数码、电脑办公、家用电器、服饰鞋包、食品生鲜、图书文娱
- 商品表通过 category_id 外键关联

## products 商品表
- 共 15 条记录，分布在 6 个品类中
- price 为小数，单位元，如 8999.00 表示 8999 元
- stock_quantity 为整数，表示当前库存量
- is_active = 0 表示商品已下架，默认查询应过滤 is_active = 1
- 手机数码品类下有 4 个商品：iPhone、华为、AirPods、小米手环
- 电脑办公品类下有 3 个商品：MacBook、ThinkPad、机械键盘

## orders 订单表
- 共 25 条记录
- order_date 格式为 YYYY-MM-DD，直接与 date('now') 返回值比较
- status 为英文小写字符串
- total_amount 为该订单总金额，单位元

## order_items 订单明细表
- 记录了每个订单中每个商品的购买数量(quantity)和单价(unit_price)
- quantity 为整数，unit_price 为小数
- 统计销量时应当用 SUM(quantity)，并只计入有效订单（排除 cancelled 和 refunded）

## reviews 评价表
- 共 15 条记录
- rating 为 1-5 的整数
- 一个商品可以有多条评价，用 AVG(rating) 计算平均分
