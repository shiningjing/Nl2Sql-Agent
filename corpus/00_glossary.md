# 业务名词词典

## 客户
- 对应表：customers
- 主键：customer_id
- 关键字段：name（姓名）、city（城市）、email（邮箱）、registration_date（注册日期）
- 城市取值范围：北京、上海、广州、深圳、杭州

## 商品
- 对应表：products
- 主键：product_id
- 关键字段：name（商品名）、price（价格，单位元）、category_id（所属品类）、stock_quantity（库存量）、is_active（1=在售，0=下架）
- 价格均为人民币元，可直接用于数值比较

## 品类
- 对应表：categories
- 主键：category_id
- 品类值：手机数码、电脑办公、家用电器、服饰鞋包、食品生鲜、图书文娱
- 商品通过 category_id 关联到品类

## 订单
- 对应表：orders
- 主键：order_id
- 关键字段：customer_id（下单客户）、order_date（日期，格式 YYYY-MM-DD）、status（状态）、total_amount（金额，单位元）

## 订单状态
- pending：待支付
- confirmed：已确认
- shipped：已发货
- delivered：已完成
- cancelled：已取消
- refunded：已退款
- 状态值全部为英文小写，查询时需用英文匹配

## 订单明细
- 对应表：order_items
- 每个订单行记录一个商品的数量和单价
- 一个订单可以有多条明细（多商品订单）
- quantity：购买数量，unit_price：下单时的单价

## 评价
- 对应表：reviews
- rating：评分，整数 1-5
- comment：评价文本
- created_at：评价日期
