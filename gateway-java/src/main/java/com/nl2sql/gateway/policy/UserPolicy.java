package com.nl2sql.gateway.policy;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@TableName("user_policy")
public class UserPolicy {

    @TableId(type = IdType.INPUT)
    public String userTag;
    public String role;
    public String allowedDbIds;   // JSON 数组字符串
    public Integer qpsLimit;
    public Long dailyQuota;
    public Integer status;
    public java.time.LocalDateTime createdAt;
    public java.time.LocalDateTime updatedAt;
}
