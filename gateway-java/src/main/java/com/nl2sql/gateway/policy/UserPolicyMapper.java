package com.nl2sql.gateway.policy;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface UserPolicyMapper extends BaseMapper<UserPolicy> {

    @Insert("""
            INSERT INTO user_policy (user_tag, role, allowed_db_ids, qps_limit, daily_quota, status)
            VALUES (#{p.userTag}, #{p.role}, #{p.allowedDbIds}, #{p.qpsLimit}, #{p.dailyQuota}, #{p.status})
            ON DUPLICATE KEY UPDATE role=VALUES(role), allowed_db_ids=VALUES(allowed_db_ids),
              qps_limit=VALUES(qps_limit), daily_quota=VALUES(daily_quota), status=VALUES(status)
            """)
    int upsert(@Param("p") UserPolicy p);
}
