package com.nl2sql.gateway.recorder;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface TaskRecordMapper extends BaseMapper<TaskRecord> {

    @Insert("""
            INSERT INTO task_record (task_id, user_tag, db_id, question, status, sql_text, row_count,
              retry_count, error, cache_hit, submitted_at, started_at, finished_at, total_latency_ms,
              node_timings, token_usage)
            VALUES (#{r.taskId}, #{r.userTag}, #{r.dbId}, #{r.question}, #{r.status}, #{r.sqlText},
              #{r.rowCount}, #{r.retryCount}, #{r.error}, #{r.cacheHit}, #{r.submittedAt}, #{r.startedAt},
              #{r.finishedAt}, #{r.totalLatencyMs}, #{r.nodeTimings}, #{r.tokenUsage})
            ON DUPLICATE KEY UPDATE status=VALUES(status), sql_text=VALUES(sql_text),
              row_count=VALUES(row_count), retry_count=VALUES(retry_count), error=VALUES(error),
              cache_hit=VALUES(cache_hit), finished_at=VALUES(finished_at),
              total_latency_ms=VALUES(total_latency_ms), node_timings=VALUES(node_timings),
              token_usage=VALUES(token_usage)
            """)
    int upsert(@Param("r") TaskRecord r);
}
