package com.nl2sql.gateway.recorder;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@TableName("task_record")
public class TaskRecord {

    @TableId(type = IdType.INPUT)
    public String taskId;
    public String userTag;
    public String dbId;
    public String question;
    public String status;
    public String sqlText;
    public Integer rowCount;
    public Integer retryCount;
    public String error;
    public Integer cacheHit;
    public java.time.LocalDateTime submittedAt;
    public java.time.LocalDateTime startedAt;
    public java.time.LocalDateTime finishedAt;
    public Integer totalLatencyMs;
    public String nodeTimings;    // JSON
    public String tokenUsage;     // JSON
}
