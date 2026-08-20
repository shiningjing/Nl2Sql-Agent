package com.nl2sql.gateway.task;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Redis task:{id} 状态 JSON——字段名与 infrastructure/task_store.py 逐字一致，null 必须保留。 */
@JsonInclude(JsonInclude.Include.ALWAYS)
public class TaskState {

    public String task_id;
    public String status;
    public String question;
    public String db_id;
    public String database_url;
    public int progress;
    public String node;
    public String sql;
    public Object exec_result;
    public Map<String, Object> token_usage = new LinkedHashMap<>();
    public Map<String, Object> node_timings = new LinkedHashMap<>();
    public int retry_count;
    public Object error;
    public String created_at;
    public String updated_at;

    /** Worker 在 feedback 后写入的会话轮次；Python 端读取时缺省 []，Java 建任务时不依赖它，
     *  反序列化兼容 List 与缺省。 */
    public Object conversation_turns = new LinkedHashMap<String, Object>();

    /** status 端点响应体：14 字段，与 pydantic TaskStatusResponse(**state) 的过滤一致。 */
    public Map<String, Object> toStatusResponse() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("task_id", task_id);
        m.put("status", status);
        m.put("question", question);
        m.put("db_id", db_id);
        m.put("progress", progress);
        m.put("node", node);
        m.put("sql", sql);
        m.put("exec_result", exec_result);
        m.put("token_usage", token_usage);
        m.put("node_timings", node_timings);
        m.put("error", error);
        m.put("retry_count", retry_count);
        m.put("created_at", created_at);
        m.put("updated_at", updated_at);
        return m;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> turns() {
        if (conversation_turns instanceof List<?> l) {
            return (List<Map<String, Object>>) (Object) l;
        }
        return List.of();
    }
}
