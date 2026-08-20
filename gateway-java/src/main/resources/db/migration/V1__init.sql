CREATE TABLE user_policy (
  user_tag       VARCHAR(64) PRIMARY KEY,
  role           VARCHAR(16) NOT NULL DEFAULT 'user',
  allowed_db_ids JSON NOT NULL,
  qps_limit      INT NOT NULL DEFAULT 10,
  daily_quota    BIGINT NOT NULL DEFAULT 1000,
  status         TINYINT NOT NULL DEFAULT 1,
  created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);

CREATE TABLE task_record (
  task_id          VARCHAR(64) PRIMARY KEY,
  user_tag         VARCHAR(64) NOT NULL,
  db_id            VARCHAR(128),
  question         TEXT,
  status           VARCHAR(16),
  sql_text         MEDIUMTEXT,
  row_count        INT,
  retry_count      TINYINT DEFAULT 0,
  error            TEXT,
  cache_hit        TINYINT DEFAULT 0,
  submitted_at     DATETIME(3),
  started_at       DATETIME(3),
  finished_at      DATETIME(3),
  total_latency_ms INT,
  node_timings     JSON,
  token_usage      JSON,
  created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  INDEX idx_user_time (user_tag, submitted_at),
  INDEX idx_status_time (status, submitted_at),
  INDEX idx_time (submitted_at)
);

INSERT INTO user_policy (user_tag, role, allowed_db_ids, qps_limit, daily_quota, status) VALUES
  ('local_user',  'user',  '["*"]', 100, 100000, 1),
  ('bench_admin', 'admin', '["*"]', 100, 100000, 1);
