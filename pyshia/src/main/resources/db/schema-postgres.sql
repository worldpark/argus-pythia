-- 운영 환경에서 ddl-auto=none 전환 시 수동 적용용 DDL
-- spring.sql.init.mode=never 기본 유지로 자동 실행되지 않음

CREATE TABLE IF NOT EXISTS jvm_metric_snapshot (
    id                      BIGSERIAL PRIMARY KEY,
    application             VARCHAR(128)              NOT NULL,
    instance                VARCHAR(128)              NOT NULL,
    collected_at            TIMESTAMP  NOT NULL,
    status                  VARCHAR(16)               NOT NULL,
    cpu_usage_percent       NUMERIC(19, 6),
    cpu_measured_at         TIMESTAMP,
    cpu_status              VARCHAR(16),
    cpu_missing_reason      VARCHAR(512),
    heap_usage_percent      NUMERIC(19, 6),
    old_gen_usage_percent   NUMERIC(19, 6),
    memory_measured_at      TIMESTAMP,
    memory_status           VARCHAR(16),
    memory_missing_reason   VARCHAR(512),
    gc_avg_duration_seconds NUMERIC(19, 6),
    gc_count                NUMERIC(19, 6),
    gc_measured_at          TIMESTAMP,
    gc_status               VARCHAR(16),
    gc_missing_reason       VARCHAR(512),
    thread_active_count     INTEGER,
    thread_peak_count       INTEGER,
    thread_daemon_count     INTEGER,
    thread_measured_at      TIMESTAMP,
    thread_status           VARCHAR(16),
    thread_missing_reason   VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS http_metric_snapshot (
    id                        BIGSERIAL PRIMARY KEY,
    application               VARCHAR(128)              NOT NULL,
    instance                  VARCHAR(128)              NOT NULL,
    collected_at              TIMESTAMP  NOT NULL,
    status                    VARCHAR(16)               NOT NULL,
    p99_measured_at           TIMESTAMP,
    p99_status                VARCHAR(16),
    p99_missing_reason        VARCHAR(512),
    rps_measured_at           TIMESTAMP,
    rps_status                VARCHAR(16),
    rps_missing_reason        VARCHAR(512),
    error_rate_measured_at    TIMESTAMP,
    error_rate_status         VARCHAR(16),
    error_rate_missing_reason VARCHAR(512)
);

-- snapshot_id FK는 양방향 @ManyToOne으로 자식 측에서 관리 (NOT NULL)
CREATE TABLE IF NOT EXISTS http_endpoint_metric_point (
    id          BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT                    NOT NULL REFERENCES http_metric_snapshot(id),
    kind        VARCHAR(16)               NOT NULL,
    endpoint    VARCHAR(512)              NOT NULL,
    value       NUMERIC(19, 6),
    measured_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hikari_metric_snapshot (
    id                      BIGSERIAL PRIMARY KEY,
    application             VARCHAR(128)              NOT NULL,
    instance                VARCHAR(128)              NOT NULL,
    collected_at            TIMESTAMP  NOT NULL,
    status                  VARCHAR(16)               NOT NULL,
    active_measured_at      TIMESTAMP,
    active_status           VARCHAR(16),
    active_missing_reason   VARCHAR(512),
    pending_measured_at     TIMESTAMP,
    pending_status          VARCHAR(16),
    pending_missing_reason  VARCHAR(512)
);

-- snapshot_id FK는 양방향 @ManyToOne으로 자식 측에서 관리 (NOT NULL)
CREATE TABLE IF NOT EXISTS hikari_pool_metric_point (
    id          BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT                    NOT NULL REFERENCES hikari_metric_snapshot(id),
    kind        VARCHAR(16)               NOT NULL,
    pool        VARCHAR(256),
    value       NUMERIC(19, 6),
    measured_at TIMESTAMP
);
