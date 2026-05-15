\connect bigdata

CREATE TABLE IF NOT EXISTS predictions_monitoring (
    zone_id                 INTEGER      NOT NULL,
    -- window_end = event_time từ dữ liệu lịch sử (2025)
    -- KHÔNG phải wall clock — hệ thống đang replay x60
    window_end              TIMESTAMPTZ  NOT NULL,
    predicted_class         SMALLINT     NOT NULL,
    pred_confidence         REAL         NOT NULL,
    used_model              VARCHAR(20)  NOT NULL,
    model_version           VARCHAR(20),
    -- predicted_at = window_end (event_time), xem lý do trên
    predicted_at            TIMESTAMPTZ  NOT NULL,
    shadow_predicted_class  SMALLINT,
    proba_0  REAL, proba_1  REAL, proba_2  REAL,
    proba_3  REAL, proba_4  REAL, proba_5  REAL,
    shadow_proba_0  REAL, shadow_proba_1  REAL, shadow_proba_2  REAL,
    shadow_proba_3  REAL, shadow_proba_4  REAL, shadow_proba_5  REAL,
    PRIMARY KEY (zone_id, window_end)
);

-- Grafana time series: ORDER BY predicted_at
CREATE INDEX IF NOT EXISTS idx_pred_predicted_at
    ON predictions_monitoring (predicted_at DESC);
-- Latest slot query: MAX(window_end)
CREATE INDEX IF NOT EXISTS idx_pred_window_end
    ON predictions_monitoring (window_end DESC);
-- Per-zone panel: WHERE zone_id = $zone
CREATE INDEX IF NOT EXISTS idx_pred_zone_time
    ON predictions_monitoring (zone_id, window_end DESC);
-- Model split panel: WHERE used_model = 'model_a'
CREATE INDEX IF NOT EXISTS idx_pred_used_model
    ON predictions_monitoring (used_model, window_end DESC);