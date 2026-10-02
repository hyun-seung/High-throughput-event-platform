CREATE TABLE IF NOT EXISTS client_event_contracts (
    client_id bigint PRIMARY KEY REFERENCES users(id),
    enabled boolean NOT NULL DEFAULT true,
    tps_limit integer NOT NULL CHECK (tps_limit > 0),
    quota_general bigint NOT NULL CHECK (quota_general > 0),
    quota_noti bigint NOT NULL CHECK (quota_noti > 0),
    quota_adv bigint NOT NULL CHECK (quota_adv > 0),
    quota_alert bigint NOT NULL CHECK (quota_alert > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);
