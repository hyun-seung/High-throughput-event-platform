-- Local CDC source tables. Flyway V13/V14 use the same definitions in delivery_results.
CREATE SCHEMA IF NOT EXISTS delivery_results;
CREATE TABLE IF NOT EXISTS delivery_results.client_event_contracts (
    client_id bigint PRIMARY KEY REFERENCES public.users(id),
    enabled boolean NOT NULL DEFAULT true,
    tps_limit integer NOT NULL CHECK (tps_limit > 0),
    quota_general bigint NOT NULL CHECK (quota_general > 0),
    quota_noti bigint NOT NULL CHECK (quota_noti > 0),
    quota_adv bigint NOT NULL CHECK (quota_adv > 0),
    quota_alert bigint NOT NULL CHECK (quota_alert > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS delivery_results.phone_carrier_mappings (
    phone_number varchar(11) PRIMARY KEY CHECK (phone_number ~ '^010[0-9]{8}$'),
    carrier varchar(3) NOT NULL CHECK (carrier IN ('SKT', 'KT', 'LGU')),
    updated_at timestamptz NOT NULL DEFAULT now()
);
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'event_reference_pub') THEN
        CREATE PUBLICATION event_reference_pub FOR TABLE
            delivery_results.client_event_contracts,
            delivery_results.phone_carrier_mappings;
    ELSE
        ALTER PUBLICATION event_reference_pub SET TABLE
            delivery_results.client_event_contracts,
            delivery_results.phone_carrier_mappings;
    END IF;
END $$;
