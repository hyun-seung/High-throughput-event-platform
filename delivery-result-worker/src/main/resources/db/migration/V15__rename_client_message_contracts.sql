DO $$
BEGIN
    IF to_regclass('delivery_results.client_message_contracts') IS NULL THEN
        ALTER TABLE delivery_results.client_event_contracts RENAME TO client_message_contracts;
    ELSIF to_regclass('delivery_results.client_event_contracts') IS NOT NULL THEN
        INSERT INTO delivery_results.client_message_contracts
            (client_id, enabled, tps_limit, quota_general, quota_noti, quota_adv, quota_alert, updated_at)
        SELECT client_id, enabled, tps_limit, quota_general, quota_noti, quota_adv, quota_alert, updated_at
        FROM delivery_results.client_event_contracts
        ON CONFLICT (client_id) DO NOTHING;
    END IF;
END $$;
