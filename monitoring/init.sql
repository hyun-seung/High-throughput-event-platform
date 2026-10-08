INSERT INTO delivery_results.client_message_contracts
    (client_id, enabled, tps_limit, quota_general, quota_noti, quota_adv, quota_alert)
SELECT id, true, 2000, 1000000, 1000000, 1000000, 1000000
FROM public.users WHERE username = 'local-user'
ON CONFLICT (client_id) DO NOTHING;
