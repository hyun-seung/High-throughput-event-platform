-- Operational lookup of exhausted batches; no extra application write on the delivery path.
CREATE INDEX customer_notification_exhausted_lookup ON customer_notification_batch(tenant_id,batch_id)
    WHERE status='EXHAUSTED';
