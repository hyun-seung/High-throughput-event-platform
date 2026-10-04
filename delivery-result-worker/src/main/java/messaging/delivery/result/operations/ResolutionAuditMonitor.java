package messaging.delivery.result.operations;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;

/** Aggregate audit visibility without exposing customer IDs or polling DynamoDB. */
public final class ResolutionAuditMonitor {
    private final JdbcTemplate sql;
    private final Clock clock;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong oldestAgeSeconds = new AtomicLong();
    private final AtomicLong lastSuccessEpochSeconds = new AtomicLong();

    public ResolutionAuditMonitor(JdbcTemplate sql, MeterRegistry meters, Clock clock) {
        this.sql = sql; this.clock = clock;
        meters.gauge("delivery.resolution.pending", pending);
        meters.gauge("delivery.resolution.pending.oldest.age", oldestAgeSeconds);
        meters.gauge("delivery.resolution.audit.scan.last.success", lastSuccessEpochSeconds);
    }

    @Scheduled(fixedDelayString = "${resolution.audit.monitor.poll-ms:30000}", scheduler = "resolutionAuditMonitorScheduler")
    public void tick() {
        try {
            var snapshot = sql.queryForObject("""
                    SELECT count(*), COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)), 0)
                    FROM delivery_resolution_action WHERE status='PENDING'
                    """, (rs, row) -> new long[]{rs.getLong(1), Math.max(0, (long) rs.getDouble(2))});
            if (snapshot == null) throw new IllegalStateException("Resolution audit snapshot missing");
            pending.set(snapshot[0]);
            oldestAgeSeconds.set(snapshot[1]);
            lastSuccessEpochSeconds.set(clock.instant().getEpochSecond());
        } catch (RuntimeException failure) {
            // Keep the last counts; the last-success metric exposes a stale scan.
            LoggerFactory.getLogger(ResolutionAuditMonitor.class).warn("Resolution audit scan failed: failure={}",
                    failure.getClass().getSimpleName());
        }
    }
}
