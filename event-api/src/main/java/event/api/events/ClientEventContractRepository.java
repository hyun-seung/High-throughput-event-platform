package event.api.events;

import event.common.events.EventType;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class ClientEventContractRepository {
    private static final Duration CACHE_AGE = Duration.ofSeconds(30);
    private static final Duration MAX_STALE_AGE = Duration.ofMinutes(5);
    private final DataSource source;
    private final ConcurrentHashMap<Long, Cached> cache = new ConcurrentHashMap<>();

    public ClientEventContractRepository(DataSource source) { this.source = source; }

    public Contract get(long clientId) {
        Cached existing = cache.get(clientId);
        Instant now = Instant.now();
        if (existing != null && now.isBefore(existing.loadedAt.plus(CACHE_AGE))) return existing.contract;
        try (var connection = source.getConnection();
             var query = connection.prepareStatement("""
                     SELECT enabled, tps_limit, quota_general, quota_noti, quota_adv, quota_alert
                     FROM client_event_contracts WHERE client_id = ?
                     """)) {
            query.setLong(1, clientId);
            try (var rows = query.executeQuery()) {
                if (!rows.next() || !rows.getBoolean("enabled")) {
                    throw new EventAdmissionException(HttpStatus.FORBIDDEN, "Client event contract is inactive");
                }
                Map<EventType, Long> quotas = new EnumMap<>(EventType.class);
                quotas.put(EventType.GENERAL, rows.getLong("quota_general"));
                quotas.put(EventType.NOTI, rows.getLong("quota_noti"));
                quotas.put(EventType.ADV, rows.getLong("quota_adv"));
                quotas.put(EventType.ALERT, rows.getLong("quota_alert"));
                Contract contract = new Contract(rows.getInt("tps_limit"), Map.copyOf(quotas));
                cache.put(clientId, new Cached(contract, now));
                return contract;
            }
        } catch (SQLException failure) {
            if (existing != null && now.isBefore(existing.loadedAt.plus(MAX_STALE_AGE))) return existing.contract;
            throw new EventAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Client event contract could not be checked", failure);
        }
    }

    public record Contract(int tpsLimit, Map<EventType, Long> monthlyQuotas) {
        public long quota(EventType type) { return monthlyQuotas.get(type); }
    }
    private record Cached(Contract contract, Instant loadedAt) { }
}
