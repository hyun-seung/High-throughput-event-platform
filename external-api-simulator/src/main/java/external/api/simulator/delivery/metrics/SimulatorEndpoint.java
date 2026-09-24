package external.api.simulator.delivery.metrics;

import external.api.simulator.delivery.service.SimulatorLedger;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.stereotype.Component;

import java.util.Map;

/** Measurement-only endpoint on the loopback management port; never used by dispatch decisions. */
@Component
@Endpoint(id = "simulator")
public class SimulatorEndpoint {
    private final SimulatorLedger ledger;
    public SimulatorEndpoint(SimulatorLedger ledger) { this.ledger = ledger; }
    @ReadOperation
    public Map<String, Object> summary() { return ledger.summary(); }
    @ReadOperation
    public Map<String, Long> counts(@Selector String key) { return ledger.counts(key); }
}
