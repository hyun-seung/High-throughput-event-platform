package external.api.simulator.customer;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
@Endpoint(id = "customerResults")
@ConditionalOnProperty(name = "simulator.customer-results.enabled", havingValue = "true")
public class CustomerResultSimulatorEndpoint {
    private final CustomerResultSimulator simulator;
    public CustomerResultSimulatorEndpoint(CustomerResultSimulator simulator) { this.simulator = simulator; }
    @ReadOperation public Map<String, Long> summary() { return simulator.summary(); }
}
