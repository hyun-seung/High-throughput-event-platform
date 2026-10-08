package messaging.verification;

import java.nio.file.Path;

/** Entry point for the active messaging dashboards and Kafka metrics collector. */
public final class VerificationTools {
    private VerificationTools() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "collector".equals(args[0])) {
            MonitoringCollector.serve();
            return;
        }
        if (args.length == 1 && "dashboards".equals(args[0])) {
            DashboardValidation.run(Path.of(""));
            return;
        }
        throw new IllegalArgumentException("Usage: verification-tools <collector|dashboards>");
    }
}
