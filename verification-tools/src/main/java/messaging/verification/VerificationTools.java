package messaging.verification;

import java.nio.file.Path;

/** Entry point for messaging dashboards, Kafka metrics and local SDK diagnostics. */
public final class VerificationTools {
    private VerificationTools() { }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "dynamodb-benchmark".equals(args[0])) {
            DynamoDbBenchmark.run(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length == 1 && "collector".equals(args[0])) {
            MonitoringCollector.serve();
            return;
        }
        if (args.length == 1 && "dashboards".equals(args[0])) {
            DashboardValidation.run(Path.of(""));
            return;
        }
        throw new IllegalArgumentException("Usage: verification-tools <collector|dashboards|dynamodb-benchmark --endpoint URI --output PATH ...>");
    }
}
