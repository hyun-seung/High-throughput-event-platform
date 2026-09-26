import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Classify runtime errors in a saved broker/application log read from stdin. */
public class LogHealth {
    private static final Map<String, Pattern> SIGNALS = new LinkedHashMap<>();
    static {
        SIGNALS.put("negative_histogram", Pattern.compile("Histogram recorded value cannot be negative", Pattern.CASE_INSENSITIVE));
        SIGNALS.put("coordinator_out_of_sync", Pattern.compile("state machine of the coordinator .*out of sync", Pattern.CASE_INSENSITIVE));
        SIGNALS.put("offset_commit_failed", Pattern.compile("Offset commit failed", Pattern.CASE_INSENSITIVE));
        SIGNALS.put("error_level", Pattern.compile("\\bERROR\\b"));
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("self-test")) {
            selfTest();
            return;
        }
        if (args.length != 0) throw new IllegalArgumentException("Usage: java scripts/testing/LogHealth.java [self-test]");
        var counts = new LinkedHashMap<String, Integer>();
        SIGNALS.keySet().forEach(name -> counts.put(name, 0));
        var samples = new ArrayList<String>();
        try (var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            for (String line; (line = input.readLine()) != null;) {
                boolean matched = false;
                for (var signal : SIGNALS.entrySet()) {
                    if (signal.getValue().matcher(line).find()) {
                        counts.merge(signal.getKey(), 1, Integer::sum);
                        matched = true;
                    }
                }
                if (matched && samples.size() < 20) samples.add(line);
            }
        }
        boolean passed = counts.values().stream().allMatch(count -> count == 0);
        var json = new StringBuilder("{\"pass\":").append(passed).append(",\"signals\":{");
        boolean first = true;
        for (var entry : counts.entrySet()) {
            if (!first) json.append(',');
            first = false;
            json.append(quote(entry.getKey())).append(':').append(entry.getValue());
        }
        json.append("},\"sampleLines\":[");
        for (int i = 0; i < samples.size(); i++) {
            if (i > 0) json.append(',');
            json.append(quote(samples.get(i)));
        }
        System.out.println(json.append("]}"));
    }

    private static String quote(String value) {
        var escaped = new StringBuilder("\"");
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) escaped.append(String.format("\\u%04x", (int) character));
                    else escaped.append(character);
                }
            }
        }
        return escaped.append('"').toString();
    }

    private static void selfTest() {
        check("[2026-09-24] ERROR [GroupCoordinator id=1] Writing records failed: Histogram recorded value cannot be negative.",
                "negative_histogram", true);
        check("WARN ConsumerCoordinator : Offset commit failed on partition p-0", "offset_commit_failed", false);
        check("ERROR The state machine of the coordinator offsets-8 is out of sync", "coordinator_out_of_sync", true);
        check("INFO Delivery stage observed", null, false);
        System.out.println("PASS LogHealth self-test");
    }

    private static void check(String line, String expected, boolean errorLevel) {
        for (var signal : SIGNALS.entrySet()) {
            boolean shouldMatch = signal.getKey().equals(expected) || signal.getKey().equals("error_level") && errorLevel;
            if (signal.getValue().matcher(line).find() != shouldMatch) {
                throw new AssertionError("Unexpected " + signal.getKey() + " classification: " + line);
            }
        }
    }
}
