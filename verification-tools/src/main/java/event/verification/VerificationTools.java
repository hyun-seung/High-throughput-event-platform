package event.verification;

import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public final class VerificationTools {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final DateTimeFormatter UTC_MICROS = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'+00:00'").withZone(ZoneOffset.UTC);
    private static final Pattern LABEL = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"([^\"\\\\]*)\"");
    private static final Map<String, List<String>> STAGES = Map.of(
            "api", List.of("api_publish"),
            "ingress", List.of("ingress_store", "ingress_publish"),
            "dispatch", List.of("dispatch_claim", "dispatch_http", "dispatch_store"));
    private static final List<String> APPS = List.of("api", "ingress", "dispatch");

    private VerificationTools() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("stall")) {
            System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(analyzeStall(Path.of(args[1]))));
            return;
        }
        if (args.length == 1 && args[0].equals("monitoring")) {
            MonitoringVerification.run(Path.of(""), "http://127.0.0.1:13000", "http://127.0.0.1:19099");
            return;
        }
        if (args.length == 4 && args[0].equals("monitoring")) {
            MonitoringVerification.run(Path.of(args[1]), args[2], args[3]);
            return;
        }
        if (args.length >= 1 && args[0].equals("demo")) {
            MonitoringDemo.run(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length == 1 && args[0].equals("dashboards")) {
            DashboardValidation.run(Path.of(""));
            return;
        }
        if (args.length >= 1 && args[0].equals("local-smoke")) {
            LocalSmoke.run(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length >= 1 && args[0].equals("lifecycle-index")) {
            LifecycleIndexMigration.run(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length >= 1 && args[0].equals("split-table")) {
            SplitTableMigration.run(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        throw new IllegalArgumentException("Usage: java -jar verification-tools/target/verification-tools-1.0-SNAPSHOT.jar <stall directory|monitoring [root grafana-url prometheus-url]|demo [--rate N] [--seconds N] [--errors]|dashboards|local-smoke [--metrics]|lifecycle-index [options]|split-table [options]>");
    }

    static Map<String, Object> analyzeStall(Path directory) throws IOException {
        var starts = new ArrayList<Double>();
        try (var input = Files.newBufferedReader(directory.resolve("requests.jsonl"), StandardCharsets.UTF_8)) {
            for (String line; (line = input.readLine()) != null;) {
                var row = JSON.readTree(line);
                if (row.get("kind").asText().equals("start")) starts.add(row.get("started").asDouble() / 1000);
            }
        }
        if (starts.isEmpty()) throw new IllegalArgumentException("No request starts in " + directory);
        double base = starts.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
        double end = starts.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
        var samples = new ArrayList<tools.jackson.databind.JsonNode>();
        try (var input = Files.newBufferedReader(directory.resolve("samples.jsonl"), StandardCharsets.UTF_8)) {
            for (String line; (line = input.readLine()) != null;) samples.add(JSON.readTree(line));
        }

        var report = new LinkedHashMap<String, Object>();
        report.put("inputStartUtc", UTC_MICROS.format(Instant.ofEpochMilli(Math.round(base * 1000))));
        report.put("source", directory.toString());
        var observations = new LinkedHashMap<String, Object>();
        report.put("auditObservations", observations);
        var intervals = new ArrayList<Map<String, Object>>();
        report.put("intervals", intervals);

        for (String app : APPS) {
            var times = new ArrayList<Double>();
            for (String line : gzipLines(directory.resolve(app + "-runtime.log.gz"))) {
                if (line.contains("delivery.audit")) {
                    String timestamp = line.split("\\s+", 2)[0];
                    var instant = OffsetDateTime.parse(timestamp).toInstant();
                    times.add(instant.getEpochSecond() + instant.getNano() / 1_000_000_000d);
                }
            }
            times.sort(Double::compareTo);
            var bins = new TreeMap<Integer, Integer>();
            var gaps = new ArrayList<Gap>();
            for (int index = 0; index < times.size(); index++) {
                double stamp = times.get(index);
                if (base <= stamp && stamp <= end) bins.merge((int) Math.floor((stamp - base) / 10) * 10, 1, Integer::sum);
                if (index > 0) {
                    double left = times.get(index - 1);
                    if (base <= left && left < stamp && stamp <= end) gaps.add(new Gap(stamp - left, left - base));
                }
            }
            gaps.sort(Comparator.comparingDouble(Gap::seconds).thenComparingDouble(Gap::startSeconds).reversed());
            var largest = new ArrayList<Map<String, Double>>();
            for (int index = 0; index < Math.min(5, gaps.size()); index++) {
                var gap = gaps.get(index);
                largest.add(Map.of("seconds", gap.seconds(), "startSeconds", gap.startSeconds()));
            }
            observations.put(app, Map.of("countsByInputRelative10s", bins, "largestGaps", largest));
        }

        for (int index = 1; index < samples.size(); index++) {
            var row = new LinkedHashMap<String, Object>();
            row.put("startSeconds", rounded(samples.get(index - 1).get("time").asDouble() - base));
            row.put("endSeconds", rounded(samples.get(index).get("time").asDouble() - base));
            row.put("lag", samples.get(index).get("lag"));
            var means = new LinkedHashMap<String, Double>();
            var gc = new LinkedHashMap<String, Double>();
            row.put("stageMeanMs", means);
            row.put("gcPauseSumMs", gc);
            for (String app : APPS) {
                String before = gzipText(directory.resolve("%04d-%s.prom.gz".formatted(index - 1, app)));
                String after = gzipText(directory.resolve("%04d-%s.prom.gz".formatted(index, app)));
                for (String stage : STAGES.get(app)) {
                    double count = metric(after, "delivery_stage_duration_seconds_count", stage)
                            - metric(before, "delivery_stage_duration_seconds_count", stage);
                    double total = metric(after, "delivery_stage_duration_seconds_sum", stage)
                            - metric(before, "delivery_stage_duration_seconds_sum", stage);
                    if (count < 0 || total < 0) throw new IllegalArgumentException("Metric counter reset: cannot compare this interval");
                    means.put(stage, count == 0 ? null : rounded(total / count * 1000));
                }
                gc.put(app, rounded(1000 * (metric(after, "jvm_gc_pause_seconds_sum", null)
                        - metric(before, "jvm_gc_pause_seconds_sum", null))));
            }
            intervals.add(row);
        }
        return report;
    }

    private record Gap(double seconds, double startSeconds) {}

    private static double rounded(double number) {
        return java.math.BigDecimal.valueOf(number).setScale(3, java.math.RoundingMode.HALF_EVEN).doubleValue();
    }

    private static double metric(String text, String name, String stage) {
        double total = 0;
        for (String line : text.split("\\R")) {
            if (!line.startsWith(name + "{") && !line.startsWith(name + " ")) continue;
            if (stage != null) {
                var labels = LABEL.matcher(line);
                boolean found = false;
                while (labels.find()) if (labels.group(1).equals("stage") && labels.group(2).equals(stage)) found = true;
                if (!found) continue;
            }
            total += Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
        }
        return total;
    }

    private static List<String> gzipLines(Path path) throws IOException {
        try (var stream = new GZIPInputStream(Files.newInputStream(path));
             var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }

    private static String gzipText(Path path) throws IOException {
        try (var stream = new GZIPInputStream(Files.newInputStream(path))) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
