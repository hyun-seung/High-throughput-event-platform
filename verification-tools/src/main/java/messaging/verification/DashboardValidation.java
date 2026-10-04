package messaging.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Validate committed Grafana provisioning files before Grafana loads them. */
final class DashboardValidation {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final List<String> NAMES = List.of("overview", "services", "providers", "customers", "errors", "infra", "trace");
    private static final Set<String> SOURCES = Set.of("platform-prometheus", "platform-loki");

    private DashboardValidation() {}

    static void run(Path root) throws Exception {
        var errors = validate(root);
        if (!errors.isEmpty()) throw new IllegalStateException("Dashboard validation failed:\n" + String.join("\n", errors));
        System.out.println("PASS: " + NAMES.size() + " provisioned Grafana dashboards");
    }

    static List<String> validate(Path root) throws Exception {
        Path directory = root.resolve("monitoring/grafana/dashboards");
        var errors = new ArrayList<String>();
        try (var files = Files.list(directory)) {
            var actual = files.filter(path -> path.getFileName().toString().endsWith(".json"))
                    .map(path -> path.getFileName().toString()).sorted().toList();
            var expected = NAMES.stream().map(name -> name + ".json").sorted().toList();
            if (!actual.equals(expected)) errors.add("Dashboard file set mismatch: " + actual);
        }
        for (String name : NAMES) {
            Path file = directory.resolve(name + ".json");
            if (!Files.exists(file)) continue;
            JsonNode board;
            try {
                board = JSON.readTree(Files.readString(file));
            } catch (Exception failure) {
                errors.add(name + ": invalid JSON: " + failure.getMessage());
                continue;
            }
            if (!("delivery-" + name).equals(board.path("uid").asText())) errors.add(name + ": UID mismatch");
            if (board.path("title").asText().isBlank()) errors.add(name + ": missing title");
            if (board.path("editable").asBoolean(true)) errors.add(name + ": dashboard must be read-only in Grafana");
            var panels = board.path("panels");
            if (!panels.isArray() || panels.isEmpty()) {
                errors.add(name + ": no panels");
                continue;
            }
            var ids = new HashSet<Integer>();
            for (JsonNode panel : panels) {
                int id = panel.path("id").asInt(-1);
                String label = name + "/" + id;
                if (id <= 0 || !ids.add(id)) errors.add(label + ": invalid or repeated panel ID");
                if (panel.path("title").asText().isBlank()) errors.add(label + ": missing panel title");
                JsonNode grid = panel.path("gridPos");
                int x = grid.path("x").asInt(-1), width = grid.path("w").asInt(-1);
                if (x < 0 || width <= 0 || x + width > 24 || grid.path("h").asInt(0) <= 0)
                    errors.add(label + ": panel outside 24-column grid");
                var targets = panel.path("targets");
                if (panel.path("type").asText().equals("text")) continue;
                String source = panel.path("datasource").path("uid").asText();
                if (!SOURCES.contains(source)) errors.add(label + ": unknown datasource " + source);
                if (!targets.isArray() || targets.isEmpty()) errors.add(label + ": no query target");
                else for (JsonNode target : targets) {
                    if (target.path("expr").asText().isBlank()) errors.add(label + ": empty query");
                }
            }
            for (JsonNode link : board.path("links")) {
                String url = link.path("url").asText();
                if (!NAMES.stream().anyMatch(other -> url.equals("/d/delivery-" + other)))
                    errors.add(name + ": unknown dashboard link " + url);
            }
        }
        return errors;
    }
}
