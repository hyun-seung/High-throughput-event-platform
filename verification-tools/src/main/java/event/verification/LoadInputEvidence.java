package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Checks the k6 request log before either load runner evaluates downstream effects. */
final class LoadInputEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private LoadInputEvidence() {}

    static void runManifest(Path path) throws IOException {
        System.out.println(JSON.writeValueAsString(readManifest(path)));
    }

    static void runComplete(String[] args) {
        if (args.length != 5) throw new IllegalArgumentException("input-complete requires planned started answered iterations dropped");
        System.out.println(completeInput(Long.parseLong(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]),
                Long.parseLong(args[3]), Long.parseLong(args[4])));
    }

    static Map<String, Map<String, JsonNode>> readManifest(Path path) throws IOException {
        Map<String, JsonNode> starts = new LinkedHashMap<>();
        Map<String, JsonNode> results = new LinkedHashMap<>();
        try (var lines = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            for (String line; (line = lines.readLine()) != null;) {
                JsonNode row = JSON.readTree(line);
                JsonNode iteration = row.path("iteration");
                if (!iteration.isIntegralNumber()) throw new IllegalArgumentException("Missing or invalid manifest iteration");
                String id = iteration.asText();
                Map<String, JsonNode> target = switch (row.path("kind").asText()) {
                    case "start" -> starts;
                    case "result" -> results;
                    default -> throw new IllegalArgumentException("Unknown or repeated manifest event");
                };
                if (target.putIfAbsent(id, row) != null)
                    throw new IllegalArgumentException("Unknown or repeated manifest event");
            }
        }
        if (starts.isEmpty() || !starts.keySet().containsAll(results.keySet()))
            throw new IllegalArgumentException("Missing starts or orphaned responses in manifest");
        for (var result : results.entrySet()) {
            String startKey = starts.get(result.getKey()).path("key").asText();
            String resultKey = result.getValue().path("key").asText();
            if (!startKey.equals(resultKey)) throw new IllegalArgumentException("Manifest key mismatch");
        }
        Map<String, Map<String, JsonNode>> output = new LinkedHashMap<>();
        output.put("starts", starts);
        output.put("results", results);
        return output;
    }

    static boolean completeInput(long planned, long started, long answered, long iterations, long dropped) {
        // An arrival exactly at the duration boundary can add one iteration in k6.
        return planned <= started && started <= planned + 1 && started == answered
                && answered == iterations && dropped == 0;
    }
}
