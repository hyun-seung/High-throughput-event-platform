package messaging.verification;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DashboardValidationTest {
    @Test
    void rejectsBrokenProvisioningFiles(@TempDir Path root) throws Exception {
        Path directory = root.resolve("monitoring/grafana/dashboards");
        Files.createDirectories(directory);
        for (String name : List.of("overview", "services", "providers", "customers", "errors", "infra", "trace")) {
            Files.writeString(directory.resolve(name + ".json"), """
                    {"uid":"messaging-%s","title":"%s","editable":false,
                     "panels":[{"id":1,"title":"Requests","type":"timeseries",
                       "gridPos":{"x":0,"y":0,"w":12,"h":8},
                       "datasource":{"uid":"platform-prometheus"},"targets":[{"expr":"up"}]}],
                     "links":[{"url":"/d/messaging-overview"}]}
                    """.formatted(name, name));
        }
        assertTrue(DashboardValidation.validate(root).isEmpty());
        Path overview = directory.resolve("overview.json");
        Files.writeString(overview, Files.readString(overview).replace("platform-prometheus", "unknown-source")
                .replace("/d/messaging-overview", "/d/unknown"));
        var errors = DashboardValidation.validate(root);
        assertTrue(errors.stream().anyMatch(error -> error.contains("unknown datasource")));
        assertTrue(errors.stream().anyMatch(error -> error.contains("unknown dashboard link")));
        Files.delete(directory.resolve("trace.json"));
        assertTrue(DashboardValidation.validate(root).stream().anyMatch(error -> error.contains("file set mismatch")));
    }
}
