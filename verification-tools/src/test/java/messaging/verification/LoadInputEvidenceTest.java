package messaging.verification;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoadInputEvidenceTest {
    @TempDir Path directory;

    @Test
    void retainsUnansweredStartsAndMatchedResults() throws Exception {
        Path path = directory.resolve("requests.jsonl");
        Files.writeString(path, start(0, "first") + start(1, "second") + result(0, "first"));
        var evidence = LoadInputEvidence.readManifest(path);
        assertEquals(2, evidence.get("starts").size());
        assertEquals(1, evidence.get("results").size());
        assertEquals("second", evidence.get("starts").get("1").get("key").asText());
    }

    @Test
    void rejectsRepeatedUnknownOrOrphanedEvents() throws Exception {
        rejects(start(0, "a") + start(0, "a"));
        rejects(start(0, "a") + result(0, "a") + result(0, "a"));
        rejects(start(0, "a") + result(1, "a"));
        rejects(result(0, "a"));
        rejects("{\"kind\":\"other\",\"iteration\":0,\"key\":\"a\"}\n");
    }

    @Test
    void rejectsKeyMismatch() throws Exception {
        rejects(start(0, "a") + result(0, "b"));
    }

    @Test
    void permitsOneBoundaryArrivalButNoLostOrUnansweredInput() {
        assertTrue(LoadInputEvidence.completeInput(10, 10, 10, 10, 0));
        assertTrue(LoadInputEvidence.completeInput(10, 11, 11, 11, 0));
        assertFalse(LoadInputEvidence.completeInput(10, 9, 9, 9, 0));
        assertFalse(LoadInputEvidence.completeInput(10, 11, 10, 10, 0));
        assertFalse(LoadInputEvidence.completeInput(10, 10, 10, 10, 1));
    }

    private void rejects(String lines) throws Exception {
        Path path = directory.resolve("invalid.jsonl");
        Files.writeString(path, lines);
        assertThrows(IllegalArgumentException.class, () -> LoadInputEvidence.readManifest(path));
    }

    private static String start(int iteration, String key) {
        return "{\"kind\":\"start\",\"iteration\":" + iteration + ",\"key\":\"" + key + "\"}\n";
    }

    private static String result(int iteration, String key) {
        return "{\"kind\":\"result\",\"iteration\":" + iteration + ",\"key\":\"" + key + "\"}\n";
    }
}
