package event.verification;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnedProcessSupervisorTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test void startsRestartsAndClosesOwnedChild(@TempDir Path directory) throws Exception {
        Path log = directory.resolve("child.log");
        try (var harness = new Harness(new OwnedProcessSupervisor(log,
                List.of("sh", "-c", "echo started; exec sleep 30")))) {
            var started = harness.read();
            long first = started.path("pid").asLong();
            assertEquals("started", started.path("event").asText());
            assertTrue(started.path("alive").asBoolean());
            assertTrue(ProcessHandle.of(first).orElseThrow().isAlive());
            for (int attempt = 0; attempt < 50 && !java.nio.file.Files.readString(log).contains("started"); attempt++)
                Thread.sleep(20);
            assertTrue(java.nio.file.Files.readString(log).contains("started"));

            var restarted = harness.command("restart");
            long second = restarted.path("pid").asLong();
            assertEquals("restarted", restarted.path("event").asText());
            assertNotEquals(first, second);
            assertFalse(ProcessHandle.of(first).map(ProcessHandle::isAlive).orElse(false));
            assertTrue(ProcessHandle.of(second).orElseThrow().isAlive());
            assertEquals(second, harness.command("status").path("pid").asLong());

            var killed = harness.command("kill");
            assertEquals("killed", killed.path("event").asText());
            assertFalse(killed.path("alive").asBoolean());
            assertFalse(ProcessHandle.of(second).map(ProcessHandle::isAlive).orElse(false));

            var recovered = harness.command("restart " + directory.resolve("restarted.log"));
            long third = recovered.path("pid").asLong();
            assertNotEquals(second, third);

            var closed = harness.command("close");
            assertEquals("closed", closed.path("event").asText());
            assertFalse(closed.path("alive").asBoolean());
            assertFalse(ProcessHandle.of(third).map(ProcessHandle::isAlive).orElse(false));
        }
        assertTrue(java.nio.file.Files.readString(log).contains("started"));
        assertTrue(java.nio.file.Files.exists(directory.resolve("restarted.log")));
    }

    @Test void inputClosureStopsOwnedChild(@TempDir Path directory) throws Exception {
        try (var harness = new Harness(new OwnedProcessSupervisor(directory.resolve("child.log"),
                List.of("sleep", "30")))) {
            long pid = harness.read().path("pid").asLong();
            harness.closeInput();
            harness.await();
            assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false));
        }
    }

    private static final class Harness implements AutoCloseable {
        private final PipedOutputStream input = new PipedOutputStream();
        private final PipedInputStream childInput;
        private final PipedInputStream output = new PipedInputStream();
        private final PrintStream childOutput;
        private final BufferedReader reader;
        private final PrintWriter writer;
        private final java.util.concurrent.ExecutorService executor = Executors.newSingleThreadExecutor();
        private final java.util.concurrent.Future<?> task;

        Harness(OwnedProcessSupervisor supervisor) throws Exception {
            childInput = new PipedInputStream(input);
            childOutput = new PrintStream(new PipedOutputStream(output), true, StandardCharsets.UTF_8);
            reader = new BufferedReader(new java.io.InputStreamReader(output, StandardCharsets.UTF_8));
            writer = new PrintWriter(input, true, StandardCharsets.UTF_8);
            task = executor.submit(() -> {
                try { supervisor.serve(childInput, childOutput); }
                catch (Exception exception) { throw new RuntimeException(exception); }
            });
        }

        tools.jackson.databind.JsonNode read() throws Exception {
            return JSON.readTree(reader.readLine());
        }

        tools.jackson.databind.JsonNode command(String value) throws Exception {
            writer.println(value);
            return read();
        }

        void closeInput() throws Exception { input.close(); }

        void await() throws Exception { task.get(5, TimeUnit.SECONDS); }

        @Override public void close() throws Exception {
            input.close();
            await();
            childOutput.close();
            output.close();
            executor.shutdownNow();
        }
    }
}
