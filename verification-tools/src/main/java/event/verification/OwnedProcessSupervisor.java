package event.verification;

import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Own a child process across start, restart, and shutdown over a line protocol. */
final class OwnedProcessSupervisor {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final Path log;
    private final List<String> command;
    private Process child;

    OwnedProcessSupervisor(Path log, List<String> command) {
        this.log = log;
        this.command = command;
    }

    static void run(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("Usage: supervise-process log-path executable [args...]");
        var supervisor = new OwnedProcessSupervisor(Path.of(args[0]), Arrays.asList(args).subList(1, args.length));
        var shutdown = new Thread(supervisor::stop, "supervised-child-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdown);
        try {
            supervisor.serve(System.in, System.out);
        } finally {
            supervisor.stop();
            try { Runtime.getRuntime().removeShutdownHook(shutdown); }
            catch (IllegalStateException ignored) { /* JVM shutdown already owns the hook. */ }
        }
    }

    void serve(InputStream input, PrintStream output) throws IOException {
        try (var reader = new BufferedReader(new java.io.InputStreamReader(input, StandardCharsets.UTF_8))) {
            start();
            output.println(response("started"));
            for (String line; (line = reader.readLine()) != null;) {
                switch (line) {
                    case "status" -> output.println(response("status"));
                    case "stop" -> { stop(); output.println(response("stopped")); }
                    case "kill" -> { kill(); output.println(response("killed")); }
                    case "restart" -> {
                        stop();
                        start();
                        output.println(response("restarted"));
                    }
                    case "close" -> {
                        stop();
                        output.println(response("closed"));
                        return;
                    }
                    default -> output.println("{\"error\":\"unknown command\"}");
                }
            }
        } finally {
            stop();
        }
    }

    private synchronized void start() throws IOException {
        if (child != null && child.isAlive()) throw new IllegalStateException("Child is already running");
        child = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile())).start();
    }

    private synchronized void stop() {
        if (child == null || !child.isAlive()) return;
        child.destroy();
        awaitExit(15);
        if (child.isAlive()) {
            child.destroyForcibly();
            awaitExit(5);
        }
        if (child.isAlive()) throw new IllegalStateException("Supervised child did not stop: " + child.pid());
    }

    private synchronized void kill() {
        if (child == null || !child.isAlive()) return;
        child.destroyForcibly();
        awaitExit(5);
        if (child.isAlive()) throw new IllegalStateException("Supervised child did not stop: " + child.pid());
    }

    private void awaitExit(int seconds) {
        try { child.waitFor(seconds, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping supervised child", interrupted);
        }
    }

    private synchronized String response(String event) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("event", event);
        value.put("pid", child.pid());
        value.put("alive", child.isAlive());
        if (!child.isAlive()) value.put("exitCode", child.exitValue());
        return JSON.writeValueAsString(value);
    }
}
