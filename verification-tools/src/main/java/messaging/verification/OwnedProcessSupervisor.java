package messaging.verification;

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
    private Path log;
    private final List<String> command;
    private Process child;
    private boolean suspended;

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
                    case "suspend" -> { signal(true); output.println(response("suspended")); }
                    case "resume" -> { signal(false); output.println(response("resumed")); }
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
                    default -> {
                        if (line.startsWith("stop ") && line.length() > 5) {
                            int grace = Integer.parseInt(line.substring(5));
                            if (grace < 1 || grace > 120) throw new IllegalArgumentException("Invalid stop grace seconds");
                            stop(grace);
                            output.println(response("stopped"));
                        } else if (line.startsWith("restart ") && line.length() > 8) {
                            stop();
                            log = Path.of(line.substring(8));
                            start();
                            output.println(response("restarted"));
                        } else output.println("{\"error\":\"unknown command\"}");
                    }
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
        suspended = false;
    }

    private synchronized void signal(boolean suspend) throws IOException {
        if (child == null || !child.isAlive() || suspended == suspend)
            throw new IllegalStateException("Invalid supervised child signal state");
        try {
            var command = new ProcessBuilder("/bin/kill", suspend ? "-STOP" : "-CONT",
                    Long.toString(child.pid())).start();
            if (!command.waitFor(5, TimeUnit.SECONDS) || command.exitValue() != 0)
                throw new IOException("Could not signal supervised child: " + child.pid());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while signaling supervised child", interrupted);
        }
        suspended = suspend;
    }

    private synchronized void stop() {
        stop(15);
    }

    private synchronized void stop(int graceSeconds) {
        if (child == null || !child.isAlive()) return;
        if (suspended) {
            try { signal(false); }
            catch (IOException failure) { throw new IllegalStateException("Could not resume supervised child", failure); }
        }
        child.destroy();
        awaitExit(graceSeconds);
        if (child.isAlive()) {
            child.destroyForcibly();
            awaitExit(5);
        }
        if (child.isAlive()) throw new IllegalStateException("Supervised child did not stop: " + child.pid());
    }

    private synchronized void kill() {
        if (child == null || !child.isAlive()) return;
        if (suspended) {
            try { signal(false); }
            catch (IOException failure) { throw new IllegalStateException("Could not resume supervised child", failure); }
        }
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
