import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/** Prepare immutable monitoring JAR copies and a local Grafana admin token. */
public class MonitorArtifacts {
    private static final List<String[]> MODULES = List.of(
            new String[] {"API", "messaging-api"},
            new String[] {"INGRESS", "delivery-ingress-worker"},
            new String[] {"DISPATCH", "dispatch-worker"},
            new String[] {"SIMULATOR", "external-api-simulator"});

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: java scripts/testing/MonitorArtifacts.java <token|snapshot> [root]");
        }
        Path root = args.length == 2 ? Path.of(args[1]).toAbsolutePath() : Path.of("").toAbsolutePath();
        switch (args[0]) {
            case "token" -> {
                byte[] secret = new byte[32];
                new SecureRandom().nextBytes(secret);
                System.out.println(Base64.getUrlEncoder().withoutPadding().encodeToString(secret));
            }
            case "snapshot" -> snapshot(root);
            default -> throw new IllegalArgumentException("Expected token or snapshot");
        }
    }

    private static void snapshot(Path root) throws Exception {
        Path directory = root.resolve(".monitoring");
        Files.createDirectories(directory);
        StringBuilder environment = new StringBuilder();
        for (String[] entry : MODULES) {
            String module = entry[1];
            Path source = root.resolve(module).resolve("target").resolve(module + "-1.0-SNAPSHOT.jar");
            String digest = sha256(source);
            Path target = directory.resolve("jars").resolve(digest).resolve(module + ".jar");
            Files.createDirectories(target.getParent());
            if (!Files.exists(target)) {
                Path temporary = Files.createTempFile(target.getParent(), module + "-", ".tmp");
                try {
                    Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING);
                    if (!sha256(temporary).equals(digest)) throw new IOException("Snapshot checksum mismatch: " + module);
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                } finally {
                    Files.deleteIfExists(temporary);
                }
            }
            if (!sha256(target).equals(digest)) throw new IOException("Snapshot checksum mismatch: " + module);
            environment.append("MONITOR_").append(entry[0]).append("_JAR=./")
                    .append(root.relativize(target).toString().replace('\\', '/')).append('\n');
        }
        Files.writeString(directory.resolve("jars.env"), environment.toString());
    }

    private static String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            for (int length; (length = input.read(buffer)) != -1;) digest.update(buffer, 0, length);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
