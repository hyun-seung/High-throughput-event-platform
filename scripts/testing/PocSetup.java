import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Install pinned local PoC tools without modifying the global Python installation. */
public final class PocSetup {
    private static final String VERSION = "1.8.1";
    private static final String MANIFEST_SHA256 = "623b62f6ead2ac46f161a8c859bd1679f4a87ea0e7d1a4c48f9a46f648873b52";

    private PocSetup() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("self-test")) {
            selfTest();
            return;
        }
        if (args.length != 0) throw new IllegalArgumentException("Usage: java scripts/testing/PocSetup.java");
        Path root = Path.of("").toAbsolutePath();
        Path tools = root.resolve(".poc-tools");
        Files.createDirectories(tools);
        Path python = tools.resolve("venv/bin/python");
        if (!Files.exists(python)) run("python3", "-m", "venv", tools.resolve("venv").toString());
        run(python.toString(), "-m", "pip", "install", "--only-binary=:all:", "-r",
                root.resolve("scripts/poc/requirements.txt").toString());

        String os = Map.of("Mac OS X", "macos", "Linux", "linux").get(System.getProperty("os.name"));
        String arch = Map.of("aarch64", "arm64", "arm64", "arm64", "x86_64", "amd64", "amd64", "amd64")
                .get(System.getProperty("os.arch"));
        if (os == null || arch == null) throw new IllegalStateException("Supported k6 platforms: macOS/Linux arm64/amd64");
        String name = "k6-v" + VERSION + "-" + os + "-" + arch + (os.equals("macos") ? ".zip" : ".tar.gz");
        String base = "https://github.com/grafana/k6/releases/download/v" + VERSION + "/";
        Path temporary = Files.createTempDirectory(tools, "k6-");
        try {
            Path manifest = temporary.resolve("checksums.txt");
            download(base + "k6-v" + VERSION + "-checksums.txt", manifest);
            verify(manifest, MANIFEST_SHA256, "k6 checksum manifest mismatch");
            String expected = Arrays.stream(Files.readString(manifest).split("\\R"))
                    .map(String::trim).filter(line -> !line.isEmpty()).map(line -> line.split("\\s+"))
                    .filter(parts -> parts.length == 2 && parts[1].replaceFirst("^\\*", "").equals(name))
                    .map(parts -> parts[0]).findFirst().orElseThrow(() -> new IllegalStateException("k6 archive missing from manifest"));
            Path archive = temporary.resolve(name);
            download(base + name, archive);
            verify(archive, expected, "k6 archive checksum mismatch");
            Path binary = temporary.resolve("k6");
            if (os.equals("macos")) extractZip(archive, binary);
            else extractTar(archive, binary);
            binary.toFile().setExecutable(true, true);
            Files.move(binary, tools.resolve("k6"), StandardCopyOption.REPLACE_EXISTING);
        } finally {
            try (var paths = Files.list(temporary)) {
                for (Path path : paths.toList()) Files.deleteIfExists(path);
            }
            Files.deleteIfExists(temporary);
        }
        run(tools.resolve("k6").toString(), "version");
        System.out.println("Ready: .poc-tools/venv/bin/python scripts/poc/실행.py --suite smoke");
    }

    private static void download(String url, Path destination) throws Exception {
        run("curl", "--fail", "--location", "--silent", "--show-error", "--retry", "2",
                "--output", destination.toString(), url);
    }

    private static void verify(Path file, String expected, String message) throws Exception {
        String actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        if (!actual.equalsIgnoreCase(expected)) throw new IllegalStateException(message);
    }

    private static void extractZip(Path archive, Path binary) throws Exception {
        try (var zip = new ZipFile(archive.toFile())) {
            var members = zip.stream().filter(entry -> !entry.isDirectory() && entry.getName().endsWith("/k6")).toList();
            if (members.size() != 1) throw new IllegalStateException("Expected one k6 executable in archive");
            try (var input = zip.getInputStream(members.getFirst())) { Files.copy(input, binary); }
        }
    }

    private static void extractTar(Path archive, Path binary) throws Exception {
        var listing = new ProcessBuilder("tar", "-tzf", archive.toString()).start();
        String names = new String(listing.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (listing.waitFor() != 0) throw new IOException("Cannot list k6 archive");
        List<String> members = names.lines().filter(name -> name.endsWith("/k6")).toList();
        if (members.size() != 1) throw new IllegalStateException("Expected one k6 executable in archive");
        var extraction = new ProcessBuilder("tar", "-xOzf", archive.toString(), members.getFirst()).start();
        try (var input = extraction.getInputStream()) { Files.copy(input, binary); }
        if (extraction.waitFor() != 0) throw new IOException("Cannot read k6 executable from archive");
    }

    private static void run(String... command) throws Exception {
        int status = new ProcessBuilder(command).inheritIO().start().waitFor();
        if (status != 0) throw new IOException(command[0] + " exited with status " + status);
    }

    private static void selfTest() throws Exception {
        Path directory = Files.createTempDirectory("poc-setup-test-");
        try {
            byte[] contents = "fixture-k6".getBytes(StandardCharsets.UTF_8);
            Path zipPath = directory.resolve("fixture.zip");
            try (var zip = new ZipOutputStream(Files.newOutputStream(zipPath))) {
                zip.putNextEntry(new ZipEntry("k6-v1/k6"));
                zip.write(contents);
                zip.closeEntry();
            }
            Path extractedZip = directory.resolve("zip-k6");
            extractZip(zipPath, extractedZip);
            if (!Arrays.equals(contents, Files.readAllBytes(extractedZip))) throw new AssertionError("ZIP extraction mismatch");
            String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(zipPath)));
            verify(zipPath, digest, "fixture digest mismatch");
            try {
                verify(zipPath, "0".repeat(64), "bad digest rejected");
                throw new AssertionError("Bad digest was accepted");
            } catch (IllegalStateException expected) {
                if (!expected.getMessage().equals("bad digest rejected")) throw expected;
            }
            Path packageDirectory = directory.resolve("k6-v1");
            Files.createDirectories(packageDirectory);
            Files.write(packageDirectory.resolve("k6"), contents);
            Path tarPath = directory.resolve("fixture.tar.gz");
            run("tar", "-czf", tarPath.toString(), "-C", directory.toString(), "k6-v1");
            Path extractedTar = directory.resolve("tar-k6");
            extractTar(tarPath, extractedTar);
            if (!Arrays.equals(contents, Files.readAllBytes(extractedTar))) throw new AssertionError("TAR extraction mismatch");
            System.out.println("PASS PoC setup archive and checksum self-test");
        } finally {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }
}
