package messaging.verification;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.tools.ToolProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class JavaTestReportTest {
    @TempDir static Path classes;
    @TempDir Path project;

    @BeforeAll
    static void compileReportTool() {
        Path source = Path.of("../scripts/testing/JavaTestReport.java").toAbsolutePath();
        assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null,
                "-d", classes.toString(), source.toString()));
    }

    @Test
    void countsDeclaredModulesIncludingNestedPathsAndIgnoresRemovedAndProfileModules() throws Exception {
        pom("<module> current-api </module><module>apps/current-worker</module>");
        report("current-api", 6, 2, 0, 0);
        report("apps/current-worker", 4, 1, 0, 0);
        report("removed-ap", 100, 0, 0, 0);
        report("old-failed-ap", 3, 0, 3, 0);
        report("profile-only", 5, 0, 0, 5);

        Result result = run("unit");
        assertEquals(0, result.status(), result.error());
        assertEquals(summary(2, 7, 3, 0, 0), result.output());
    }

    @Test
    void oldPassingReportsCannotMakeAnEmptyCurrentRunPass() throws Exception {
        pom("<module>current-ap</module>");
        report("removed-ap", 100, 0, 0, 0);

        Result result = run("unit");
        assertNotEquals(0, result.status());
        assertEquals(summary(0, 0, 0, 0, 0), result.output());
    }

    @ParameterizedTest
    @CsvSource({"1,0", "0,1"})
    void currentFailuresAndErrorsStillRejectVerification(int failures, int errors) throws Exception {
        pom("<module>current-ap</module>");
        report("current-ap", 3, 0, failures, errors);
        report("removed-ap", 100, 0, 0, 0);

        Result result = run("unit");
        assertNotEquals(0, result.status());
        assertEquals(summary(1, 3, 0, failures, errors), result.output());
    }

    @ParameterizedTest
    @CsvSource({"unit,0", "integration,1"})
    void skippedTestsAreReportedAndRejectedInIntegrationMode(String mode, int status) throws Exception {
        pom("<module>current-ap</module>");
        report("current-ap", 3, 1, 0, 0);

        Result result = run(mode);
        assertEquals(status, result.status(), result.error());
        assertEquals(summary(1, 2, 1, 0, 0), result.output());
    }

    private void pom(String modules) throws Exception {
        Files.writeString(project.resolve("pom.xml"), """
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <modules>%s</modules>
                  <profiles><profile><id>inactive</id>
                    <modules><module>profile-only</module></modules>
                  </profile></profiles>
                </project>
                """.formatted(modules));
    }

    private void report(String module, int tests, int skipped, int failures, int errors) throws Exception {
        Path file = project.resolve(module).resolve("target/surefire-reports/TEST-fixture.xml");
        Files.createDirectories(file.getParent());
        Files.writeString(file, """
                <testsuite tests="%d" skipped="%d" failures="%d" errors="%d"/>
                """.formatted(tests, skipped, failures, errors));
    }

    private Result run(String mode) throws Exception {
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classes.toString(), "JavaTestReport", mode).directory(project.toFile()).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS), "Report tool did not exit");
            return new Result(process.exitValue(), new String(process.getInputStream().readAllBytes()).strip(),
                    new String(process.getErrorStream().readAllBytes()));
        } finally {
            process.destroyForcibly();
        }
    }

    private static String summary(int suites, int executed, int skipped, int failures, int errors) {
        return "{\"suites\":%d,\"executed\":%d,\"skipped\":%d,\"failures\":%d,\"errors\":%d}"
                .formatted(suites, executed, skipped, failures, errors);
    }

    private record Result(int status, String output, String error) { }
}
