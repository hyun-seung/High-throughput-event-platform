import java.nio.file.*;
import java.util.*;
import javax.xml.parsers.DocumentBuilderFactory;

/** Summarize JUnit/Surefire results, keeping skipped tests separate from executed tests. */
public class JavaTestReport {
    public static void main(String[] args) throws Exception {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        int tests = 0, skipped = 0, failures = 0, errors = 0, suites = 0;
        try (var modules = Files.list(Path.of("."))) {
            for (var module : modules.filter(Files::isDirectory).toList()) {
                var reports = module.resolve("target/surefire-reports");
                if (!Files.isDirectory(reports)) continue;
                try (var files = Files.newDirectoryStream(reports, "TEST-*.xml")) {
                    for (var file : files) {
                        var root = factory.newDocumentBuilder().parse(file.toFile()).getDocumentElement();
                        suites++;
                        tests += Integer.parseInt(root.getAttribute("tests"));
                        skipped += Integer.parseInt(root.getAttribute("skipped"));
                        failures += Integer.parseInt(root.getAttribute("failures"));
                        errors += Integer.parseInt(root.getAttribute("errors"));
                    }
                }
            }
        }
        System.out.printf("{\"suites\":%d,\"executed\":%d,\"skipped\":%d,\"failures\":%d,\"errors\":%d}%n",
                suites, tests - skipped, skipped, failures, errors);
        if (tests == 0 || failures > 0 || errors > 0 || (args.length > 0 && args[0].equals("integration") && skipped > 0)) {
            throw new IllegalStateException("Java test verification incomplete; inspect reports and Maven exit status");
        }
    }
}
