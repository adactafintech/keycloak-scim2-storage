import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads and extends the list of supported Keycloak versions, used by keycloak-version-check.yml.
 *
 * Usage (from the repository root):
 *   java .github/scripts/KeycloakVersions.java list           prints the versions in the build.yml matrix, one per line
 *   java .github/scripts/KeycloakVersions.java add <version>  adds a version to the build.yml matrix (the single source
 *                                                             of truth) and a keycloak-X-Y-Z profile to pom.xml for
 *                                                             local builds
 *
 * Fails (exit 1) when a file does not have the expected shape, so the workflow never opens a half-edited PR.
 */
public class KeycloakVersions {
    private static final Path BUILD_YML = Path.of(".github/workflows/build.yml");
    private static final Path POM = Path.of("pom.xml");

    private static final Pattern VERSION = Pattern.compile("\\d+\\.\\d+\\.\\d+");
    private static final Pattern MATRIX_KEY = Pattern.compile("^(\\s*)keycloak:\\s*$");
    private static final Pattern MATRIX_ITEM = Pattern.compile("^(\\s*)- (\\d+\\.\\d+\\.\\d+)\\s*$");
    private static final Comparator<String> BY_VERSION = Comparator.comparing(
            v -> Arrays.stream(v.split("\\.")).mapToInt(Integer::parseInt).toArray(), Arrays::compare);

    public static void main(String[] args) throws IOException {
        if (args.length == 1 && args[0].equals("list")) {
            matrixVersions(read(BUILD_YML)).forEach(System.out::println);
        } else if (args.length == 2 && args[0].equals("add") && VERSION.matcher(args[1]).matches()) {
            add(args[1]);
        } else {
            fail("usage: KeycloakVersions list | add <major.minor.patch>");
        }
    }

    private static void add(String version) throws IOException {
        List<String> versions = matrixVersions(read(BUILD_YML));
        if (versions.contains(version)) {
            fail(version + " is already in " + BUILD_YML);
        }
        List<String> updated = new ArrayList<>(versions);
        updated.add(version);
        updated.sort(BY_VERSION);

        write(BUILD_YML, replaceMatrix(read(BUILD_YML), updated));
        write(POM, addProfile(read(POM), version));
    }

    /** The items of the first "keycloak:" list in build.yml (the build matrix). */
    private static List<String> matrixVersions(List<String> lines) {
        int[] block = matrixBlock(lines);
        List<String> versions = new ArrayList<>();
        for (int i = block[0]; i < block[1]; i++) {
            Matcher m = MATRIX_ITEM.matcher(lines.get(i));
            m.matches();
            versions.add(m.group(2));
        }
        return versions;
    }

    /** [first item line, line after the last item] of the matrix list. */
    private static int[] matrixBlock(List<String> lines) {
        for (int i = 0; i < lines.size(); i++) {
            if (MATRIX_KEY.matcher(lines.get(i)).matches()) {
                int end = i + 1;
                while (end < lines.size() && MATRIX_ITEM.matcher(lines.get(end)).matches()) {
                    end++;
                }
                if (end == i + 1) {
                    fail(BUILD_YML + ": the keycloak: matrix has no version items");
                }
                return new int[]{i + 1, end};
            }
        }
        throw fail(BUILD_YML + ": no keycloak: matrix found");
    }

    private static List<String> replaceMatrix(List<String> lines, List<String> versions) {
        int[] block = matrixBlock(lines);
        Matcher first = MATRIX_ITEM.matcher(lines.get(block[0]));
        first.matches();
        String indent = first.group(1);
        List<String> out = new ArrayList<>(lines.subList(0, block[0]));
        versions.forEach(v -> out.add(indent + "- " + v));
        out.addAll(lines.subList(block[1], lines.size()));
        return out;
    }

    private static List<String> addProfile(List<String> lines, String version) {
        String id = "keycloak-" + version.replace('.', '-');
        if (lines.stream().anyMatch(l -> l.trim().equals("<id>" + id + "</id>"))) {
            return lines; // already there, e.g. added by hand
        }
        int end = indexOf(lines, l -> l.trim().equals("</profiles>"), POM + ": no </profiles>");
        String indent = lines.get(end).substring(0, lines.get(end).indexOf('<')) + "    ";
        List<String> out = new ArrayList<>(lines.subList(0, end));
        out.add(indent + "<profile>");
        out.add(indent + "    <id>" + id + "</id>");
        out.add(indent + "    <properties>");
        out.add(indent + "        <keycloak.version>" + version + "</keycloak.version>");
        out.add(indent + "    </properties>");
        out.add(indent + "</profile>");
        out.addAll(lines.subList(end, lines.size()));
        return out;
    }

    private static int indexOf(List<String> lines, java.util.function.Predicate<String> p, String error) {
        for (int i = 0; i < lines.size(); i++) {
            if (p.test(lines.get(i))) {
                return i;
            }
        }
        throw fail(error);
    }

    private static List<String> read(Path file) throws IOException {
        return new ArrayList<>(Files.readAllLines(file, StandardCharsets.UTF_8));
    }

    /** Writes with the file's line separator, and a final newline only if the file had one. */
    private static void write(Path file, List<String> lines) throws IOException {
        String original = Files.readString(file, StandardCharsets.UTF_8);
        String eol = original.contains("\r\n") ? "\r\n" : "\n";
        String finalEol = original.endsWith("\n") ? eol : "";
        Files.writeString(file, String.join(eol, lines) + finalEol, StandardCharsets.UTF_8);
    }

    private static RuntimeException fail(String message) {
        System.err.println(message);
        System.exit(1);
        return new IllegalStateException(message);
    }
}
