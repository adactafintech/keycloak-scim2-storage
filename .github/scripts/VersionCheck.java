import org.apache.maven.artifact.versioning.ComparableVersion;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;

/**
 * Compares the pom version against existing tags using Maven version ordering (2.12 > 2.9).
 *
 * Usage: java -cp maven-artifact.jar VersionCheck.java <pomVersion> [tag...]
 * Prints "highest=<tag>" (empty when there are no tags) and exits 1 when pomVersion < highest tag.
 */
public class VersionCheck {
    public static void main(String[] args) {
        ComparableVersion pom = new ComparableVersion(args[0]);
        Optional<String> highest = Arrays.stream(args, 1, args.length)
                .max(Comparator.comparing(ComparableVersion::new));

        System.out.println("highest=" + highest.orElse(""));
        if (highest.isPresent() && pom.compareTo(new ComparableVersion(highest.get())) < 0) {
            System.exit(1);
        }
    }
}
