package io.astropipe;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Polls for a condition instead of sleeping for a guessed duration.
 *
 * <p>Shared by the functional and integration suites. Nothing in either may assert on
 * how long the pipeline takes, only that it gets there: detection latency in particular
 * is a property of the platform rather than of this application.
 */
final class Await {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final long POLL_MILLIS = 20;

    private Await() { }

    /** Waits for {@code condition}, failing with {@code expectation} if it never holds. */
    static void until(String expectation, Condition condition) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.isMet()) return;
            TimeUnit.MILLISECONDS.sleep(POLL_MILLIS);
        }
        if (!condition.isMet()) fail("timed out after " + TIMEOUT + " waiting until " + expectation);
    }

    interface Condition {
        boolean isMet() throws Exception;
    }
}
