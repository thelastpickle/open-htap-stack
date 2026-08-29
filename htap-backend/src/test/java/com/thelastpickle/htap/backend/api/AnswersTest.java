package com.thelastpickle.htap.backend.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * The three answers a read can have, without a container.
 *
 * <p>Worth its own test rather than being left to the routes: every route tests the
 * connection before it calls {@link Answers#orElse}, so a suite run against a closed port
 * reaches the read that failed against a *connected* Cassandra in no other way, and that
 * is the branch this class exists for.
 */
class AnswersTest {

    @Test
    void aReadThatAnswersIsTheAnswer() {
        AtomicBoolean fell = new AtomicBoolean();
        String answered = Answers.orElse("/api/test", () -> "rows", () -> {
            fell.set(true);
            return "zeros";
        });
        assertEquals("rows", answered);
        assertFalse(fell.get(), "the fallback was built for a read that answered");
    }

    /** A table the sink has not created yet, which the page redraws its way out of. */
    @Test
    void aFailedReadIsTheEmptyShape() {
        String answered = Answers.orElse(
                "/api/test",
                () -> {
                    throw new IllegalStateException("unconfigured table demo.events");
                },
                () -> "zeros");
        assertEquals("zeros", answered);
    }

    /** A refusal the route decided on is the answer, so it is not turned into zeros. */
    @Test
    void aRefusalPassesThroughWithItsStatus() {
        ApiException refused = assertThrows(
                ApiException.class,
                () -> Answers.orElse(
                        "/api/test",
                        () -> {
                            throw new ApiException(404, "No such asset: drone-1");
                        },
                        () -> "zeros"));
        assertEquals(404, refused.status());
        assertEquals("No such asset: drone-1", refused.getMessage());
    }
}
