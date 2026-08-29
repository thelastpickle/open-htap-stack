package com.thelastpickle.htap.backend.api;

import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * A read that answers the empty shape rather than a 500.
 *
 * <p>Every panel on the Overview and Map pages polls, and a stack that is still starting has
 * tables the sink has not created yet. A page that draws nothing and keeps polling recovers on
 * its own; a page holding an error banner needs the operator. So a failed read is logged and
 * answered with zeros, which is the Python's {@code except Exception} branch on those routes.
 *
 * <p>A refusal the route decided on is different, and {@link ApiException} passes through: a
 * 404 for an asset that does not exist is the answer, not a failure to answer.
 */
public final class Answers {

    private static final Logger LOG = Logger.getLogger(Answers.class);

    private Answers() {}

    public static <T> T orElse(String route, Supplier<T> read, Supplier<T> fallback) {
        try {
            return read.get();
        } catch (ApiException e) {
            throw e;
        } catch (RuntimeException e) {
            LOG.warnf("%s failed: %s", route, e);
            return fallback.get();
        }
    }
}
