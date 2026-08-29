package com.thelastpickle.htap.backend.api;

import static io.restassured.RestAssured.when;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * Every read here is a synchronous driver call, so each route must run on a worker thread. A
 * route that reached the event loop would stall every other request behind its own read, and
 * under a Cassandra that has stopped answering it would stall the whole server.
 *
 * <p>Two halves make that structural rather than incidental. The framework dispatches a route
 * that returns a plain value onto a worker, which the probe route measures; and no route asks
 * for anything else, which reflection over the resource classes checks.
 */
@QuarkusTest
class DispatchThreadTest {

    private static final List<Class<?>> RESOURCES = List.of(
            AlertsResource.class,
            LivenessResource.class,
            MapResource.class,
            OverviewResource.class,
            PlatformResource.class,
            ZonesResource.class);

    @Test
    void aRouteReturningAPlainValueRunsOffTheEventLoop() {
        String thread = when().get("/test/dispatch").then().statusCode(200).extract().asString();

        assertFalse(thread.startsWith("vert.x-eventloop"), "dispatched on the event loop: " + thread);
        assertTrue(thread.startsWith("executor-thread"), "unexpected worker pool: " + thread);
    }

    /**
     * {@code @NonBlocking} and a reactive return type each move a route onto the event loop,
     * which is the one thing a blocking read must not do.
     */
    @Test
    void noRouteAsksForTheEventLoop() {
        for (Class<?> resource : RESOURCES) {
            for (Method route : resource.getDeclaredMethods()) {
                if (!isRoute(route)) {
                    continue;
                }
                String where = resource.getSimpleName() + '.' + route.getName();
                for (var annotation : route.getAnnotations()) {
                    assertFalse(
                            "NonBlocking".equals(annotation.annotationType().getSimpleName()),
                            where + " is annotated @NonBlocking");
                }
                assertFalse(isReactive(route.getReturnType()), where + " returns a reactive type");
            }
        }
    }

    private static boolean isRoute(Method method) {
        for (var annotation : method.getAnnotations()) {
            if (annotation.annotationType().isAnnotationPresent(jakarta.ws.rs.HttpMethod.class)) {
                return true;
            }
        }
        return false;
    }

    /** By simple name, so this holds without a compile dependency on Mutiny. */
    private static boolean isReactive(Class<?> type) {
        return CompletionStage.class.isAssignableFrom(type)
                || Future.class.isAssignableFrom(type)
                || List.of("Uni", "Multi").contains(type.getSimpleName());
    }
}
