package com.backend.resilience;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadConfig;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class ResilienceDemoApp {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(1500))
            .build();

    public static void main(String[] args) throws Exception {

        // =========================================================================
        // 1. ISOLATED THREAD POOLS (BULKHEADS)
        // =========================================================================
        // Core Internal Pool: High priority, generous allocation
        ThreadPoolBulkhead coreBulkhead = ThreadPoolBulkhead.of("core-service-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(5)
                        .maxThreadPoolSize(5)
                        .queueCapacity(10)
                        .build());

        // 3rd-Party External Pool: Constrained to 2 threads, 1 queue slot to contain blast radius
        ThreadPoolBulkhead externalApiBulkhead = ThreadPoolBulkhead.of("external-api-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(2)
                        .maxThreadPoolSize(2)
                        .queueCapacity(1)
                        .build());

        // =========================================================================
        // 2. EXPONENTIAL BACKOFF + JITTER (RETRY)
        // =========================================================================
        // Initial wait 200ms, doubles each retry (2.0 multiplier), randomized jitter factor 0.5
        IntervalFunction backoffWithJitter = IntervalFunction.ofExponentialRandomBackoff(
                Duration.ofMillis(200),
                2.0,
                0.5
        );

        Retry retry = Retry.of("externalApiRetry", RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(backoffWithJitter)
                .retryExceptions(RuntimeException.class)
                .build());

        retry.getEventPublisher()
                .onRetry(e -> System.out.printf("[RETRY EVENT] Attempt #%d for %s (Wait: %dms)%n",
                        e.getNumberOfRetryAttempts(), e.getName(), e.getWaitInterval().toMillis()));

        // =========================================================================
        // 3. CIRCUIT BREAKER
        // =========================================================================
        CircuitBreaker circuitBreaker = CircuitBreaker.of("externalApiBreaker", CircuitBreakerConfig.custom()
                .slidingWindowSize(4)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(3))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build());

        circuitBreaker.getEventPublisher()
                .onStateTransition(event ->
                        System.out.printf("\n>>> [CIRCUIT BREAKER STATE CHANGE] -> %s <<<\n\n", event.getStateTransition()));


        // -------------------------------------------------------------------------
        // DEMO 1: Thread Isolation (Core vs External) & Bulkhead Saturation
        // -------------------------------------------------------------------------
        System.out.println("=================================================================");
        System.out.println("TEST 1: Bulkhead Isolation & Queue Rejection");
        System.out.println("Firing 5 slow 3rd-party calls (Pool=2, Queue=1) + 1 Core Service call");
        System.out.println("=================================================================");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // 1 Core Request runs smoothly in its own unblocked pool
        futures.add(executeTask(100, coreBulkhead, circuitBreaker, retry,
                "https://jsonplaceholder.typicode.com/users/1", false, false));

        // 5 concurrent requests hit the constrained external pool
        for (int i = 1; i <= 5; i++) {
            futures.add(executeTask(i, externalApiBulkhead, circuitBreaker, retry,
                    "https://jsonplaceholder.typicode.com/users/" + i, true, false));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        Thread.sleep(1000);

        // -------------------------------------------------------------------------
        // DEMO 2: Transient Errors -> Exponential Backoff + Jitter Retries
        // -------------------------------------------------------------------------
        System.out.println("\n=================================================================");
        System.out.println("TEST 2: Exponential Backoff with Jitter in Action");
        System.out.println("=================================================================");

        // Simulating a transient network blip (fails twice, succeeds on attempt 3)
        executeTask(201, externalApiBulkhead, circuitBreaker, retry,
                "https://jsonplaceholder.typicode.com/users/3", false, true).join();

        Thread.sleep(1000);

        // -------------------------------------------------------------------------
        // DEMO 3: Persistent Outage -> Circuit Breaker Trips to OPEN
        // -------------------------------------------------------------------------
        System.out.println("\n=================================================================");
        System.out.println("TEST 3: Downstream Outage -> Circuit Breaker OPEN");
        System.out.println("=================================================================");

        // Hit 4 missing routes to breach the 50% failure rate threshold
        for (int i = 1; i <= 4; i++) {
            executeTask(300 + i, externalApiBulkhead, circuitBreaker, retry,
                    "https://jsonplaceholder.typicode.com/invalid_404_route/" + i, false, false).join();
        }

        // -------------------------------------------------------------------------
        // DEMO 4: Fast-Failure (Call Blocked Before Reaching Network)
        // -------------------------------------------------------------------------
        System.out.println("\n=================================================================");
        System.out.println("TEST 4: Fast-Failing (Zero Network I/O)");
        System.out.println("=================================================================");

        executeTask(401, externalApiBulkhead, circuitBreaker, retry,
                "https://jsonplaceholder.typicode.com/users/1", false, false).join();

        // Teardown
        coreBulkhead.close();
        externalApiBulkhead.close();
    }

    private static CompletableFuture<Void> executeTask(
            int reqId,
            ThreadPoolBulkhead bulkhead,
            CircuitBreaker cb,
            Retry retry,
            String url,
            boolean simulateSlowNetwork,
            boolean simulateFlakyNetwork) {

        // Raw business call
        Supplier<String> rawHttpCall = () -> {
            if (simulateSlowNetwork) {
                try { Thread.sleep(400); } catch (InterruptedException ignored) {}
            }
            if (simulateFlakyNetwork) {
                if (ThreadLocalRandom.current().nextInt(10) > 3) {
                    throw new RuntimeException("Transient Connection Timeout");
                }
            }
            return fetchHttp(url);
        };

        Supplier<String> retryDecorated = Retry.decorateSupplier(retry, rawHttpCall);
        Supplier<String> cbDecorated = CircuitBreaker.decorateSupplier(cb, retryDecorated);

        CompletionStage<String> stage;
        try {
            stage = bulkhead.executeSupplier(cbDecorated);
        } catch (BulkheadFullException e) {
            // Caught synchronously when bulkhead pool & bounded queue reject submission
            stage = CompletableFuture.failedStage(e);
        }

        return stage.toCompletableFuture()
                .thenAccept(data -> System.out.printf("[Req #%03d SUCCESS]  Thread: %-32s | Result: %s%n",
                        reqId, Thread.currentThread().getName(), data))
                .exceptionally(throwable -> {
                    Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
                    String reason;

                    if (cause instanceof BulkheadFullException) {
                        reason = "BULKHEAD_FULL (Thread pool & queue saturated - rejected fast)";
                    } else if (cause instanceof CallNotPermittedException) {
                        reason = "CIRCUIT_OPEN (Breaker tripped - zero socket I/O)";
                    } else {
                        reason = "FALLBACK_TRIGGERED (" + cause.getMessage() + ")";
                    }

                    System.out.printf("[Req #%03d FALLBACK] Thread: %-32s | Fallback: %s%n",
                            reqId, Thread.currentThread().getName(), reason);
                    return null;
                });
    }

    private static String fetchHttp(String targetUrl) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();

            HttpResponse<String> resp = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 400) {
                throw new RuntimeException("HTTP Status " + resp.statusCode());
            }
            return "200 OK -> " + resp.body().replaceAll("\\s+", " ").substring(0, Math.min(35, resp.body().length())) + "...";
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}