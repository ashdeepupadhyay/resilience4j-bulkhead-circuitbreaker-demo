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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ResilienceDemoApp {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(1500))
            .build();

    private static final int APP_POOL_CAPACITY = 10;

    public static void main(String[] args) throws Exception {

        // =========================================================================
        // 0. APPLICATION SERVER THREAD POOL (Simulating Tomcat / Jetty Worker Threads)
        // =========================================================================
        ThreadPoolExecutor appServerPool = new ThreadPoolExecutor(
                APP_POOL_CAPACITY, APP_POOL_CAPACITY,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "app-server-worker-" + counter.getAndIncrement());
                    }
                }
        );

        // =========================================================================
        // 1. ISOLATED BULKHEAD THREAD POOLS (Resilience4j)
        // =========================================================================
        // Core Internal Pool: High priority, dedicated capacity
        ThreadPoolBulkhead coreBulkhead = ThreadPoolBulkhead.of("core-service-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(5)
                        .maxThreadPoolSize(5)
                        .queueCapacity(10)
                        .build());

        // 3rd-Party External Pool: Constrained to 2 threads, 1 queue slot (Total capacity = 3)
        ThreadPoolBulkhead externalApiBulkhead = ThreadPoolBulkhead.of("external-api-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(2)
                        .maxThreadPoolSize(2)
                        .queueCapacity(1)
                        .build());

        // =========================================================================
        // 2. RETRY WITH EXPONENTIAL BACKOFF + RANDOM JITTER
        // =========================================================================
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
                .onRetry(e -> System.out.printf("   [RETRY EVENT] Attempt #%d for %s (Wait: %dms)%n",
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
                        System.out.printf("%n>>> [CIRCUIT BREAKER STATE CHANGE] -> %s <<<%n%n", event.getStateTransition()));


        // -------------------------------------------------------------------------
        // TEST 1: Dual Bulkhead Isolation (Core vs External) & Inbound App Threads
        // -------------------------------------------------------------------------
        System.out.println("========================================================================================================================");
        System.out.println("TEST 1: Bulkhead Isolation & Inbound App Thread Allocation");
        System.out.println("App Server Pool: 10 threads | External Bulkhead Pool: 2 threads (Queue: 1) | Core Bulkhead Pool: 5 threads");
        System.out.println("Firing 1 Core call + 5 External calls concurrently across App Server Threads...");
        System.out.println("========================================================================================================================");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Inbound request for Core Service -> executes via coreBulkhead
        futures.add(submitInboundRequest(100, appServerPool, coreBulkhead, circuitBreaker, retry,
                "https://jsonplaceholder.typicode.com/users/1", false, false));

        // 5 concurrent inbound requests for External API -> execute via externalApiBulkhead
        for (int i = 1; i <= 5; i++) {
            futures.add(submitInboundRequest(i, appServerPool, externalApiBulkhead, circuitBreaker, retry,
                    "https://jsonplaceholder.typicode.com/users/" + i, true, false));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        Thread.sleep(1000);

        // -------------------------------------------------------------------------
        // TEST 2: Transient Errors -> Exponential Backoff + Jitter Retries
        // -------------------------------------------------------------------------
        System.out.println("\n========================================================================================================================");
        System.out.println("TEST 2: Exponential Backoff with Jitter in Action");
        System.out.println("========================================================================================================================");

        submitInboundRequest(201, appServerPool, externalApiBulkhead, circuitBreaker, retry,
                "https://jsonplaceholder.typicode.com/users/3", false, true).join();

        Thread.sleep(1000);

        // -------------------------------------------------------------------------
        // TEST 3: Downstream Outage -> Circuit Breaker Trips to OPEN
        // -------------------------------------------------------------------------
        System.out.println("\n========================================================================================================================");
        System.out.println("TEST 3: Downstream Outage -> Circuit Breaker OPEN");
        System.out.println("========================================================================================================================");

        for (int i = 1; i <= 4; i++) {
            submitInboundRequest(300 + i, appServerPool, externalApiBulkhead, circuitBreaker, retry,
                    "https://jsonplaceholder.typicode.com/invalid_404_route/" + i, false, false).join();
        }

        // -------------------------------------------------------------------------
        // TEST 4: Fast-Failing (Zero Network I/O)
        // -------------------------------------------------------------------------
        System.out.println("\n========================================================================================================================");
        System.out.println("TEST 4: Fast-Failing (Zero Network I/O)");
        System.out.println("========================================================================================================================");

        submitInboundRequest(401, appServerPool, externalApiBulkhead, circuitBreaker, retry,
                "https://jsonplaceholder.typicode.com/users/1", false, false).join();

        // Teardown
        try {
            coreBulkhead.close();
            externalApiBulkhead.close();
            appServerPool.shutdown();
        } catch (Exception ignored) {}
    }

    private static CompletableFuture<Void> submitInboundRequest(
            int reqId,
            ThreadPoolExecutor appServerPool,
            ThreadPoolBulkhead bulkhead,
            CircuitBreaker cb,
            Retry retry,
            String url,
            boolean simulateSlowNetwork,
            boolean simulateFlakyNetwork) {

        return CompletableFuture.runAsync(() -> {
            String inboundAppThread = Thread.currentThread().getName();
            int active = appServerPool.getActiveCount();
            int remaining = Math.max(0, APP_POOL_CAPACITY - active);
            String threadStats = String.format("[App Pool: %d/10 Active | %d Free]", active, remaining);

            executeTask(reqId, inboundAppThread, threadStats, bulkhead, cb, retry, url, simulateSlowNetwork, simulateFlakyNetwork);
        }, appServerPool);
    }

    private static void executeTask(
            int reqId,
            String appThreadName,
            String threadStats,
            ThreadPoolBulkhead bulkhead,
            CircuitBreaker cb,
            Retry retry,
            String url,
            boolean simulateSlowNetwork,
            boolean simulateFlakyNetwork) {

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
            stage = CompletableFuture.failedStage(e);
        }

        stage.toCompletableFuture()
                .thenAccept(data -> System.out.printf("[Req #%03d SUCCESS]  Inbound: %-20s %-32s | Worker Pool: %-32s | %s%n",
                        reqId, appThreadName, threadStats, Thread.currentThread().getName(), data))
                .exceptionally(throwable -> {
                    Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
                    String reason;

                    if (cause instanceof BulkheadFullException) {
                        reason = "BULKHEAD_FULL (Instant fast-rejection -> App thread freed)";
                    } else if (cause instanceof CallNotPermittedException) {
                        reason = "CIRCUIT_OPEN (Breaker tripped -> zero socket I/O)";
                    } else {
                        reason = "FALLBACK (" + cause.getMessage() + ")";
                    }

                    System.out.printf("[Req #%03d FALLBACK] Inbound: %-20s %-32s | Handled On:  %-32s | %s%n",
                            reqId, appThreadName, threadStats, Thread.currentThread().getName(), reason);
                    return null;
                }).join();
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
            return "200 OK -> " + resp.body().replaceAll("\\s+", " ").substring(0, Math.min(25, resp.body().length())) + "...";
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}