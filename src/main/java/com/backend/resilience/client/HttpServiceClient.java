package com.backend.resilience.client;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class HttpServiceClient {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(1500))
            .build();

    public static void execute(
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

        // Pipeline: Bulkhead Pool -> CircuitBreaker -> Retry -> HTTP Call
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