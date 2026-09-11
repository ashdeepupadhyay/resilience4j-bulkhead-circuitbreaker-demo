package com.backend.resilience.scenario;

import com.backend.resilience.client.HttpServiceClient;
import com.backend.resilience.config.ResilienceConfig;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

public class ResilienceScenarios {

    public static void runTest1_BulkheadIsolation(
            ThreadPoolExecutor appServerPool,
            ThreadPoolBulkhead coreBulkhead,
            ThreadPoolBulkhead externalBulkhead,
            CircuitBreaker cb,
            Retry retry) {

        System.out.println("========================================================================================================================");
        System.out.println("TEST 1: Bulkhead Isolation & Inbound App Thread Allocation");
        System.out.println("App Server Pool: 10 threads | External Bulkhead Pool: 2 threads (Queue: 1) | Core Bulkhead Pool: 5 threads");
        System.out.println("Firing 1 Core call + 5 External calls concurrently across App Server Threads...");
        System.out.println("========================================================================================================================");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Core call
        futures.add(submitInboundRequest(100, appServerPool, coreBulkhead, cb, retry,
                "https://jsonplaceholder.typicode.com/users/1", false, false));

        // 5 concurrent external calls (Pool=2, Queue=1 -> 2 will be rejected immediately)
        for (int i = 1; i <= 5; i++) {
            futures.add(submitInboundRequest(i, appServerPool, externalBulkhead, cb, retry,
                    "https://jsonplaceholder.typicode.com/users/" + i, true, false));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public static void runTest2_RetryWithJitter(
            ThreadPoolExecutor appServerPool,
            ThreadPoolBulkhead externalBulkhead,
            CircuitBreaker cb,
            Retry retry) {

        System.out.println("\n========================================================================================================================");
        System.out.println("TEST 2: Exponential Backoff with Jitter in Action");
        System.out.println("========================================================================================================================");

        submitInboundRequest(201, appServerPool, externalBulkhead, cb, retry,
                "https://jsonplaceholder.typicode.com/users/3", false, true).join();
    }

    public static void runTest3_CircuitBreakerTripping(
            ThreadPoolExecutor appServerPool,
            ThreadPoolBulkhead externalBulkhead,
            CircuitBreaker cb,
            Retry retry) {

        System.out.println("\n========================================================================================================================");
        System.out.println("TEST 3: Downstream Outage -> Circuit Breaker OPEN");
        System.out.println("========================================================================================================================");

        for (int i = 1; i <= 4; i++) {
            submitInboundRequest(300 + i, appServerPool, externalBulkhead, cb, retry,
                    "https://jsonplaceholder.typicode.com/invalid_404_route/" + i, false, false).join();
        }
    }

    public static void runTest4_FastFailing(
            ThreadPoolExecutor appServerPool,
            ThreadPoolBulkhead externalBulkhead,
            CircuitBreaker cb,
            Retry retry) {

        System.out.println("\n========================================================================================================================");
        System.out.println("TEST 4: Fast-Failing (Zero Network I/O)");
        System.out.println("========================================================================================================================");

        submitInboundRequest(401, appServerPool, externalBulkhead, cb, retry,
                "https://jsonplaceholder.typicode.com/users/1", false, false).join();
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
            int remaining = Math.max(0, ResilienceConfig.APP_POOL_CAPACITY - active);
            String threadStats = String.format("[App Pool: %d/10 Active | %d Free]", active, remaining);

            HttpServiceClient.execute(reqId, inboundAppThread, threadStats, bulkhead, cb, retry, url, simulateSlowNetwork, simulateFlakyNetwork);
        }, appServerPool);
    }
}