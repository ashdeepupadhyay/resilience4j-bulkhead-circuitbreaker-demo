package com.backend.resilience;

import com.backend.resilience.config.ResilienceConfig;
import com.backend.resilience.scenario.ResilienceScenarios;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;

import java.util.concurrent.ThreadPoolExecutor;

public class ResilienceDemoApp {

    public static void main(String[] args) throws Exception {

        // Initialize components
        ThreadPoolExecutor appServerPool = ResilienceConfig.createAppServerPool();
        ThreadPoolBulkhead coreBulkhead = ResilienceConfig.createCoreBulkhead();
        ThreadPoolBulkhead externalBulkhead = ResilienceConfig.createExternalBulkhead();
        Retry retry = ResilienceConfig.createRetryWithJitter();
        CircuitBreaker circuitBreaker = ResilienceConfig.createCircuitBreaker();

        try {
            // Run Scenario 1: Bulkhead isolation & fast queue rejection
            ResilienceScenarios.runTest1_BulkheadIsolation(appServerPool, coreBulkhead, externalBulkhead, circuitBreaker, retry);
            Thread.sleep(1000);

            // Run Scenario 2: Exponential backoff with random jitter
            ResilienceScenarios.runTest2_RetryWithJitter(appServerPool, externalBulkhead, circuitBreaker, retry);
            Thread.sleep(1000);

            // Run Scenario 3: Downstream failure tripping breaker to OPEN
            ResilienceScenarios.runTest3_CircuitBreakerTripping(appServerPool, externalBulkhead, circuitBreaker, retry);

            // Run Scenario 4: Fast failing with zero network I/O
            ResilienceScenarios.runTest4_FastFailing(appServerPool, externalBulkhead, circuitBreaker, retry);

        } finally {
            // Graceful Teardown
            coreBulkhead.close();
            externalBulkhead.close();
            appServerPool.shutdown();
        }
    }
}