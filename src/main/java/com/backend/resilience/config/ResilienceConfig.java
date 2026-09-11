package com.backend.resilience.config;

import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ResilienceConfig {

    public static final int APP_POOL_CAPACITY = 10;

    public static ThreadPoolExecutor createAppServerPool() {
        return new ThreadPoolExecutor(
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
    }

    public static ThreadPoolBulkhead createCoreBulkhead() {
        return ThreadPoolBulkhead.of("core-service-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(5)
                        .maxThreadPoolSize(5)
                        .queueCapacity(10)
                        .build());
    }

    public static ThreadPoolBulkhead createExternalBulkhead() {
        return ThreadPoolBulkhead.of("external-api-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(2)
                        .maxThreadPoolSize(2)
                        .queueCapacity(1)
                        .build());
    }

    public static Retry createRetryWithJitter() {
        IntervalFunction backoffWithJitter = IntervalFunction.ofExponentialRandomBackoff(
                Duration.ofMillis(200), 2.0, 0.5);

        Retry retry = Retry.of("externalApiRetry", RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(backoffWithJitter)
                .retryExceptions(RuntimeException.class)
                .build());

        retry.getEventPublisher().onRetry(e ->
                System.out.printf("   [RETRY EVENT] Attempt #%d for %s (Wait: %dms)%n",
                        e.getNumberOfRetryAttempts(), e.getName(), e.getWaitInterval().toMillis()));

        return retry;
    }

    public static CircuitBreaker createCircuitBreaker() {
        CircuitBreaker cb = CircuitBreaker.of("externalApiBreaker", CircuitBreakerConfig.custom()
                .slidingWindowSize(4)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(3))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build());

        cb.getEventPublisher().onStateTransition(event ->
                System.out.printf("%n>>> [CIRCUIT BREAKER STATE CHANGE] -> %s <<<%n%n", event.getStateTransition()));

        return cb;
    }
}