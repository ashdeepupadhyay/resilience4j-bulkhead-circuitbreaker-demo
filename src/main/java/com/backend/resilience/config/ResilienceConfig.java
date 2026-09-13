package com.backend.resilience.config;

import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ResilienceConfig {

    public static final int APP_POOL_CAPACITY = 10;
    private static final Map<String, Object> CONFIG_MAP = loadYamlConfiguration();

    // -------------------------------------------------------------------------
    // Inbound Application Server Pool
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Core Service Bulkhead Pool
    // -------------------------------------------------------------------------
    public static ThreadPoolBulkhead createCoreBulkhead() {
        return ThreadPoolBulkhead.of("core-service-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(5)
                        .maxThreadPoolSize(5)
                        .queueCapacity(10)
                        .build());
    }

    // -------------------------------------------------------------------------
    // External / Kitchen Service Bulkhead (Loaded from YAML)
    // -------------------------------------------------------------------------
    public static ThreadPoolBulkhead createExternalBulkhead() {
        Map<String, Object> cfg = getInstanceConfig("thread-pool-bulkhead", "kitchenService");

        int coreThreads = getInt(cfg, "coreThreadPoolSize", 2);
        int maxThreads = getInt(cfg, "maxThreadPoolSize", 2);
        int queueCapacity = getInt(cfg, "queueCapacity", 1);

        return ThreadPoolBulkhead.of("external-api-pool",
                ThreadPoolBulkheadConfig.custom()
                        .coreThreadPoolSize(coreThreads)
                        .maxThreadPoolSize(maxThreads)
                        .queueCapacity(queueCapacity)
                        .build());
    }

    // -------------------------------------------------------------------------
    // Retry with Exponential Backoff and Jitter (Loaded from YAML)
    // -------------------------------------------------------------------------
    public static Retry createRetryWithJitter() {
        Map<String, Object> cfg = getInstanceConfig("retry", "kitchenService");

        int maxAttempts = getInt(cfg, "maxAttempts", 3);
        Duration waitDuration = parseDuration(cfg.get("waitDuration"), Duration.ofMillis(200));
        double multiplier = getDouble(cfg, "exponentialBackoffMultiplier", 2.0);
        double jitterFactor = getDouble(cfg, "randomizedWaitFactor", 0.5);

        IntervalFunction backoffWithJitter = IntervalFunction.ofExponentialRandomBackoff(
                waitDuration, multiplier, jitterFactor);

        Retry retry = Retry.of("externalApiRetry", RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .intervalFunction(backoffWithJitter)
                .retryExceptions(RuntimeException.class)
                .build());

        retry.getEventPublisher().onRetry(e ->
                System.out.printf("   [RETRY EVENT] Attempt #%d for %s (Wait: %dms)%n",
                        e.getNumberOfRetryAttempts(), e.getName(), e.getWaitInterval().toMillis()));

        return retry;
    }

    // -------------------------------------------------------------------------
    // Circuit Breaker (Loaded from YAML)
    // -------------------------------------------------------------------------
    public static CircuitBreaker createCircuitBreaker() {
        Map<String, Object> cfg = getInstanceConfig("circuitbreaker", "kitchenService");

        int windowSize = getInt(cfg, "slidingWindowSize", 4);
        float failureThreshold = (float) getDouble(cfg, "failureRateThreshold", 50.0);
        Duration waitInOpen = parseDuration(cfg.get("waitDurationInOpenState"), Duration.ofSeconds(3));
        int permittedInHalfOpen = getInt(cfg, "permittedNumberOfCallsInHalfOpenState", 2);

        CircuitBreaker cb = CircuitBreaker.of("externalApiBreaker", CircuitBreakerConfig.custom()
                .slidingWindowSize(windowSize)
                .failureRateThreshold(failureThreshold)
                .waitDurationInOpenState(waitInOpen)
                .permittedNumberOfCallsInHalfOpenState(permittedInHalfOpen)
                .build());

        cb.getEventPublisher().onStateTransition(event ->
                System.out.printf("%n>>> [CIRCUIT BREAKER STATE CHANGE] -> %s <<<%n%n", event.getStateTransition()));

        return cb;
    }

    // -------------------------------------------------------------------------
    // YAML Loader & Helper Utilities
    // -------------------------------------------------------------------------
    private static Map<String, Object> loadYamlConfiguration() {
        ClassLoader loader = ResilienceConfig.class.getClassLoader();
        // Check for application.yml first, then application.yaml
        InputStream is = loader.getResourceAsStream("application.yml");
        if (is == null) {
            is = loader.getResourceAsStream("application.yaml");
        }

        if (is == null) {
            System.err.println("[WARN] Neither application.yml nor application.yaml found on classpath. Falling back to default values.");
            return Collections.emptyMap();
        }

        try (InputStream in = is) {
            Map<String, Object> parsed = new Yaml().load(in);
            return parsed != null ? parsed : Collections.emptyMap();
        } catch (Exception e) {
            System.err.println("[WARN] Error reading YAML configuration: " + e.getMessage() + ". Using defaults.");
            return Collections.emptyMap();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getInstanceConfig(String component, String instanceName) {
        Object r4j = CONFIG_MAP.get("resilience4j");
        if (!(r4j instanceof Map)) return Collections.emptyMap();

        Object compObj = ((Map<String, Object>) r4j).get(component);
        if (!(compObj instanceof Map)) return Collections.emptyMap();

        Object instancesObj = ((Map<String, Object>) compObj).get("instances");
        if (!(instancesObj instanceof Map)) return Collections.emptyMap();

        Map<String, Object> instances = (Map<String, Object>) instancesObj;
        Object instance = instances.get(instanceName);
        if (instance instanceof Map) {
            return (Map<String, Object>) instance;
        }

        // If the specific instance name isn't matched, read the first available instance
        return instances.values().stream()
                .filter(Map.class::isInstance)
                .map(m -> (Map<String, Object>) m)
                .findFirst()
                .orElse(Collections.emptyMap());
    }

    private static int getInt(Map<String, Object> map, String key, int fallback) {
        Object val = map.get(key);
        return (val instanceof Number) ? ((Number) val).intValue() : fallback;
    }

    private static double getDouble(Map<String, Object> map, String key, double fallback) {
        Object val = map.get(key);
        return (val instanceof Number) ? ((Number) val).doubleValue() : fallback;
    }

    private static Duration parseDuration(Object raw, Duration fallback) {
        if (raw == null) return fallback;
        String str = raw.toString().trim().toLowerCase();
        try {
            if (str.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(str.replace("ms", "").trim()));
            } else if (str.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(str.replace("s", "").trim()));
            } else if (str.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(str.replace("m", "").trim()));
            }
            return Duration.ofMillis(Long.parseLong(str));
        } catch (Exception e) {
            return fallback;
        }
    }
}