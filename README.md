# resilience4j-bulkhead-circuitbreaker-demo
Production-ready reference implementation of Microservice Fault Tolerance in Java using Resilience4j: ThreadPool Bulkheads, Circuit Breakers, Exponential Backoff with Jitter, and Graceful Fallbacks.


A production-grade reference implementation demonstrating how distributed systems isolate failures, prevent cascading outages, and maintain graceful degradation using **Resilience4j** and **Java 21**.

---

## 1. High-Level Concept: The Restaurant Kitchen Analogy

Consider a busy restaurant kitchen:

* You have **10 line cooks** (the inbound application server worker threads) processing customer orders.
* One dish requires **fresh seafood delivered by an external vendor** (a slow 3rd-party remote API).
* The vendor's delivery vehicle gets stuck in traffic.

If all 10 cooks abandon their prep stations, walk to the loading dock, and wait for the delivery:
1. **The entire kitchen freezes.**
2. Customers who only ordered pasta, salad, or soup starve because no cooks remain to prepare regular food.
3. The restaurant fails not because internal recipes failed, but because an external dependency starved shared worker capacity.

In software, when downstream endpoints degrade, unconstrained blocking network calls starve the inbound application server thread pool (such as Tomcat or Jetty), taking down the entire service fleet.

---

## 2. Two-Tier Thread Architecture

To prevent thread starvation, the architecture splits execution into two decoupled tiers:

```text
                     [Inbound Client Traffic]
                                │
                                ▼
    [Tier 1: Application Server Pool] (app-server-worker-1..10)

    • Managed by application container (Jetty / Tomcat).
    • Accepts incoming HTTP requests, handles parsing and routing.
                                │
        ┌───────────────────────┴───────────────────────┐
        │                                               │
        ▼                                               ▼
[Tier 2A: Core Bulkhead Pool]           [Tier 2B: External Bulkhead Pool]

• Name: core-service-pool               • Name: external-api-pool
• Core/Max: 5 threads                   • Core/Max: 2 threads
• Queue Capacity: 10                    • Queue Capacity: 1
• Purpose: Protects internal paths      • Purpose: Constrains 3rd-party risk
```


### The Inbound Handoff Lifecycle
1. **Submission:** An application thread (`app-server-worker-X`) accepts the inbound request and dispatches the task to the dedicated bulkhead pool via `bulkhead.executeSupplier(...)`.
2. **Acceptance:** If the bulkhead has capacity, the task is enqueued or immediately picked up by an isolated worker (`bulkhead-external-api-pool-Y`).
3. **Instant Rejection:** If both the bulkhead worker threads and the bounded queue are full, `BulkheadFullException` is thrown **synchronously on the application thread**. The app thread never blocks on a network socket; it catches the exception in sub-milliseconds, returns a fallback response, and returns immediately to the pool to serve subsequent users.

---

## 3. Configuration Parameters Explained

```java
ThreadPoolBulkhead coreBulkhead = ThreadPoolBulkhead.of("core-service-pool",
        ThreadPoolBulkheadConfig.custom()
                .coreThreadPoolSize(5)
                .maxThreadPoolSize(5)
                .queueCapacity(10)
                .build());
```
- #### coreThreadPoolSize :
    -  The baseline number of worker threads kept alive in the pool.
    - In Resilience4j, these threads remain allocated and ready to process incoming tasks immediately, avoiding the latency overhead of on-demand thread creation.
- #### maxThreadPoolSize:
    -  The upper limit of execution threads the pool can allocate under peak load.
    -  Setting coreThreadPoolSize == maxThreadPoolSize creates a fixed-size thread pool, which is the production standard for bulkheads. It enforces a strict, predictable ceiling on concurrent compute resources allocated to a given dependency.
- #### queueCapacity:
    - The capacity limit of the internal ArrayBlockingQueue holding tasks when all worker threads are active.
    - Total concurrency capacity before rejection is defined as:
  ```text
    Capacity = maxThreadPoolSize + queueCapacity
  ```
    - For coreBulkhead: $5 + 10 = 15$ concurrent requests allowed.
    - For externalApiBulkhead: $2 + 1 = 3$ concurrent requests allowed. Any 4th concurrent submission is rejected immediately.
- #### TimeUnit
    - An enum in java.util.concurrent defining the granularity of time (e.g., TimeUnit.MILLISECONDS, TimeUnit.SECONDS).
    - In new ThreadPoolExecutor(..., 0L, TimeUnit.MILLISECONDS, ...), 0L, TimeUnit.MILLISECONDS specifies the keepAliveTime for threads exceeding the core count. Because the pool is fixed-size, excess threads never exist, making 0ms the standard baseline setting
 
## 4. Segregated Repository Structure
To maintain single-responsibility principles, the implementation is organized into four modular components:

```text
src/main/java/com/backend/resilience/
├── ResilienceDemoApp.java          # Orchestrator running scenarios sequentially
├── config/
│   └── ResilienceConfig.java       # Component factories and pool definitions
├── client/
│   └── HttpServiceClient.java      # Pipeline decoration, execution, and fallbacks
└── scenario/
    └── ResilienceScenarios.java    # Isolated test cases
```

### Decorator Ordering Pipeline
The decoration order in HttpServiceClient determines the operational hierarchy:

```java
Supplier<String> retryDecorated = Retry.decorateSupplier(retry, rawHttpCall);
Supplier<String> cbDecorated = CircuitBreaker.decorateSupplier(cb, retryDecorated);
stage = bulkhead.executeSupplier(cbDecorated);
```

```text
Caller (App Thread)
  │
  ▼
[ThreadPool Bulkhead]   --> Enforces concurrency and bounded queue limits
  │
  ▼
[Circuit Breaker]       --> Short-circuits calls if failure threshold is reached
  │
  ▼
[Retry + Jitter]        --> Retries transient network failures with backoff
  │
  ▼
[HTTP Client Call]      --> Outbound Network I/O
```

## 5. Line-by-Line Execution & Output Analysis

### 1: Bulkhead Isolation & Inbound Pool AllocationConsole
    - Console Output
      ```text
      ========================================================================================================================
      TEST 1: Bulkhead Isolation & Inbound App Thread Allocation
      App Server Pool: 10 threads | External Bulkhead Pool: 2 threads (Queue: 1) | Core Bulkhead Pool: 5 threads
      Firing 1 Core call + 5 External calls concurrently across App Server Threads...
      ========================================================================================================================
      [Req #004 FALLBACK] Inbound: app-server-worker-5  [App Pool: 6/10 Active | 4 Free] | Handled On:  app-server-worker-5              | BULKHEAD_FULL (Instant fast-rejection -> App thread freed)
      [Req #003 FALLBACK] Inbound: app-server-worker-4  [App Pool: 5/10 Active | 5 Free] | Handled On:  app-server-worker-4              | BULKHEAD_FULL (Instant fast-rejection -> App thread freed)
      [Req #100 SUCCESS]  Inbound: app-server-worker-1  [App Pool: 1/10 Active | 9 Free] | Worker Pool: bulkhead-core-service-pool-1     | 200 OK -> { "id": 1, "name": "Leann...
      [Req #005 SUCCESS]  Inbound: app-server-worker-6  [App Pool: 6/10 Active | 4 Free] | Worker Pool: bulkhead-external-api-pool-2     | 200 OK -> { "id": 5, "name": "Chels...
      [Req #001 SUCCESS]  Inbound: app-server-worker-2  [App Pool: 4/10 Active | 6 Free] | Worker Pool: bulkhead-external-api-pool-1     | 200 OK -> { "id": 1, "name": "Leann...
      [Req #002 SUCCESS]  Inbound: app-server-worker-3  [App Pool: 4/10 Active | 6 Free] | Worker Pool: bulkhead-external-api-pool-2     | 200 OK -> { "id": 2, "name": "Ervin...
      ```
 ## Line-by-Line Mechanics

1. `[Req #004 FALLBACK]` & `[Req #003 FALLBACK]` **(Lines 1 & 2):**
   * **Source Code:**
     ```java
     try {
         stage = bulkhead.executeSupplier(cbDecorated);
     } catch (BulkheadFullException e) {
         stage = CompletableFuture.failedStage(e);
     }
     ```
   * **Mechanism:** 5 external requests hit the external bulkhead concurrently. The bulkhead permits only 3 requests (2 executing + 1 queued). When Requests #4 and #3 arrived, capacity was exhausted.
   * `BulkheadFullException` was thrown **synchronously on the application threads** (`app-server-worker-5` and `app-server-worker-4`).
   * **Why they printed first:** The accepted requests had to perform 400ms network calls. Rejections took < 0.1 ms. The application threads served fallbacks immediately and returned to the pool while external calls were still in flight.

2. `[Req #100 SUCCESS]` **(Line 3):**
   * Handled by `app-server-worker-1` and executed on `bulkhead-core-service-pool-1`.
   * Despite complete saturation on the external pool, core requests ran unimpeded on their own isolated thread pool.

3. `[Req #005, #001, #002 SUCCESS]` **(Lines 4, 5, 6):**
   * Handled inbound by application workers, executed outbound on `bulkhead-external-api-pool-1` and `bulkhead-external-api-pool-2`.
      

## TEST 2: Exponential Backoff with Jitter in Action

### Console Output

```plaintext
========================================================================================================================
TEST 2: Exponential Backoff with Jitter in Action
========================================================================================================================
   [RETRY EVENT] Attempt #1 for externalApiRetry (Wait: 211ms)
[Req #201 SUCCESS]  Inbound: app-server-worker-7  [App Pool: 1/10 Active | 9 Free] | Worker Pool: bulkhead-external-api-pool-1     | 200 OK -> { "id": 3, "name": "Cleme...
```
## Line-by-Line Mechanics
Source Code:
```java
IntervalFunction backoffWithJitter = IntervalFunction.ofExponentialRandomBackoff(
        Duration.ofMillis(200), 2.0, 0.5);
```
- Attempt 0 simulated a transient connection failure.

- The retry interceptor calculated the backoff with randomized jitter:
- 
   ```text
    Wait Interval ∈ [200 × (1 - 0.5), 200 × (1 + 0.5)] = [100ms, 300ms]
    ```
- The delay resolved to 211ms. Attempt 1 succeeded on bulkhead-external-api-pool-1, preventing synchronized retry waves (thundering herd).

- 200ms (Base Interval): The scheduled baseline delay for the first retry attempt.
- 0.5 (Randomization Factor): The allowable variance percentage ($\pm 50\%$) around the base duration.
- Lower Bound:
  ```text
  200 × (1 - 0.5) = 200 × 0.5 = 100ms
  ```
- Upper Bound:
   ```text
  200 × (1 + 0.5) = 200 × 1.5 = 300ms
  ```
   The latest time the first retry can fire.
- ∈ [100ms, 300ms]: The actual wait duration is picked at random from a uniform distribution across this range (e.g., 211ms).

- #### Progression Across Subsequent Retries :
    - Because the multiplier is 2.0, the base interval doubles on each failed attempt while maintaining the same $\pm 50\%$ spread:
  ```text
  +---------+---------------+--------------------------+-----------------------+
  | Attempt | Base Interval | Formula Calculation      | Possible Delay Range  |
  +---------+---------------+--------------------------+-----------------------+
  | #1      | 200ms         | [200 × 0.5, 200 × 1.5]   | [100ms, 300ms]        |
  | #2      | 400ms         | [400 × 0.5, 400 × 1.5]   | [200ms, 600ms]        |
  | #3      | 800ms         | [800 × 0.5, 800 × 1.5]   | [400ms, 1200ms]       |
  | #4      | 1600ms        | [1600 × 0.5, 1600 × 1.5] | [800ms, 2400ms]       |
  +---------+---------------+--------------------------+-----------------------+
  ```
- #### Why Jitter Matters:
    - Solving the Thundering HerdSuppose 500 concurrent HTTP requests fail simultaneously at time t = 0\text{ms}
    - ##### Without Jitter (Synchronized Retry Spike)
        - All 500 threads pause for exactly $200\text{ms}$ and fire together, knocking down the recovering server again:
     ```text
          Time (ms):  0ms                   200ms
            |          |                       |
          Traffic:    [500 Failed] ---------> [500 Retries Hit Simultaneously!] (Crash)
     ```
  - ##### With Jitter (Distributed Traffic Flow)
      - The 500 threads spread their retries across the entire interval, turning a destructive traffic wall into a manageable trickle:
     ```text
        Time (ms):  0ms         100ms        150ms        211ms        280ms        300ms
            |        |            |            |            |            |
        Traffic:    [500 Failed]  (12 reqs)    (45 reqs)    (88 reqs)    (30 reqs)    ...
    ```
## TEST 3: Downstream Outage -> Circuit Breaker OPEN
```text
========================================================================================================================
TEST 3: Downstream Outage -> Circuit Breaker OPEN
========================================================================================================================
   [RETRY EVENT] Attempt #1 for externalApiRetry (Wait: 243ms)
   [RETRY EVENT] Attempt #2 for externalApiRetry (Wait: 574ms)
[Req #301 FALLBACK] Inbound: app-server-worker-8  [App Pool: 1/10 Active | 9 Free] | Handled On:  bulkhead-external-api-pool-2     | FALLBACK (HTTP Status 404)
   [RETRY EVENT] Attempt #1 for externalApiRetry (Wait: 205ms)
   [RETRY EVENT] Attempt #2 for externalApiRetry (Wait: 360ms)

>>> [CIRCUIT BREAKER STATE CHANGE] -> State transition from CLOSED to OPEN <<<

[Req #302 FALLBACK] Inbound: app-server-worker-9  [App Pool: 1/10 Active | 9 Free] | Handled On:  bulkhead-external-api-pool-1     | FALLBACK (HTTP Status 404)
[Req #303 FALLBACK] Inbound: app-server-worker-10 [App Pool: 1/10 Active | 9 Free] | Handled On:  bulkhead-external-api-pool-2     | CIRCUIT_OPEN (Breaker tripped -> zero socket I/O)
[Req #304 FALLBACK] Inbound: app-server-worker-5  [App Pool: 1/10 Active | 9 Free] | Handled On:  bulkhead-external-api-pool-1     | CIRCUIT_OPEN (Breaker tripped -> zero socket I/O)
```
### Line-by-Line Mechanics

1. **Requests #301 and #302:** Targeted an invalid route returning HTTP 404. Both exhausted retries ($243\text{ms} \rightarrow 574\text{ms}$ and $205\text{ms} \rightarrow 360\text{ms}$) before failing.

2. **State Transition (`CLOSED -> OPEN`):**
   * Configured sliding window: 4 calls, failure threshold: 50%.
   * 2 evaluated calls both failed (100% error rate $\ge 50\%$). The breaker tripped to `OPEN`.

3. **Requests #303 and #304:**
   * **Source Code:**
     ```java
     else if (cause instanceof CallNotPermittedException) {
         reason = "CIRCUIT_OPEN (Breaker tripped -> zero socket I/O)";
     }
     ```
   * Notice that **no retry events appeared**. The open breaker intercepted the calls before invocation, avoiding network calls and resource consumption entirely.
  

## TEST 4: Fast-Failing (Zero Network I/O)
```text
========================================================================================================================
TEST 4: Fast-Failing (Zero Network I/O)
========================================================================================================================
[Req #401 FALLBACK] Inbound: app-server-worker-4  [App Pool: 1/10 Active | 9 Free] | Handled On:  bulkhead-external-api-pool-2     | CIRCUIT_OPEN (Breaker tripped -> zero socket I/O)
```
### Line-by-Line Mechanics

* `Req #401` targeted a valid URL (`/users/1`).
* Because the breaker was in its cooldown window in the `OPEN` state, it immediately threw `CallNotPermittedException`.
* Response completed in < 1ms, serving a fallback without network interaction.

---

## 6. How to Run

### Prerequisites

* JDK 21+
* Apache Maven 3.8+
* Git

```bash
java -version
mvn -version
git --version
```
#### Command Line Execution
```bash
# 1. Clone repository
git clone [https://github.com/ashdeepupadhyay/resilience4j-bulkhead-circuitbreaker-demo.git](https://github.com/ashdeepupadhyay/resilience4j-bulkhead-circuitbreaker-demo.git)
cd resilience4j-bulkhead-circuitbreaker-demo

# 2. Compile and execute
mvn clean compile exec:java -Dexec.mainClass="com.backend.resilience.ResilienceDemoApp"
```

#### IntelliJ IDEA Execution

- Open the project root or its pom.xml (File → Open...).

- Reload Maven dependencies via the Maven tool window.

- Verify Project SDK is set to Java 21 (File → Project Structure).

- Navigate to src/main/java/com/backend/resilience/ResilienceDemoApp.java.

- Click the green Run icon next to the main method.
<img width="1014" height="359" alt="image" src="https://github.com/user-attachments/assets/0de1fa0c-843d-42a5-b1e8-7571cac18791" />

