# Distributed Middleware for AI Inference

A distributed middleware system for managing AI inference workers, resource discovery, load-aware request routing, and worker failure detection.

The project is implemented in Python using Flask and simulates inference workloads to evaluate distributed scheduling and fault-tolerance mechanisms.

## 1. Overview

The system consists of three main components:

- **Client:** Submits inference requests to the middleware.
- **Middleware:** Registers workers, discovers compatible resources, monitors worker health, and routes requests.
- **Worker:** Processes simulated inference requests and periodically reports resource and load information.

The middleware separates clients from workers, allowing inference requests to be routed based on model availability, current load, and advertised processing capacity.

## 2. System Architecture

```text
                      +------------------+
                      |      Client      |
                      +---------+--------+
                                |
                         POST /infer
                                |
                                v
                      +------------------+
                      |    Middleware    |
                      |                  |
                      | Worker Registry  |
                      | Resource Lookup  |
                      | Failure Detector |
                      | Routing Policies |
                      +---------+--------+
                                |
                  Select compatible,
                    healthy worker
                                |
              +-----------------+-----------------+
              |                 |                 |
              v                 v                 v
       +-------------+   +-------------+   +-------------+
       |  Worker 1   |   |  Worker 2   |   |  Worker 3   |
       | Capacity: 8 |   | Capacity: 4 |   | Capacity: 2 |
       +-------------+   +-------------+   +-------------+
              |                 |                 |
              +-----------------+-----------------+
                                |
                       Inference responses
```

Workers register with middleware at startup and periodically send heartbeat messages containing their current resource metrics.

When a client submits an inference request, middleware:

1. Identifies workers supporting the requested model.
2. Filters out unavailable workers.
3. Determines which routing policy to use.
4. Selects a compatible worker.
5. Forwards the request and returns its response.

## 3. Project Structure

```text
distributed_middleware/
├── README.md
├── requirements.txt
├── src/
│   ├── client.py
│   ├── middleware/
│   │   ├── middleware.py
│   │   ├── routing.py
│   │   └── failure_detector.py
│   └── worker/
│       ├── worker.py
│       ├── heartbeat.py
│       └── metrics.py
└── tests/
    ├── benchmark_routing.py
    ├── test_metrics.py
    └── test_routing.py
```

Generated benchmark results are stored locally in `results/routing/` and excluded from version control.

## 4. Implemented Features

### Worker Registration and Resource Discovery

Workers register their ID, network address, supported models, advertised capacity, and hardware information.

Middleware maintains an in-memory worker registry and discovers workers capable of processing a requested model.

### Heartbeats and Failure Detection

Workers periodically report their active request counts, advertised capacity, and resource metrics.

Middleware monitors heartbeat timestamps and marks workers unavailable if they stop reporting within the configured timeout.

Unavailable workers remain visible in the registry but are excluded from inference routing.

### Load-Aware Routing

The middleware supports five routing modes:

| Policy           | Description                                                                |
| ---------------- | -------------------------------------------------------------------------- |
| `round_robin`    | Distributes requests sequentially among compatible workers.                |
| `least_loaded`   | Selects the worker with the fewest active requests.                        |
| `capacity_aware` | Selects the worker with the lowest relative utilization.                   |
| `power_of_two`   | Randomly samples two workers and chooses the less-loaded candidate.        |
| `adaptive`       | Automatically selects a routing policy based on current worker conditions. |

The default routing mode is `adaptive`. Individual policies remain available for manual selection and benchmarking.

## 5. Setup

### Requirements

- Python 3
- Flask
- Requests

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run all commands from the project root.

## 6. Running the System

### Step 1 — Start Middleware

Start middleware using adaptive routing:

```bash
python src/middleware/middleware.py --port 8000
```

To select a specific routing policy:

```bash
python src/middleware/middleware.py --port 8000 --routing-policy least_loaded
```

Available policies:

```text
round_robin
least_loaded
capacity_aware
power_of_two
adaptive
```

### Step 2 — Start Workers

Open a separate terminal for each worker.

**Worker 1**

```bash
python src/worker/worker.py --id worker-1 --port 9001 --models model-a --capacity 8
```

**Worker 2**

```bash
python src/worker/worker.py --id worker-2 --port 9002 --models model-a --capacity 4
```

**Worker 3**

```bash
python src/worker/worker.py --id worker-3 --port 9003 --models model-a --capacity 2
```

Each worker registers with middleware automatically.

Workers can advertise multiple models using comma-separated names:

```bash
python src/worker/worker.py --id worker-4 --port 9004 --models model-a,model-b --capacity 4
```

### Step 3 — Submit an Inference Request

```bash
python src/client.py --model model-a --input "Hello world"
```

Example response:

```json
{
  "worker_id": "worker-1",
  "model": "model-a",
  "result": "Simulated inference result from worker-1 using model-a: Hello world"
}
```

The selected worker depends on the routing policy and current system conditions.

## 7. Worker Configuration

Workers support several configurable parameters for simulating different processing capabilities.

| Argument               | Description                               |
| ---------------------- | ----------------------------------------- |
| `--id`                 | Unique worker identifier                  |
| `--port`               | Worker HTTP port                          |
| `--models`             | Comma-separated supported models          |
| `--capacity`           | Advertised concurrent request capacity    |
| `--delay`              | Base inference delay in seconds           |
| `--jitter`             | Random variation added to inference delay |
| `--load-penalty`       | Additional delay based on concurrent load |
| `--heartbeat-interval` | Time between heartbeat messages           |
| `--gpu`                | GPU hardware label                        |
| `--standalone`         | Run without middleware registration       |

Example simulated worker:

```bash
python src/worker/worker.py --id worker-1 --port 9001 --models model-a --capacity 8 --delay 0.5 --load-penalty 0.3 --heartbeat-interval 1
```

Worker capacity is currently **advertised scheduling metadata**, not an enforced maximum number of concurrent inference requests.

## 8. Routing Algorithms

### Round Robin

Workers are selected sequentially using a per-model rotation index.

This provides simple request distribution without considering workload differences. Shared routing state is protected with a lock to support concurrent middleware requests.

### Least Loaded

Middleware selects the worker with the fewest reported active requests.

This can improve load distribution, but it does not account for differences in worker processing capacity.

### Capacity Aware

Worker utilization is calculated as:

```text
utilization = active_requests / capacity
```

Middleware selects the worker with the lowest utilization.

Example:

```text
Worker 1: 2 / 8 = 25%
Worker 2: 1 / 2 = 50%

Selected: Worker 1
```

Workers without valid advertised capacity are excluded from capacity-based comparison when valid alternatives exist.

If no worker reports valid capacity, routing falls back to round robin.

### Power of Two Choices

The middleware randomly samples two compatible workers and compares their active request counts.

The worker with the lower load is selected. Equal loads are resolved randomly.

This reduces the number of candidates examined per routing decision compared with scanning the complete worker pool.

### Adaptive Routing

Adaptive routing automatically selects a scheduling algorithm for each inference request.

The following rules are evaluated in order:

| Condition                                              | Selected policy |
| ------------------------------------------------------ | --------------- |
| Only one compatible worker                             | Round robin     |
| Missing or invalid load information                    | Round robin     |
| All workers idle                                       | Round robin     |
| Different valid worker capacities with active requests | Capacity aware  |
| Load difference of at least 2 requests                 | Least loaded    |
| At least 4 compatible workers                          | Power of two    |
| Otherwise                                              | Round robin     |

Adaptive routing logs the selected worker, routing algorithm, and reason for the decision.

The middleware can still be started with an explicit policy to support controlled comparisons.

## 9. Health Monitoring and Failure Detection

Middleware uses periodic heartbeat messages to monitor worker availability.

Default configuration:

```text
Heartbeat interval: 10 seconds
Heartbeat timeout: 30 seconds
Failure detector check interval: 1 second
```

For experiments requiring more frequent load updates:

```bash
--heartbeat-interval 1
```

Workers that fail to send heartbeats within the timeout are marked unavailable and excluded from future routing decisions.

### Monitoring Endpoints

**Middleware health**

```bash
curl http://127.0.0.1:8000/health
```

**Registered workers**

```bash
curl http://127.0.0.1:8000/workers
```

**Worker metrics**

```bash
curl http://127.0.0.1:9001/metrics
```

The registry records worker status, heartbeat timestamps, active requests, capacity, and hardware information.

Because the registry is currently stored in memory, restarting middleware clears worker registrations. Workers must be restarted to register again.

## 10. Performance Benchmarking

Routing policies were evaluated using concurrent inference requests and simulated heterogeneous workers.

Benchmark implementation:

```text
tests/benchmark_routing.py
```

### Experimental Configuration

Three simulated workers were configured with different capacities and processing delays.

| Configuration        | Worker 1 | Worker 2 | Worker 3 |
| -------------------- | -------: | -------: | -------: |
| Advertised capacity  |        8 |        4 |        2 |
| Base inference delay |    0.4 s |    0.5 s |    0.7 s |
| Load penalty         |      0.3 |      0.3 |      0.3 |
| Heartbeat interval   |    0.2 s |    0.2 s |    0.2 s |

Each routing policy was tested using:

| Parameter                  | Value |
| -------------------------- | ----- |
| Total requests per trial   | 60    |
| Concurrent client requests | 6     |
| Trials per policy          | 3     |
| Routing policies           | 5     |
| Total benchmark runs       | 15    |
| Total inference requests   | 900   |

Workers were simulated locally using Flask, with configurable inference delays and load penalties to approximate heterogeneous processing performance.

### Running Benchmarks

Start middleware using the desired routing policy, then start the three workers with the experimental configuration above.

Example:

```bash
python src/middleware/middleware.py --port 8000 --routing-policy adaptive
```

Run a benchmark:

```bash
python tests/benchmark_routing.py --policy adaptive --scenario heterogeneous --count 60 --concurrency 6 --trial 1
```

Repeat using `--trial 2` and `--trial 3`.

Run the same procedure for:

```text
round_robin
least_loaded
capacity_aware
power_of_two
adaptive
```

Each trial records:

- Successful and failed requests
- Mean end-to-end inference latency
- P95 end-to-end inference latency
- Successful-request throughput
- Request distribution across workers
- Selected routing policies

Benchmark outputs are saved to:

```text
results/routing/
```

An aggregate summary is stored in:

```text
results/routing/summary.csv
```

## 11. Benchmark Results

### 11.1 Overall Results

All five policies were evaluated across three trials with identical workload parameters.

**All 900 inference requests completed successfully, with zero failed requests.**

The following table reports averages of the three trial-level metrics.

| Routing policy | Mean latency (ms) ↓ | P95 latency (ms) ↓ | Throughput (req/s) ↑ |
| -------------- | ------------------: | -----------------: | -------------------: |
| Round robin    |              759.10 |            1143.76 |                 7.34 |
| Least loaded   |              706.25 |            1125.00 |                 7.93 |
| Capacity aware |              687.98 |             932.88 |                 8.30 |
| Power of two   |              728.69 |            1136.42 |                 7.96 |
| **Adaptive**   |          **683.12** |         **929.67** |             **8.33** |

Adaptive routing achieved the lowest observed mean latency and P95 latency and the highest mean throughput.

Capacity-aware routing performed similarly, suggesting that capacity normalization was an important factor in handling the heterogeneous workload.

### 11.2 Improvement Over Round Robin

Compared with the round-robin baseline, adaptive routing achieved:

| Metric       | Round robin |   Adaptive |      Improvement |
| ------------ | ----------: | ---------: | ---------------: |
| Mean latency |   759.10 ms |  683.12 ms | 10.01% reduction |
| P95 latency  |  1143.76 ms |  929.67 ms | 18.72% reduction |
| Throughput   |  7.34 req/s | 8.33 req/s |  13.53% increase |

These results indicate that load-aware routing can improve inference performance when workers have different processing capabilities.

### 11.3 Individual Trial Results

The following measurements were collected across the three benchmark trials.

| Policy         | Trial | Mean latency (ms) | P95 latency (ms) | Throughput (req/s) |
| -------------- | ----: | ----------------: | ---------------: | -----------------: |
| Round robin    |     1 |            756.83 |          1142.82 |               7.33 |
| Round robin    |     2 |            762.36 |          1145.84 |               7.32 |
| Round robin    |     3 |            758.10 |          1142.61 |               7.37 |
| Least loaded   |     1 |            704.76 |          1124.92 |               8.10 |
| Least loaded   |     2 |            695.61 |          1124.88 |               7.96 |
| Least loaded   |     3 |            718.38 |          1125.19 |               7.74 |
| Capacity aware |     1 |            685.45 |           928.93 |               8.33 |
| Capacity aware |     2 |            692.91 |           938.37 |               8.29 |
| Capacity aware |     3 |            685.57 |           931.35 |               8.28 |
| Power of two   |     1 |            735.39 |          1141.29 |               8.00 |
| Power of two   |     2 |            749.82 |          1125.30 |               7.60 |
| Power of two   |     3 |            700.86 |          1142.68 |               8.29 |
| Adaptive       |     1 |            679.72 |           919.20 |               8.48 |
| Adaptive       |     2 |            694.11 |           955.19 |               8.19 |
| Adaptive       |     3 |            675.53 |           914.62 |               8.33 |

Every trial completed 60/60 requests successfully.

### 11.4 Latency Consistency

The standard deviation of trial-level mean latency was calculated for each policy.

| Policy         | Mean latency (ms) | Standard deviation (ms) |
| -------------- | ----------------: | ----------------------: |
| Round robin    |            759.10 |                    2.90 |
| Least loaded   |            706.25 |                   11.46 |
| Capacity aware |            687.98 |                    4.27 |
| Power of two   |            728.69 |                   25.16 |
| Adaptive       |            683.12 |                    9.75 |

Capacity-aware routing demonstrated relatively consistent performance across the three trials.

Power of Two Choices showed the greatest variability, which is consistent with its randomized candidate selection.

Adaptive routing achieved the lowest average latency, although its performance varied more than capacity-aware routing.

### 11.5 Adaptive Routing Behavior

Across the three adaptive benchmark trials, middleware selected:

| Selected algorithm | Requests | Percentage |
| ------------------ | -------: | ---------: |
| Capacity aware     |      162 |        90% |
| Round robin        |       18 |        10% |
| Least loaded       |        0 |         0% |
| Power of two       |        0 |         0% |

Because worker capacities differed, the adaptive selector primarily chose capacity-aware routing once workers were processing requests.

When all workers were idle, adaptive routing selected round robin.

This explains why adaptive and capacity-aware routing produced similar performance.

Adaptive routing improved mean latency by approximately 0.71% over capacity-aware routing, but this small difference does not establish a statistically significant advantage.

### 11.6 Key Findings

**Finding 1 — Load awareness improves performance**

Least-loaded routing reduced mean latency compared with round robin by considering the number of active requests.

**Finding 2 — Capacity normalization benefits heterogeneous workers**

Capacity-aware routing performed better than raw least-loaded routing in this configuration. It directed more requests toward workers with greater advertised capacity and shorter simulated inference delays.

**Finding 3 — Randomized routing introduces variability**

Power of Two Choices performed better than round robin on average but had greater trial-to-trial latency variability.

**Finding 4 — Adaptive routing achieved the best observed averages**

Adaptive routing produced the lowest mean latency and P95 latency and the highest throughput across the three trials.

However, because it primarily selected capacity-aware scheduling, the measured improvement cannot be attributed solely to dynamic policy switching.

**Finding 5 — Routing policies should be evaluated under multiple workloads**

These results are specific to a heterogeneous-worker configuration with simulated inference delays and six concurrent client requests. Additional workload scenarios would be required to establish broader performance advantages.

## 12. Automated Testing

The project includes automated tests for worker metrics and routing correctness.

### Worker Metrics Tests

File:

```text
tests/test_metrics.py
```

Tests verify:

- Active request increment and decrement
- Prevention of negative request counts
- Correct cleanup when inference raises exceptions
- Concurrent metrics tracking
- Peak load measurement
- Capacity and available-slot reporting
- Capacity acquisition limits

### Routing and Middleware Tests

File:

```text
tests/test_routing.py
```

Tests verify:

- Round-robin rotation and thread safety
- Least-loaded worker selection
- Capacity-aware utilization comparison
- Missing-capacity fallback
- Power of Two candidate sampling and tie handling
- Adaptive policy decisions
- Empty and single-worker cases
- Heartbeat registry updates
- Unavailable-worker exclusion under all five routing modes
- Request validation and unavailable-worker handling

### Run All Tests

From the project root:

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

Latest verified result:

```text
Ran 23 tests in 0.127s

OK
```

| Test suite             | Passed | Failed |
| ---------------------- | -----: | -----: |
| Worker metrics         |      7 |      0 |
| Routing and middleware |     16 |      0 |
| **Total**              | **23** |  **0** |

All 23 automated tests passed.
