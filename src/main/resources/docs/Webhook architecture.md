# Webhook Callback Processing — Production Architecture (v5)

## Document Version

| Field | Value |
|-------|-------|
| Version | 5.0 |
| Based On | v4 (campaign_id removed, new table removed) |
| Key Changes | Uses existing `reports` table; lookup by `wamid` via `message_id` column; no `campaign_id` anywhere |
| Service | WhatsappMessage (`com.aigrenntick.service.WhatsappMessage`) |

---

## 1. Problem Statement

Meta's WhatsApp Business API delivers webhook callbacks (`sent`, `delivered`, `read`, `failed`) for every message dispatched through the platform. In a broadcast scenario, a single broadcast targeting 50,000 recipients generates 50,000–200,000 callback events (multiple status transitions per message). The system must ingest, deduplicate, and persist these callbacks with zero data loss, even under burst traffic, slow databases, or partial infrastructure failure.

> **Project Context:** This service uses `broadcastId + mobile` as the primary business keys. The existing `reports` table is the single source of truth — no new tables are needed. `campaign_id` is not used in this service.

---

## 2. Architecture Goals

| Goal | Target Metric |
|------|---------------|
| Zero data loss | No callback dropped under any failure scenario |
| High throughput | Sustain 10,000+ callbacks/sec at steady state |
| Low DB write latency | p99 batch write < 200ms |
| Horizontal scalability | Linear throughput gain per consumer instance added |
| Non-blocking retry | No thread blocked on delay/sleep |
| Idempotent processing | Re-processing any event produces the same DB state |
| Observability | Every bottleneck detectable via metrics before user impact |

---

## 3. High-Level Flow

```
Meta Webhook POST
       │
       ▼
┌──────────────────┐
│ WebhookController│  ── Validates X-Hub-Signature-256
│  (Spring Boot)   │  ── Extracts status events from entry[].changes[].value.statuses[]
└───────┬──────────┘  ── Returns HTTP 200 immediately to Meta
        │  KafkaTemplate.send(key = wamid)
        ▼
┌──────────────────────────┐
│  whatsapp.status.inbound  │  ── Partition key: wamid
│  (Kafka, 12+ partitions)  │  ── Ensures per-wamid ordering
└───────┬──────────────────┘
        │
        ▼
┌──────────────────────┐
│  StatusEventConsumer  │  ── ConcurrentKafkaListenerContainerFactory
│  (N instances)        │  ── Backpressure: pause/resume partitions
└───────┬──────────────┘
        │
        ▼
┌───────────────────────────┐
│  StatusBatchAccumulator    │  ── ConcurrentLinkedQueue (lock-free)
│                            │  ── Flush on count (1000) OR time (3s)
└───────┬───────────────────┘
        │
        ▼
┌──────────────────────┐
│  StatusBatchProcessor │  ── Dedup by (wamid + status) in-memory
│                       │  ── Bulk UPDATE reports WHERE message_id = wamid
│                       │  ── Route failures → retry topics
└───────┬───────────────┘
        │ (failures only)
        ▼
┌────────────────────────────────────────────────────────┐
│  Retry Chain (Kafka Topics)                             │
│  retry.1 (3s) → retry.2 (6s) → retry.3 (12s) → DLQ   │
└────────────────────────────────────────────────────────┘
```

---

## 4. Kafka Topic Design

### 4.1 Topic Inventory

| Topic | Partitions | Retention | Partition Key | Purpose |
|-------|-----------|-----------|---------------|---------|
| `whatsapp.status.inbound` | 12 | 7 days | wamid | Main ingestion |
| `whatsapp.status.retry.1` | 6 | 3 days | wamid | 1st retry (3s delay) |
| `whatsapp.status.retry.2` | 6 | 3 days | wamid | 2nd retry (6s delay) |
| `whatsapp.status.retry.3` | 3 | 3 days | wamid | 3rd retry (12s delay) |
| `whatsapp.status.dlq` | 3 | 30 days | wamid | Dead letter queue |

### 4.2 Why wamid as Partition Key

All status transitions for a single message (`sent → delivered → read`) land on the same partition, guaranteeing per-message ordering. This is critical because the DB WHERE guard relies on the stored `status_priority` not having already advanced past the incoming status.

### 4.3 Partition Scaling Rule

When consumer lag on `whatsapp.status.inbound` exceeds 100,000 for more than 5 minutes, increase partition count. Each new partition requires a corresponding consumer instance to be effective.

---

## 5. Component Deep Dive

### 5.1 WebhookController

**Responsibility:** Accept Meta webhook POST, validate `X-Hub-Signature-256`, extract status events, publish each to Kafka.

**Key behaviors:**
- Validate `X-Hub-Signature-256` against app secret. Reject invalid payloads internally but always return HTTP 200 to Meta — returning non-200 triggers Meta retry storms.
- Parse `entry[].changes[].value.statuses[]` — one webhook POST can contain multiple status events for different wamids.
- Assign `receivedAt = Instant.now()` to each event for latency tracking.
- Publish each status event as a separate Kafka message with key = wamid.
- Return HTTP 200 immediately (before Kafka ack) — Meta has a 20-second timeout; never block.

> **Kafka Down?** If Kafka is truly down, `KafkaTemplate.send().get(5, SECONDS)` throws. Log the failure — Meta retries webhooks for up to 7 days, so no permanent data loss.

---

### 5.2 StatusEvent DTO (Kafka Message)

This is the object published to `whatsapp.status.inbound`. **No `campaignId` field** — the `wamid` is the sole lookup key for the `reports` table.

```java
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class StatusEvent {
    private String  wamid;           // Meta's message ID → maps to reports.message_id
    private String  status;          // "sent" | "delivered" | "read" | "failed"
    private String  phoneNumber;     // recipient number (informational only)
    private String  errorCode;       // nullable — populated when status=failed
    private String  errorMessage;    // nullable
    private Instant receivedAt;      // set by WebhookController
    private int     retryCount;      // 0 on first attempt; incremented per retry hop
}
```

---

### 5.3 StatusEventConsumer (with Backpressure)

**Responsibility:** Consume from Kafka, apply backpressure when accumulator buffer is full, feed events to accumulator.

**Backpressure thresholds:**

| Threshold | Buffer Level | Action |
|-----------|-------------|--------|
| Pause | >= 40,000 (80% of 50K) | `consumer.pause(assignedPartitions)` — Kafka retains messages |
| Resume | <= 25,000 (50% of 50K) | `consumer.resume(pausedPartitions)` — normal consumption resumes |

The 80/50 hysteresis gap prevents rapid pause/resume oscillation under borderline load. A narrow gap (e.g. 90/85) would cause the consumer to toggle every few hundred milliseconds.

**Consumer configuration:**
```yaml
spring:
  kafka:
    consumer:
      group-id: webhook-status-processor
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500
      properties:
        max.poll.interval.ms: 300000   # 5 min — generous for backpressure pauses
        session.timeout.ms: 30000
        heartbeat.interval.ms: 10000
```

**Edge cases handled:**
- Consumer rebalance during pause: On `onPartitionsRevoked`, clear pause state. On `onPartitionsAssigned`, re-evaluate buffer and pause if needed.
- Buffer check race condition: Use `AtomicInteger` for buffer size counter.
- Poll timeout during pause: `max.poll.interval.ms` set to 5 minutes. If DB is down longer than 5 min, the consumer is evicted and rebalances — which is the correct behavior.

---

### 5.4 StatusBatchAccumulator

**Responsibility:** Buffer individual events and flush as batches to the processor.

**Flush strategy — dual trigger (whichever fires first):**
- **Count trigger:** flush when buffer reaches 1,000 events
- **Time trigger:** flush every 3 seconds regardless of count (`@Scheduled` or `ScheduledExecutorService`)

**Implementation details:**
- `ConcurrentLinkedQueue<StatusEvent>` — lock-free, high-throughput
- `AtomicInteger` tracks size (`ConcurrentLinkedQueue.size()` is O(n) — avoid it)
- On flush: drain up to 1,000 events with `poll()`, decrement counter, hand off to processor
- Use `ReentrantLock.tryLock()` on the flush method to prevent concurrent double-flush
- `@PreDestroy` hook: flush remaining events before JVM shutdown

**Edge cases handled:**
- Flush during drain: `ConcurrentLinkedQueue.poll()` is thread-safe; `tryLock()` ensures only one thread flushes at a time.
- JVM shutdown during buffered events: `@PreDestroy` flush + uncommitted Kafka offsets guarantee redelivery on restart.
- Empty flush: timer fires but buffer is empty → no-op, no DB call.

---

### 5.5 StatusBatchProcessor — DB Update Against `reports` Table

**Responsibility:** Deduplicate events in-memory, execute bulk UPDATE against the existing `reports` table, and route failures to retry topics.

**Step 1 — In-batch deduplication:**

Group events by `(wamid + status)`. If duplicates exist in the same batch, keep the one with the earliest `receivedAt`. This runs entirely in memory before any DB call.

**Step 2 — Bulk UPDATE against `reports`:**

```sql
-- The WHERE guard uses a CASE expression since reports has no generated column.
-- Status priority: queued=0, sent=1, delivered=2, read=3, failed=0 (terminal branch)

UPDATE reports
SET    message_status = :newStatus,
       status         = :newStatus,
       updated_at     = NOW()
WHERE  message_id = :wamid
  AND  CASE message_status
           WHEN 'queued'    THEN 0
           WHEN 'sent'      THEN 1
           WHEN 'delivered' THEN 2
           WHEN 'read'      THEN 3
           WHEN 'failed'    THEN 0
       END < :newStatusPriority
```

This WHERE guard is the idempotency guarantee: out-of-order callbacks (`delivered` arriving after `read`) produce 0 rows affected and are silently discarded.

**Spring JdbcTemplate batch execution:**

```java
jdbcTemplate.batchUpdate(
    "UPDATE reports " +
    "SET message_status=?, status=?, updated_at=NOW() " +
    "WHERE message_id=? " +
    "AND CASE message_status " +
    "    WHEN 'queued' THEN 0 WHEN 'sent' THEN 1 " +
    "    WHEN 'delivered' THEN 2 WHEN 'read' THEN 3 ELSE 0 END < ?",
    events, 1000,
    (ps, event) -> {
        ps.setString(1, event.getStatus());
        ps.setString(2, event.getStatus());
        ps.setString(3, event.getWamid());
        ps.setInt(4, event.getStatusPriority());
    }
);
```

**Step 3 — Handle results:**
- Rows affected = 0 → duplicate or out-of-order. Safe to discard (idempotent).
- DB exception → route the failed batch to the appropriate retry topic.

**Step 4 — Retry routing:**

| retryCount in event | Route to |
|---------------------|----------|
| 0 (first failure) | `whatsapp.status.retry.1` |
| 1 | `whatsapp.status.retry.2` |
| 2 | `whatsapp.status.retry.3` |
| >= 3 | `whatsapp.status.dlq` |

**Edge cases handled:**
- Partial batch failure: Only the failed events are routed to retry. The successful ones are not re-processed.
- `wamid` not found in `reports.message_id`: Race condition — callback arrived before `WhatsappReportUpdater` stored the wamid. Route to retry.1; the row will exist by then.
- Batch too large: A batch of 1,000 status events is well under MySQL's 64MB max packet. If batches grow in future, chunk into sub-batches of 500.

---

### 5.6 Retry Consumers (Non-Blocking Delay)

One consumer group per retry topic. Implements delay without `Thread.sleep`.

**Delay mechanism (Kafka-native timestamp approach):**
1. Consumer polls messages from retry topic.
2. For each message, check: `now - message.timestamp() >= delayMs`?
3. If yes → attempt DB write (same UPDATE logic as above).
4. If no → `consumer.pause(partition)`, schedule `consumer.resume(partition)` via `ScheduledExecutorService` after remaining delay.
5. On resume → re-poll and process.

**Edge cases handled:**
- Retry consumer crash: Uncommitted offsets → Kafka redelivers. Timestamp doesn't change, so delay calculation remains correct on redeliver.
- Retry succeeds on DB write but fails to commit offset: Redelivered and re-processed. The WHERE guard makes it idempotent.

---

### 5.7 Dead Letter Queue (DLQ) Handler

Events landing in `whatsapp.status.dlq` have failed 3 retry attempts and are **not** automatically reprocessed.

**DLQ event structure:**
- Original `StatusEvent` payload (wamid, status, etc.)
- `retryCount: 3`
- `lastFailureReason: "connection_timeout" | "row_not_found" | "deadlock" | ...`
- `firstFailedAt`, `lastFailedAt` timestamps

**Resolution strategies:**

| lastFailureReason | Resolution |
|-------------------|------------|
| `row_not_found` (wamid not in `reports.message_id`) | Race condition. Scheduled hourly DLQ replay resolves it once the row is committed. |
| `connection_timeout` | DB was down. Manual or scheduled replay once DB recovers. |
| `deadlock` after 3 retries | Investigate hot-row patterns under concurrent broadcasts. |

**DLQ monitoring:** Alert if DLQ receives more than 100 events in a 5-minute window.

---

## 6. Database Design

> **IMPORTANT: No new table is created.** The existing `reports` table is the target of all webhook callback writes. The `message_id` column stores the wamid returned by Meta after send. `campaign_id` is not referenced anywhere in this flow.

### 6.1 `reports` Table — Relevant Columns

| Column | Type | Role in Webhook Processing |
|--------|------|---------------------------|
| `id` | BIGINT PK | Row identifier |
| `broadcast_id` | BIGINT | Links row to a broadcast (set at send time, read-only for webhook processor) |
| `mobile` | VARCHAR | Recipient phone number (set at send time) |
| `message_id` | VARCHAR | Stores wamid from Meta send response — **PRIMARY lookup key for webhook callbacks** |
| `message_status` | VARCHAR | Current delivery status: `queued → sent → delivered → read` \| `failed` |
| `status` | VARCHAR | Mirrors `message_status` — updated together |
| `payload` | LONGTEXT | JSON payload sent to Meta (set at send time) |
| `response` | JSON | Meta's raw send response (set at send time) |
| `updated_at` | DATETIME | Updated on every webhook status change |

### 6.2 How `message_id` Gets Populated

The `reports` row is created at send time by the existing flow. The wamid is available only after Meta accepts the message. The Broadcast Service calls back to `/internal/broadcast/callbacks/message-results` with `providerMessageId` (the wamid) — `WhatsappReportUpdater.updateSendResult()` stores it in `message_id`. Once stored, the webhook processor can look up the row by `message_id = wamid`.

### 6.3 Required Indexes on `reports`

```sql
-- Critical: webhook processor lookup by wamid
CREATE UNIQUE INDEX idx_reports_message_id
    ON reports (message_id);

-- Optional: broadcast-level status queries (dashboard — "how many delivered in broadcast X?")
CREATE INDEX idx_reports_broadcast_status
    ON reports (broadcast_id, message_status);

-- Optional: housekeeping / archival
CREATE INDEX idx_reports_updated_at
    ON reports (updated_at);
```

> **Do NOT add indexes on** `error_code`, `mobile` alone, or `payload`. Every additional index slows the bulk UPDATE.

### 6.4 Batch Update Strategy

Use `JdbcTemplate.batchUpdate()` with batch size 1000. This issues a single network round-trip to MySQL for up to 1,000 UPDATE statements, which is far more efficient than 1,000 separate calls.

```java
// In StatusBatchProcessor
@Autowired
private JdbcTemplate jdbcTemplate;

public void processBatch(List<StatusEvent> events) {
    jdbcTemplate.batchUpdate(
        "UPDATE reports " +
        "SET message_status=?, status=?, updated_at=NOW() " +
        "WHERE message_id=? " +
        "AND CASE message_status " +
        "    WHEN 'queued' THEN 0 WHEN 'sent' THEN 1 " +
        "    WHEN 'delivered' THEN 2 WHEN 'read' THEN 3 ELSE 0 END < ?",
        events, 1000,
        (ps, event) -> {
            ps.setString(1, event.getStatus());
            ps.setString(2, event.getStatus());
            ps.setString(3, event.getWamid());
            ps.setInt(4, event.getStatusPriority());
        }
    );
}
```

---

## 7. Spring Boot Class Responsibilities

| Class | Package | Responsibility |
|-------|---------|---------------|
| `WebhookController` | `controller` | Accept Meta POST, validate signature, publish `StatusEvent` to Kafka |
| `StatusEvent` | `dto` | Kafka message DTO — `wamid`, `status`, `phoneNumber`, `errorCode`, `retryCount`. No `campaignId`. |
| `StatusEventConsumer` | `consumer` | Consume from inbound topic, backpressure via pause/resume |
| `StatusBatchAccumulator` | `accumulator` | `ConcurrentLinkedQueue` buffer, dual-trigger flush |
| `StatusBatchProcessor` | `processor` | In-batch dedup, `JdbcTemplate.batchUpdate` to `reports`, retry routing |
| `RetryConsumer` | `consumer.retry` | One bean per retry topic, non-blocking timestamp-based delay |
| `DlqConsumer` | `consumer.dlq` | Log and persist DLQ events for manual review / replay |
| `StatusPriority` | `domain` | Enum: `QUEUED=0, SENT=1, DELIVERED=2, READ=3, FAILED=0` |
| `BackpressureManager` | `consumer` | Encapsulates partition pause/resume + hysteresis logic |
| `MetricsPublisher` | `monitoring` | Micrometer gauges/counters: buffer size, consumer paused, dlq volume |

---

## 8. Use Cases

### UC-1: Normal Broadcast (50K recipients)

**Flow:** 50K messages → Meta sends callbacks over 1–24 hours.
**Expected volume:** ~150K callbacks (3 per message average).
**Peak rate:** ~5,000/sec in first 10 minutes.
**Behavior:** Buffer flushes every 3s or 1,000 events. No backpressure triggered. Retry topics stay empty.

---

### UC-2: Back-to-Back Broadcasts (2 × 50K)

**Flow:** Broadcast A starts, Broadcast B starts 2 minutes later. Both generate callbacks simultaneously.
**Expected volume:** ~300K callbacks overlapping.
**Peak rate:** ~10,000/sec.
**Behavior:** Buffer may approach 80% threshold. Consumer pauses briefly, Kafka retains messages. Resumes within seconds as batches drain. Slight latency increase (~5–10s), zero data loss.

---

### UC-3: `wamid` Not Yet in `reports` (Race Condition)

**Trigger:** Webhook callback arrives before `WhatsappReportUpdater` has stored the wamid in `reports.message_id`.

**Behavior:**
1. Batch processor UPDATE returns 0 rows affected (`message_id` not found).
2. Event routed to `retry.1`.
3. 3 seconds later, the wamid is in `reports.message_id`, UPDATE succeeds.
4. If still missing after 3 retries → DLQ → scheduled hourly replay.

---

### UC-4: Out-of-Order Callbacks

**Trigger:** Meta sends `read` before `delivered` (happens in practice).

**Behavior:**
1. `read` arrives → UPDATE sets status to `read` (priority 3).
2. `delivered` arrives later → `WHERE CASE ... END < 2` fails (3 < 2 is false).
3. `delivered` silently discarded — correct behavior.

No special handling needed. The WHERE guard makes this automatic.

---

### UC-5: Duplicate Webhooks from Meta

**Trigger:** Meta retries a webhook our 200 response was lost in transit.

**Behavior:**
1. Controller publishes to Kafka again.
2. In-batch dedup catches it if both copies land in the same batch.
3. If in different batches, the WHERE guard catches it (same wamid + same status → 0 rows affected).

**Data loss:** NONE. **Data corruption:** NONE.

---

### UC-6: DB Slow / Connection Pool Exhaustion

**Behavior:**
1. Batch writes slow down → accumulator drains slower → buffer grows.
2. Buffer hits 80% → consumer pauses.
3. Kafka retains messages (7-day retention).
4. DB recovers → buffer drains → consumer resumes.
5. Backlog processes at full speed until caught up.

**Data loss:** NONE. Kafka is the durable buffer.

---

### UC-7: Consumer Instance Crash

**Trigger:** OOM, deployment restart, pod eviction.

**Behavior:**
1. Kafka detects missing heartbeat after `session.timeout.ms` (30s).
2. Consumer group rebalances.
3. Partitions reassigned to surviving instances.
4. Uncommitted offsets → messages re-consumed from last committed position.
5. Re-consumed messages hit the WHERE guard → no duplicates.

**Data loss:** NONE.

---

### UC-8: Poison Message (Unparseable Event)

**Trigger:** Malformed event in Kafka topic (corrupt serialization, schema change).

**Behavior:**
1. Consumer deserialization fails.
2. Spring Kafka `DefaultErrorHandler` catches it.
3. Event sent directly to DLQ with `failureReason: "deserialization_error"`.
4. Consumer continues with next message — no stuck partition.

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(kafkaTemplate);
    return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0));
}
```

---

## 9. Scaling Roadmap

### Tier 1: Current (up to 10K callbacks/sec)

- 12 Kafka partitions, 3 consumer instances
- Single MySQL instance, connection pool 20
- Buffer: 50K per instance
- Batch size: 1,000

### Tier 2: Medium (10K–50K callbacks/sec)

- 24 Kafka partitions, 8–12 consumer instances
- MySQL connection pool 50, dedicated write instance (no reporting queries)
- Buffer: 50K per instance
- Batch size: 2,000

### Tier 3: High (50K–200K callbacks/sec)

- 48 Kafka partitions, 24 consumer instances
- MySQL cluster with read replicas for dashboards
- Connection pool 100 on write instance
- Batch size: 5,000 with chunked sub-batches of 1,000

### Tier 4: Extreme (200K+ callbacks/sec)

- Shard `reports` by `broadcast_id` hash (4–8 shards)
- Each shard gets its own consumer group processing a subset of partitions
- Kafka cluster with dedicated brokers for status topics
- Consider switching bulk UPDATE to `INSERT ... ON DUPLICATE KEY UPDATE` for better InnoDB page locality

---

## 10. Failure Matrix

| Failure | Data Loss? | Auto-Recovery? | Human Action? |
|---------|-----------|----------------|---------------|
| Kafka broker down (1 of 3) | NO | YES (replication) | Monitor |
| Kafka broker down (all) | POSSIBLE* | YES (Meta retries) | Restore Kafka |
| MySQL slow | NO | YES (backpressure) | Check slow queries |
| MySQL down | NO | YES (backpressure + Kafka retention) | Restore MySQL |
| Consumer OOM | NO | YES (rebalance + replay) | Check for memory leak |
| Poison message | NO | YES (DLQ routing) | Fix schema |
| Race: wamid not in `reports` yet | NO | YES (retry chain) | None |
| Out-of-order callback | NO | YES (WHERE guard) | None |
| Network partition (consumer ↔ Kafka) | NO | YES (reconnect) | Monitor |
| DLQ overflow | NO** | NO | Manual replay after root cause fix |

\* Meta retries webhooks for 7 days, so even full Kafka loss is recoverable if Kafka is restored within that window.  
\** Events are in DLQ, not lost. They are not in DB until manually replayed.

---

## 11. Monitoring & Alerting

### 11.1 Micrometer Metrics to Expose

| Metric | Type | Description |
|--------|------|-------------|
| `webhook.events.received` | Counter | Total events received at controller |
| `webhook.events.published` | Counter | Events successfully published to Kafka |
| `buffer.size` | Gauge | Current accumulator buffer size |
| `buffer.utilization.percent` | Gauge | buffer.size / 50000 × 100 |
| `consumer.paused` | Gauge (0/1) | Whether consumer is currently paused |
| `batch.flush.count` | Counter | Number of batch flushes |
| `batch.flush.size` | Histogram | Events per flush |
| `batch.db.write.duration` | Timer | Time for `batchUpdate()` call |
| `retry.topic.1.volume` | Counter | Events sent to retry.1 |
| `retry.topic.2.volume` | Counter | Events sent to retry.2 |
| `retry.topic.3.volume` | Counter | Events sent to retry.3 |
| `dlq.volume` | Counter | Events sent to DLQ |
| `consumer.lag` | Gauge | Kafka consumer group lag per partition |

### 11.2 Alert Rules

| Condition | Severity | Action |
|-----------|----------|--------|
| `buffer.utilization > 80%` for 2 min | WARNING | Check DB health, consider adding consumers |
| `consumer.paused = 1` for 5 min | CRITICAL | DB likely down, investigate immediately |
| `dlq.volume` increases by 100+ in 5 min | WARNING | Check for schema changes or DB issues |
| `consumer.lag > 100,000` for 5 min | WARNING | Scale consumers or partitions |
| `batch.db.write.duration p99 > 2s` | WARNING | DB performance degrading |
| `retry.topic.1.volume > 1000/min` | WARNING | Elevated race conditions or DB pressure |

---

## 12. Configuration Reference (`application.yaml` additions)

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5

    consumer:
      group-id: webhook-status-processor
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500
      properties:
        spring.json.trusted.packages: "com.aigrenntick.service.WhatsappMessage.dto"
        max.poll.interval.ms: 300000
        session.timeout.ms: 30000
        heartbeat.interval.ms: 10000

webhook:
  accumulator:
    buffer-capacity: 50000
    pause-threshold-percent: 80
    resume-threshold-percent: 50
    flush-count: 1000
    flush-interval-ms: 3000
  retry:
    delays:
      retry-1-ms: 3000
      retry-2-ms: 6000
      retry-3-ms: 12000
    max-retries: 3
```

---

## 13. Out of Scope

1. **Real-time status push to frontend** — separate WebSocket/SSE layer reading from DB or a status-change event topic.
2. **Analytics aggregation** — broadcast-level stats (% delivered, % read) should be computed by a read-optimized service or materialized view, not by this callback processor.
3. **Multi-region deployment** — this design assumes single-region Kafka + MySQL. Cross-region requires Kafka MirrorMaker and MySQL replication.
4. **Template/message content storage** — the callback processor only handles status transitions, not message content.

---

## 14. Final Architecture Properties

| Property | Value | How |
|----------|-------|-----|
| Data Loss | ZERO | Kafka retention + backpressure + Meta retries |
| Throughput | 10K–200K+/sec | Horizontal scaling via partitions + consumers |
| Retry | Async, non-blocking | Kafka retry topics with timestamp-based delay |
| Backpressure | Hysteresis-based | Pause at 80%, resume at 50% |
| Idempotency | Full | WHERE priority guard + in-batch dedup |
| Ordering | Per-message | wamid partition key |
| Scalability | Linear horizontal | Add partitions + consumer instances |
| DB target | Existing `reports` table | `UPDATE WHERE message_id = wamid` — no new tables |
| `campaignId` usage | NONE | Not used anywhere in webhook callback flow |
| Observability | Comprehensive | Micrometer metrics + structured alert rules |