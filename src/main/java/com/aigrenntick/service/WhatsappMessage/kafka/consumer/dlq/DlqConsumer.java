package com.aigrenntick.service.WhatsappMessage.kafka.consumer.dlq;

import com.aigrenntick.service.WhatsappMessage.dto.StatusEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes from whatsapp.status.dlq.
 *
 * Events land here after exhausting all 3 retry attempts.
 * NOT auto-reprocessed — requires manual intervention or a scheduled replay job.
 *
 * What this does:
 *  1. Deserializes the event
 *  2. Logs full details at ERROR level (triggers alerting)
 *  3. Acks the offset — message stays in DLQ topic (Kafka retains for 30 days)
 *     and is available for manual replay via kafka-console-consumer or a replay job
 *
 * Common lastFailureReason values and what they mean:
 *  "row_not_found"       — wamid never made it into reports.message_id
 *                          (send failed before WhatsappReportUpdater ran)
 *  "connection_timeout"  — DB was down for > 3 retry cycles (~21s total)
 *  "deadlock"            — hot-row contention under concurrent broadcasts
 *
 * Resolution:
 *  - Fix the root cause
 *  - Replay DLQ events by republishing them to whatsapp.status.inbound
 *    (a manual replay job or admin endpoint can do this)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DlqConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics   = "${kafka.topics.status-dlq:whatsapp.status.dlq}",
            groupId  = "webhook-dlq-monitor",
            containerFactory = "webhookKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            StatusEvent event = objectMapper.readValue(record.value(), StatusEvent.class);

            // ERROR level — should trigger an alert (architecture doc §11.2)
            // Alert rule: dlq.volume increases by 100+ in 5 min → WARNING
            log.error(
                    "DLQ EVENT — wamid={} status={} retryCount={} " +
                    "lastFailureReason={} firstFailedAt={} lastFailedAt={} phoneNumber={}",
                    event.getWamid(),
                    event.getStatus(),
                    event.getRetryCount(),
                    event.getLastFailureReason(),
                    event.getFirstFailedAt(),
                    event.getLastFailedAt(),
                    event.getPhoneNumber()
            );

        } catch (Exception e) {
            // Unparseable even as StatusEvent — log raw value
            log.error(
                    "DLQ EVENT (unparseable) — partition={} offset={} rawValue={} error={}",
                    record.partition(), record.offset(), record.value(), e.getMessage()
            );
        } finally {
            // Always ack — message remains in topic for 30 days for manual replay
            ack.acknowledge();
        }
    }
}