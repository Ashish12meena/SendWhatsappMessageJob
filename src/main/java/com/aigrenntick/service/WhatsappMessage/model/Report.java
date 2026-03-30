package com.aigrenntick.service.WhatsappMessage.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import com.aigrenntick.service.WhatsappMessage.enums.Platform;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "reports")
public class Report {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "broadcast_id")
    private Long broadcastId;

    @Column(name = "campaign_id")
    private Long campaignId;

    @Column(name = "group_send_id")
    private Long groupSendId;

    @Column(name = "tag_log_id")
    private Long tagLogId;

    private String mobile;
    private String type;

    @Column(name = "message_id")
    private String messageId;

    @Column(name = "wa_id")
    private String waId;

    @Column(name = "message_status")
    private String messageStatus;

    private String status;

    @Column(columnDefinition = "longtext")
    private String payload;

    @Column(name = "payment_status")
    private Integer paymentStatus;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "json")
    private String response;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "json")
    private String contact;

    @Enumerated(EnumType.STRING)
    private Platform platform;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;
}