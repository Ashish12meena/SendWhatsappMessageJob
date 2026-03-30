package com.aigrenntick.service.WhatsappMessage.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import com.aigrenntick.service.WhatsappMessage.enums.Platform;

import jakarta.persistence.Column;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Report {

    private Long id;
    private Long userId;
    private Long broadcastId;
    private Long campaignId;
    private Long groupSendId;
    private Long tagLogId;

    private String mobile;
    private String type;
    private String messageId;
    private String waId;
    private String messageStatus;
    private String status;

    private String payload; // longtext
    private Integer paymentStatus;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "json")
    private String response; // json
    
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "json")
    private String contact; // json

    @Enumerated(EnumType.STRING)
    private Platform platform; // enum stored as String

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime deletedAt;
}