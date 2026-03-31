package com.aigrenntick.service.WhatsappMessage.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.aigrenntick.service.WhatsappMessage.model.Report;

@Repository
public interface ReportRepository extends JpaRepository<Report, Long> {

    Optional<Report> findByBroadcastIdAndMobile(Long broadcastId, String mobile);

    @Modifying
    @Query("""
            UPDATE Report r
            SET r.messageId     = :messageId,
                r.messageStatus = :messageStatus,
                r.waId          = :waId,
                r.status        = :status,
                r.payload       = :payload
            WHERE r.broadcastId = :broadcastId
            AND r.mobile        = :mobile
            """)
    int updateSendResult(@Param("broadcastId") Long broadcastId,
                         @Param("mobile") String mobile,
                         @Param("messageId") String messageId,
                         @Param("messageStatus") String messageStatus,
                         @Param("waId") String waId,
                         @Param("status") String status,
                         @Param("payload") String payload);
}
