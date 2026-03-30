package com.aigrenntick.service.WhatsappMessage.dto;

import lombok.Data;

@Data
public class RecipientResult {
     private Recipient recipient;
        private boolean   success;
        private String    messageId;
        private String    messageStatus;
        private String    waId;
        private String    status;
}
