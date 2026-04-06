package com.aigrenntick.service.WhatsappMessage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling 
public class WhatsappMessageApplication {
	public static void main(String[] args) {
		SpringApplication.run(WhatsappMessageApplication.class, args);
	}
}
