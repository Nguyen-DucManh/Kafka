package com.example.consumer.controller;

import com.example.consumer.model.Message;
import com.example.consumer.service.HighAvailabilityConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/ha")
@Slf4j
@RequiredArgsConstructor
@Profile("cluster")
public class HighAvailabilityController {

    private final HighAvailabilityConsumerService haConsumerService;

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getConsumerHealth() {
        log.info("Checking consumer health");
        Map<String, Object> health = haConsumerService.getConsumerHealth();
        return ResponseEntity.ok(health);
    }
    
    @PostMapping("/reset-health")
    public ResponseEntity<String> resetHealth() {
        log.info("Manually resetting consumer health status");
        haConsumerService.resetHealth();
        return ResponseEntity.ok("Consumer health status reset to healthy");
    }
    
    @GetMapping("/messages/count")
    public ResponseEntity<Map<Message.MessageType, Long>> getMessageStats() {
        log.info("Retrieving high availability message statistics");
        Map<Message.MessageType, Long> stats = haConsumerService.getMessageCountByType();
        return ResponseEntity.ok(stats);
    }
}
