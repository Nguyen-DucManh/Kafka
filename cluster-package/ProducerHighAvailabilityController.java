package com.example.producer.controller;

import com.example.producer.model.Message;
import com.example.producer.service.HighAvailabilityProducerService;
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

    private final HighAvailabilityProducerService haProducerService;

    @PostMapping("/messages")
    public ResponseEntity<Message> sendMessage(@RequestBody Message message) {
        log.info("Received message to send with HA: {}", message);
        haProducerService.sendMessage(message);
        return ResponseEntity.ok(message);
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getClusterHealth() {
        log.info("Checking cluster health");
        Map<String, Object> health = haProducerService.getClusterHealth();
        return ResponseEntity.ok(health);
    }
    
    @PostMapping("/resend-pending")
    public ResponseEntity<String> resendPendingMessages() {
        log.info("Manually triggering resend of pending messages");
        haProducerService.resendAllPendingMessages();
        return ResponseEntity.ok("Resend of pending messages triggered");
    }
}
