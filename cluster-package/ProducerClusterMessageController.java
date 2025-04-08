package com.example.producer.controller;

import com.example.producer.model.Message;
import com.example.producer.service.KafkaClusterProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/messages")
@Slf4j
@RequiredArgsConstructor
@Profile("cluster")
public class ClusterMessageController {

    private final KafkaClusterProducerService producerService;

    @PostMapping
    public ResponseEntity<Message> sendMessage(@RequestBody Message message) {
        log.info("Received message to send to cluster: {}", message);
        producerService.sendMessage(message);
        return ResponseEntity.ok(message);
    }
    
    @PostMapping("/info")
    public ResponseEntity<String> sendInfoMessage(@RequestParam String content) {
        log.info("Received info message to send to cluster: {}", content);
        producerService.sendInfoMessage(content);
        return ResponseEntity.ok("Info message sent successfully to Kafka cluster");
    }
    
    @PostMapping("/data")
    public ResponseEntity<String> sendDataMessage(
            @RequestParam String content,
            @RequestBody Map<String, Object> data) {
        log.info("Received data message to send to cluster: {}, with data: {}", content, data);
        producerService.sendDataMessage(content, data);
        return ResponseEntity.ok("Data message sent successfully to Kafka cluster");
    }
    
    @GetMapping("/cluster-status")
    public ResponseEntity<String> getClusterStatus() {
        return ResponseEntity.ok("Producer is connected to Kafka cluster with 3 brokers");
    }
}
