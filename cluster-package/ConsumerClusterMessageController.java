package com.example.consumer.controller;

import com.example.consumer.model.Message;
import com.example.consumer.service.KafkaClusterConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/messages")
@Slf4j
@RequiredArgsConstructor
@Profile("cluster")
public class ClusterMessageController {

    private final KafkaClusterConsumerService consumerService;

    @GetMapping
    public ResponseEntity<List<Message>> getAllMessages() {
        log.info("Retrieving all received messages from cluster");
        List<Message> messages = consumerService.getAllReceivedMessages();
        return ResponseEntity.ok(messages);
    }
    
    @GetMapping("/{id}")
    public ResponseEntity<Message> getMessageById(@PathVariable String id) {
        log.info("Retrieving message with id: {} from cluster", id);
        Message message = consumerService.getMessageById(id);
        if (message == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(message);
    }
    
    @GetMapping("/type/{type}")
    public ResponseEntity<List<Message>> getMessagesByType(@PathVariable Message.MessageType type) {
        log.info("Retrieving messages with type: {} from cluster", type);
        List<Message> messages = consumerService.getMessagesByType(type);
        return ResponseEntity.ok(messages);
    }
    
    @GetMapping("/stats")
    public ResponseEntity<Map<Message.MessageType, Long>> getMessageStats() {
        log.info("Retrieving message statistics from cluster");
        Map<Message.MessageType, Long> stats = consumerService.getMessageCountByType();
        return ResponseEntity.ok(stats);
    }
    
    @DeleteMapping
    public ResponseEntity<Void> clearMessages() {
        log.info("Clearing all messages from cluster");
        consumerService.clearMessages();
        return ResponseEntity.ok().build();
    }
    
    @GetMapping("/cluster-status")
    public ResponseEntity<String> getClusterStatus() {
        return ResponseEntity.ok("Consumer is connected to Kafka cluster with 3 brokers");
    }
}
