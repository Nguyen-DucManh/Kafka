package com.example.consumer.service;

import com.example.consumer.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
@Slf4j
@Profile("cluster")
public class HighAvailabilityConsumerService {

    private final List<Message> receivedMessages = new CopyOnWriteArrayList<>();
    private final Map<String, Integer> processedMessageIds = new HashMap<>();
    private final AtomicBoolean healthy = new AtomicBoolean(true);
    private long lastMessageTimestamp = System.currentTimeMillis();
    private static final int MAX_PROCESSING_ATTEMPTS = 3;

    @KafkaListener(
        topics = "${kafka.topic.name}", 
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "clusterKafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, Message> record, Acknowledgment acknowledgment) {
        Message message = record.value();
        String messageId = message.getId();
        
        try {
            log.info("Received message from cluster: {} on partition: {}", message, record.partition());
            lastMessageTimestamp = System.currentTimeMillis();
            
            // Kiểm tra xem tin nhắn đã được xử lý trước đó chưa
            if (processedMessageIds.containsKey(messageId)) {
                log.warn("Duplicate message detected: {}, already processed", messageId);
                acknowledgment.acknowledge();
                return;
            }
            
            // Xử lý tin nhắn
            processMessage(message);
            
            // Đánh dấu tin nhắn đã được xử lý
            processedMessageIds.put(messageId, 1);
            receivedMessages.add(message);
            
            // Xác nhận tin nhắn đã được xử lý
            acknowledgment.acknowledge();
            log.debug("Message acknowledged: {}", messageId);
            
            // Duy trì kích thước của map processedMessageIds
            if (processedMessageIds.size() > 10000) {
                cleanupProcessedMessages();
            }
            
        } catch (Exception e) {
            log.error("Error processing message: {}", message, e);
            
            // Kiểm tra số lần thử xử lý tin nhắn
            int attempts = processedMessageIds.getOrDefault(messageId, 0) + 1;
            processedMessageIds.put(messageId, attempts);
            
            if (attempts >= MAX_PROCESSING_ATTEMPTS) {
                log.error("Max processing attempts reached for message: {}, acknowledging to prevent redelivery", messageId);
                acknowledgment.acknowledge();
                healthy.set(false);
            } else {
                log.warn("Will retry processing message: {}, attempt: {}/{}", messageId, attempts, MAX_PROCESSING_ATTEMPTS);
                // Không xác nhận để Kafka gửi lại tin nhắn
            }
        }
    }

    private void processMessage(Message message) {
        // Mô phỏng xử lý tin nhắn
        log.info("Processing message: {}", message.getId());
        
        // Mô phỏng lỗi ngẫu nhiên để kiểm tra cơ chế retry
        if (Math.random() < 0.05) { // 5% khả năng xảy ra lỗi
            throw new RuntimeException("Simulated random processing error");
        }
    }
    
    private void cleanupProcessedMessages() {
        log.info("Cleaning up processed message IDs map, current size: {}", processedMessageIds.size());
        // Giữ lại 5000 tin nhắn gần nhất
        if (processedMessageIds.size() > 5000) {
            List<String> keysToRemove = new ArrayList<>(processedMessageIds.keySet()).subList(0, processedMessageIds.size() - 5000);
            keysToRemove.forEach(processedMessageIds::remove);
            log.info("Cleaned up processed message IDs map, new size: {}", processedMessageIds.size());
        }
    }

    public List<Message> getAllReceivedMessages() {
        return new ArrayList<>(receivedMessages);
    }
    
    public List<Message> getMessagesByType(Message.MessageType type) {
        return receivedMessages.stream()
                .filter(message -> type.equals(message.getType()))
                .collect(Collectors.toList());
    }
    
    public Message getMessageById(String id) {
        return receivedMessages.stream()
                .filter(message -> id.equals(message.getId()))
                .findFirst()
                .orElse(null);
    }
    
    public void clearMessages() {
        receivedMessages.clear();
    }
    
    public Map<Message.MessageType, Long> getMessageCountByType() {
        return receivedMessages.stream()
                .collect(Collectors.groupingBy(
                        Message::getType, 
                        Collectors.counting()));
    }
    
    public Map<String, Object> getConsumerHealth() {
        Map<String, Object> health = new HashMap<>();
        health.put("healthy", healthy.get());
        health.put("processedMessageCount", processedMessageIds.size());
        health.put("receivedMessageCount", receivedMessages.size());
        health.put("lastMessageReceivedMs", System.currentTimeMillis() - lastMessageTimestamp);
        
        // Kiểm tra xem consumer có đang nhận tin nhắn không
        boolean receiving = (System.currentTimeMillis() - lastMessageTimestamp) < 60000; // 1 phút
        health.put("receiving", receiving);
        
        return health;
    }
    
    public void resetHealth() {
        healthy.set(true);
        log.info("Consumer health status reset to healthy");
    }
}
