package com.example.producer.service;

import com.example.producer.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
@RequiredArgsConstructor
@Profile("cluster")
public class HighAvailabilityProducerService {

    private final KafkaTemplate<String, Message> clusterKafkaTemplate;
    
    // Lưu trữ tin nhắn đã gửi để có thể gửi lại nếu cần
    private final Map<String, Message> pendingMessages = new ConcurrentHashMap<>();
    
    // Theo dõi số lần thử lại cho mỗi tin nhắn
    private final Map<String, AtomicInteger> retryCount = new ConcurrentHashMap<>();
    
    // Số lần thử lại tối đa
    private static final int MAX_RETRIES = 5;

    @Value("${kafka.topic.name}")
    private String topicName;

    public void sendMessage(Message message) {
        if (message.getId() == null || message.getId().isEmpty()) {
            message.setId(UUID.randomUUID().toString());
        }
        
        if (message.getTimestamp() == 0) {
            message.setTimestamp(System.currentTimeMillis());
        }
        
        if (message.getType() == null) {
            message.setType(Message.MessageType.INFO);
        }
        
        if (message.getAdditionalData() == null) {
            message.setAdditionalData(new HashMap<>());
        }
        
        // Lưu tin nhắn vào bộ nhớ tạm thời để có thể gửi lại nếu cần
        pendingMessages.put(message.getId(), message);
        retryCount.put(message.getId(), new AtomicInteger(0));
        
        sendWithRetry(message);
    }
    
    private void sendWithRetry(Message message) {
        // Tạo record với headers để theo dõi
        ProducerRecord<String, Message> record = new ProducerRecord<>(
            topicName, 
            null, // Để Kafka tự chọn partition
            message.getId(), // key
            message, // value
            new ArrayList<>() {{
                add(new RecordHeader("retry-count", 
                    String.valueOf(retryCount.get(message.getId()).get()).getBytes(StandardCharsets.UTF_8)));
                add(new RecordHeader("timestamp", 
                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));
            }}
        );

        ListenableFuture<SendResult<String, Message>> future = 
            clusterKafkaTemplate.send(record);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Message>>() {
            @Override
            public void onSuccess(SendResult<String, Message> result) {
                log.info("Message sent successfully to cluster: [{}] with offset: [{}], partition: [{}]", 
                    message, 
                    result.getRecordMetadata().offset(),
                    result.getRecordMetadata().partition());
                
                // Xóa tin nhắn khỏi bộ nhớ tạm thời khi gửi thành công
                pendingMessages.remove(message.getId());
                retryCount.remove(message.getId());
            }

            @Override
            public void onFailure(Throwable ex) {
                int currentRetry = retryCount.get(message.getId()).incrementAndGet();
                log.error("Failed to send message to cluster: [{}], retry attempt: {}", message, currentRetry, ex);
                
                if (currentRetry <= MAX_RETRIES) {
                    // Thử lại sau một khoảng thời gian
                    log.info("Retrying message: [{}], attempt: {}/{}", message.getId(), currentRetry, MAX_RETRIES);
                    try {
                        // Tăng thời gian chờ theo cấp số nhân để tránh quá tải hệ thống
                        Thread.sleep(1000 * (long)Math.pow(2, currentRetry - 1));
                        sendWithRetry(message);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Retry interrupted for message: [{}]", message.getId(), e);
                    }
                } else {
                    log.error("Max retries reached for message: [{}], giving up", message.getId());
                    // Có thể lưu vào dead letter queue hoặc cơ sở dữ liệu để xử lý thủ công sau
                    handleMaxRetriesReached(message);
                }
            }
        });
    }
    
    private void handleMaxRetriesReached(Message message) {
        log.error("Message could not be delivered after {} attempts: {}", MAX_RETRIES, message);
        // Trong môi trường thực tế, bạn có thể:
        // 1. Lưu vào cơ sở dữ liệu
        // 2. Gửi thông báo cho admin
        // 3. Gửi vào dead letter queue
        
        // Xóa khỏi bộ nhớ tạm thời để tránh rò rỉ bộ nhớ
        pendingMessages.remove(message.getId());
        retryCount.remove(message.getId());
    }
    
    public void sendInfoMessage(String content) {
        Message message = Message.builder()
                .id(UUID.randomUUID().toString())
                .content(content)
                .timestamp(System.currentTimeMillis())
                .type(Message.MessageType.INFO)
                .build();
        
        sendMessage(message);
    }
    
    public void sendDataMessage(String content, Map<String, Object> data) {
        Message message = Message.builder()
                .id(UUID.randomUUID().toString())
                .content(content)
                .timestamp(System.currentTimeMillis())
                .type(Message.MessageType.DATA)
                .additionalData(data)
                .build();
        
        sendMessage(message);
    }
    
    // Phương thức để kiểm tra trạng thái của các broker
    public Map<String, Object> getClusterHealth() {
        Map<String, Object> health = new HashMap<>();
        health.put("pendingMessages", pendingMessages.size());
        health.put("status", pendingMessages.isEmpty() ? "HEALTHY" : "DEGRADED");
        return health;
    }
    
    // Phương thức để gửi lại tất cả các tin nhắn đang chờ xử lý
    public void resendAllPendingMessages() {
        log.info("Resending {} pending messages", pendingMessages.size());
        for (Message message : pendingMessages.values()) {
            // Reset retry count
            retryCount.put(message.getId(), new AtomicInteger(0));
            sendWithRetry(message);
        }
    }
}
