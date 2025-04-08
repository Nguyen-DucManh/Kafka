# Trao đổi dữ liệu qua Kafka với Spring Boot và Kafka Cluster

## Tạo 2 ứng dụng Spring boot trao đổi dữ liệu (JSON) với nhau thông qua Kafka:
![Screenshot 2025-04-08 215512](https://github.com/user-attachments/assets/5b2d74f6-d413-4111-b306-2343278775c9)
![Screenshot 2025-04-08 215532](https://github.com/user-attachments/assets/413cc187-b8fc-476c-b17c-8fac6e3e04f0)


## Nâng cấp Kafka thành cluster gồm 3 máy:

### Zookeeper
![Screenshot 2025-04-08 222513](https://github.com/user-attachments/assets/77a14232-cbbb-474e-854e-15c183bfcc08)
![Screenshot 2025-04-08 222526](https://github.com/user-attachments/assets/69aaf9c7-a180-4562-8462-6f7498ea7566)
![Screenshot 2025-04-08 222535](https://github.com/user-attachments/assets/c2592df3-bb55-460b-97f3-026f649d01f9)


### Kafka
![Screenshot 2025-04-08 222542](https://github.com/user-attachments/assets/68a2f760-2ce4-4e31-82f0-819d9002ccfb)
![Screenshot 2025-04-08 222549](https://github.com/user-attachments/assets/5add00dc-d087-4d8b-9f11-b9b396a972c8)

![Screenshot 2025-04-08 222555](https://github.com/user-attachments/assets/531631e8-853d-48e5-8cec-1ee52674e203)


## Cấu hình Kafka Cluster

Kafka Cluster được cấu hình với 3 broker:
- Broker 1: localhost:9092
- Broker 2: localhost:9093
- Broker 3: localhost:9094

Topic "example-topic" được tạo với 3 partition và replication factor 3, đảm bảo mỗi partition có bản sao trên tất cả các broker.

### Các cấu hình quan trọng:
- `min.insync.replicas=2`: Yêu cầu ít nhất 2 replica phải xác nhận việc ghi
- `acks=all`: Producer yêu cầu xác nhận từ tất cả các replica trong ISR
- `unclean.leader.election.enable=false`: Không cho phép bầu leader từ replica không trong ISR

## Kiểm tra khả năng chịu lỗi của Kafka Cluster:

1. **Xác định broker leader**: Sử dụng lệnh kafka-topics.bat --describe để xác định broker nào là leader cho mỗi partition của topic "example-topic".
2. **Gửi tin nhắn ban đầu**: Gửi một số tin nhắn thử nghiệm thông qua API của producer để xác nhận hệ thống đang hoạt động bình thường.
3. **Dừng broker leader**: Dừng broker được xác định là leader cho một hoặc nhiều partition.
4. **Theo dõi quá trình bầu leader mới**: Sử dụng lệnh kafka-topics.bat --describe để xác định broker nào được bầu làm leader mới.
5. **Kiểm tra hoạt động của hệ thống**: Gửi và nhận tin nhắn sau khi broker leader bị dừng để xác nhận hệ thống vẫn hoạt động bình thường.
6. **Khởi động lại broker đã dừng**: Khởi động lại broker đã dừng và theo dõi quá trình tham gia lại cluster.

### Kết quả kiểm tra

Kết quả của lệnh `kafka-topics.bat --describe --topic example-topic --bootstrap-server localhost:9092`:

```
Topic: example-topic    PartitionCount: 3       ReplicationFactor: 3    Configs: segment.bytes=1073741824
        Topic: example-topic    Partition: 0    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
        Topic: example-topic    Partition: 1    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
        Topic: example-topic    Partition: 2    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
```

Từ kết quả trên, ta xác định:
- Broker 1 là leader cho Partition 0
- Broker 2 là leader cho Partition 1
- Broker 3 là leader cho Partition 2

### Dừng Broker 1 (leader cho Partition 0)

Sau khi dừng Broker 1, kiểm tra lại thông tin leader:

```
Topic: example-topic    PartitionCount: 3       ReplicationFactor: 3    Configs: segment.bytes=1073741824
        Topic: example-topic    Partition: 0    Leader: 2       Replicas: 1,2,3 Isr: 2,3
        Topic: example-topic    Partition: 1    Leader: 2       Replicas: 2,3,1 Isr: 2,3
        Topic: example-topic    Partition: 2    Leader: 3       Replicas: 3,1,2 Isr: 3,2
```

Từ kết quả trên:
- Broker 2 đã được bầu làm leader mới cho Partition 0
- Broker 1 đã bị loại khỏi danh sách ISR của tất cả các partition
- Các partition khác vẫn giữ nguyên leader
- Thời gian từ khi dừng Broker 1 đến khi leader mới được bầu là khoảng 15 giây.

### Kiểm tra hoạt động sau khi leader thay đổi

Sau khi broker leader bị dừng, gửi một số tin nhắn thử nghiệm:

```
curl -X POST http://localhost:8083/api/messages -H "Content-Type: application/json" -d "{\"content\":\"Test message after leader failure\",\"type\":\"WARNING\"}"
```

Kết quả: Tin nhắn đã được gửi thành công và nhận bởi consumer.

Kiểm tra trạng thái health của producer và consumer:
```
curl http://localhost:8083/api/ha/health
curl http://localhost:8084/api/ha/health
```

Kết quả: Cả producer và consumer đều báo cáo trạng thái "HEALTHY".
