---
title: 06 - Consumer: Đọc dữ liệu đúng cách
---

# 1. Mô hình cốt lõi

- Consumer **chủ động kéo dữ liệu (pull)** theo nhịp riêng của mình → tự kiểm soát tốc độ đọc, tự làm chủ việc replay, broker không cần theo dõi trạng thái từng consumer.
- Consumer giữ **một offset riêng cho mỗi partition được giao**.

## Vòng lặp `poll()`

```java
while (running) {
    ConsumerRecords<K,V> records = consumer.poll(Duration.ofMillis(100));
    for (var r : records) process(r);
    consumer.commitSync();   // hoặc commitAsync
}
```

`poll()` phải được gọi đều đặn vì nó vừa lấy dữ liệu mới, vừa là "nhịp tim" báo với Kafka rằng consumer vẫn còn sống. **Nếu code xử lý bên trong vòng poll bị chặn quá lâu, consumer sẽ bị coi là chết và bị đá khỏi group** (gây rebalance).

# 2. Offset & Commit — quyết định at-least-once hay at-most-once

| Cách commit | Hành vi | Hệ quả |
|---|---|---|
| Commit **trước** khi xử lý | Nếu xử lý lỗi sau khi đã commit → message coi như đã đọc nhưng chưa thực sự xử lý xong | **At-most-once** (có thể mất dữ liệu) |
| Commit **sau** khi xử lý (khuyến nghị) | Nếu crash giữa lúc xử lý và lúc commit → xử lý lại từ offset cũ | **At-least-once** (có thể trùng — downstream cần tự chống trùng) |

- Khuyến nghị chung: `enable.auto.commit=false` + tự commit thủ công **sau khi xử lý xong**.
- **Nếu consumer crash mà chưa kịp commit:** khi khởi động lại, nó đọc từ offset đã commit **gần nhất**, nên sẽ xử lý lại phần dữ liệu giữa lần commit cuối và lúc crash (đây là lý do tại sao downstream cần chịu được xử lý trùng).

## `auto.offset.reset` (dùng khi group mới hoặc offset cũ đã bị xoá)

| Giá trị | Hành vi |
|---|---|
| `earliest` | Đọc lại từ đầu topic |
| `latest` | Bỏ qua lịch sử, chỉ đọc dữ liệu mới |
| `none` | Báo lỗi, buộc lập trình viên phải xử lý tường minh |

# 3. Consumer Group — chia việc đọc

**Ví dụ dễ hình dung — nhóm bạn chia nhau đọc thư:** thay vì 1 người đọc hết một chồng thư, cả nhóm chia nhau ra, mỗi lá thư chỉ được **đúng 1 người trong nhóm** đọc — công việc được chia đều và không ai đọc trùng của ai.

- **Một partition, tại một thời điểm, chỉ thuộc về đúng 1 consumer trong 1 group.** Đây chính là cơ chế chống xử lý trùng trong nội bộ 1 group.
- **Nhiều group khác nhau đọc cùng 1 topic hoàn toàn độc lập** (mỗi group giữ offset riêng) — đây là cách Kafka cho phép nhiều hệ thống khác nhau cùng tiêu thụ 1 luồng dữ liệu (fan-out).
- **Trần song song = số partition của topic:**

```
6 partition, 3 consumer → mỗi consumer nhận 2 partition   (cân bằng tốt)
6 partition, 6 consumer → mỗi consumer nhận 1 partition   (tối đa hoá song song)
6 partition, 8 consumer → 2 consumer NGỒI KHÔNG (không có việc)
```
Muốn scale hơn số partition hiện có → phải tăng số partition trước (xem file 04).

# 4. Rebalance — khi nào và ảnh hưởng gì

**Rebalance xảy ra khi:** có consumer vào/ra group (chủ động hoặc do timeout), số partition thay đổi, hoặc danh sách topic đăng ký thay đổi.

| Kiểu rebalance | Hành vi |
|---|---|
| **Eager (kiểu cũ)** | **Mọi** consumer trong group tạm dừng, trả lại **toàn bộ** partition đang giữ, rồi mới gán lại và chạy tiếp — gây gián đoạn cả group |
| **Cooperative (khuyến nghị)** | Chỉ những partition thực sự cần chuyển mới bị thu hồi; các consumer khác không bị ảnh hưởng, vẫn chạy tiếp |

- **Static membership** (`group.instance.id`): cho consumer một "danh tính" cố định — nếu nó restart trong vòng `session.timeout.ms`, Kafka trả lại đúng các partition cũ mà **không cần rebalance**. Rất hữu ích khi chạy trên Kubernetes (pod hay bị khởi động lại).

## Hai đồng hồ hay bị nhầm lẫn

| Tham số | Đo cái gì | Hết hạn thì sao |
|---|---|---|
| `session.timeout.ms` | Nhịp tim (heartbeat) từ thread nền | Coordinator coi consumer đã chết → rebalance |
| `max.poll.interval.ms` (mặc định 5 phút) | Khoảng cách tối đa giữa 2 lần gọi `poll()` | Bị đá khỏi group nếu xử lý bên trong quá chậm |

**Vòng lặp tệ hại (rebalance storm):** xử lý mất quá lâu → bị đá khỏi group → rebalance → nhận lại partition → lại xử lý chậm → lại bị đá... **Cách sửa:** tăng `max.poll.interval.ms` hoặc giảm `max.poll.records`, và đưa phần xử lý chậm ra khỏi luồng chạy `poll()` chính.

# 5. `__consumer_offsets` & đặt lại offset để replay

- Offset của mọi consumer group được lưu trong 1 **topic nội bộ** tên `__consumer_offsets`.
- Muốn đọc lại lịch sử (replay), có thể: đặt lại offset của group (`--reset-offsets --to-earliest`), tạo hẳn 1 consumer group mới đọc từ đầu (không ảnh hưởng group cũ), hoặc dùng `seek()` trong code.

```bash
# Nhớ dừng consumer của group trước khi reset
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group order-service --topic orders \
  --reset-offsets --to-earliest --execute
```

# 6. Ví dụ Spring Boot tối thiểu

```yaml
spring:
  kafka:
    consumer:
      group-id: order-processor-group
      auto-offset-reset: earliest
      enable-auto-commit: false
    listener:
      ack-mode: manual_immediate
```

```java
@KafkaListener(topics = "order-events", groupId = "order-processor-group")
public void handle(ConsumerRecord<String, String> rec, Acknowledgment ack) {
    process(rec.value());
    ack.acknowledge();   // commit SAU khi xử lý xong
}
```
