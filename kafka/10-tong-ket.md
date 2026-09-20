---
title: 10 - Tổng kết (Cheat Sheet & Câu hỏi phỏng vấn)
---

# 1. Ba bộ cấu hình "công thức" cần nhớ

```properties
# (A) Không được mất dữ liệu (tài chính, đơn hàng)
acks=all
enable.idempotence=true
retries=2147483647
linger.ms=5
compression.type=lz4
# topic/broker:
replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
# consumer:
enable.auto.commit=false
isolation.level=read_committed   # nếu topic có dùng transaction
```

```properties
# (B) Throughput cao, chấp nhận mất chút ít (analytics/telemetry)
acks=1
linger.ms=20
batch.size=65536
compression.type=lz4   # hoặc zstd
```

```properties
# (C) Exactly-once trong nội bộ Kafka (đọc → xử lý → ghi lại)
# producer: enable.idempotence=true, acks=all, transactional.id=<ổn định theo vai trò>
# consumer: isolation.level=read_committed, enable.auto.commit=false
# Kafka Streams: processing.guarantee=exactly_once_v2
# ra hệ ngoài Kafka: bắt buộc thêm idempotency key + ràng buộc UNIQUE / Outbox pattern
```

# 2. Bảng quyết định nhanh

| Câu hỏi | Trả lời |
|---|---|
| Cần thứ tự theo 1 entity? | Dùng ID entity đó làm **key** |
| Cần thứ tự toàn cục? | Dùng 1 partition (đánh đổi mất khả năng song song) |
| Consumer không theo kịp producer? | Chẩn đoán trước (poison message? DB chậm? rebalance?) rồi mới thêm consumer/xử lý theo batch |
| Muốn scale hơn số partition hiện có? | Phải tăng số partition trước |
| Cần đọc lại lịch sử? | Reset offset / tạo group mới / dùng `seek()` |
| Cần "bảng trạng thái hiện tại" theo key? | `cleanup.policy=compact` (bắt buộc phải có key) |
| Ghi ra hệ ngoài mà cần đúng 1 lần? | Idempotency key + ràng buộc UNIQUE (có thể thêm Redis) |
| Pipeline thuần Kafka → Kafka cần đúng 1 lần? | Kafka Streams `exactly_once_v2` |
| Cần đưa dữ liệu DB ↔ Kafka? | Kafka Connect (CDC dùng Debezium) |
| Cần join/aggregate/window? | Kafka Streams |
| Cần dự phòng thảm hoạ đa vùng? | MirrorMaker 2 |

# 3. Những câu hỏi hay dùng để phân biệt junior/senior

1. **Kafka có đảm bảo thứ tự toàn cục không?** Không — chỉ đảm bảo **trong 1 partition**.
2. **Vì sao không giảm được số partition, còn tăng thì lại phá thứ tự?** Vì `hash(key) % N` sẽ đổi khi N đổi, trong khi dữ liệu cũ không được di chuyển lại theo mapping mới.
3. **`acks=all` có đủ để không mất dữ liệu không?** Không — cần đi kèm `min.insync.replicas ≥ 2`, `replication.factor=3`, và tắt `unclean.leader.election`.
4. **Idempotence (producer) có chặn được mọi trường hợp trùng lặp không?** Không — chỉ chặn trùng do retry **trong 1 phiên** chạy của producer; muốn chống trùng xuyên qua việc restart cần dùng transaction.
5. **Kafka có "exactly-once" thật sự không?** Chỉ đúng **trong phạm vi nội bộ Kafka**. Ra khỏi Kafka (DB, API ngoài) cần tự làm consumer chống trùng (idempotent) hoặc dùng Outbox pattern.
6. **Vì sao consumer chỉ đọc được tới High Watermark?** Để tránh đọc phải dữ liệu có thể biến mất nếu leader chết trước khi kịp sao chép hết cho follower.
7. **Vì sao broker nên để heap JVM nhỏ?** Để dành phần lớn RAM cho page cache của hệ điều hành, giữ được hiệu năng đọc nhanh và cơ chế zero-copy.
8. **`session.timeout.ms` khác `max.poll.interval.ms` ở điểm nào?** Cái đầu đo nhịp tim (heartbeat) từ thread nền; cái sau đo khoảng cách giữa 2 lần gọi `poll()` — quá thời gian này nghĩa là code xử lý đang chạy quá chậm.
9. **Vì sao KRaft failover nhanh hơn ZooKeeper nhiều?** Vì các controller dự phòng liên tục kéo (pull) và giữ sẵn metadata trong RAM, không cần nạp lại từ đầu khi controller cũ chết.
10. **Kafka khác RabbitMQ ở điểm cốt lõi nào?** Kafka là log có thể đọc lại (replay), consumer chủ động kéo dữ liệu, throughput rất cao; RabbitMQ là broker chủ động đẩy dữ liệu, routing linh hoạt hơn, nhưng xoá message ngay khi được xác nhận.

---

*Đọc theo thứ tự 00 → 10 để nắm mạch Kafka từ khái niệm cốt lõi tới vận hành thực tế.*
