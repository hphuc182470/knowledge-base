---
title: 00 - Kafka là gì & Lộ trình đọc
---

# Kafka là gì (1 câu)

Kafka = một cuốn **sổ cái ghi nối tiếp, không xoá khi đọc** (distributed commit log) + nền tảng event streaming. Sinh ra ở LinkedIn để thay các message broker cũ.

- Dữ liệu ghi **nối tiếp (append-only)**, **bất biến** — không sửa, không xoá khi consumer đọc.
- Message chỉ mất đi theo **thời gian** (`retention.ms`, mặc định 7 ngày) hoặc **dung lượng** (`retention.bytes`), hoặc bị "nén" theo key (Log Compaction — xem file 09).
- Hệ quả quan trọng nhất: **replay được** (đọc lại quá khứ), và **nhiều hệ thống khác nhau đọc độc lập cùng 1 luồng dữ liệu** (fan-out) mà không đụng nhau.

# Kafka khác message queue truyền thống (RabbitMQ...) thế nào

| Tiêu chí | Kafka | RabbitMQ / ActiveMQ |
|---|---|---|
| Bản chất | Log phân tán, ghi đĩa | Message broker (Exchange + Queue) |
| Giữ message | Theo thời gian/dung lượng, có thể giữ mãi (compaction) | Xoá ngay khi consumer ACK |
| Cách nhận | Consumer **chủ động kéo** (pull) | Broker **đẩy** (push) |
| Replay lại lịch sử | Có (đặt lại offset) | Không |
| Thứ tự | Đảm bảo **trong 1 partition** | Trong 1 queue, dễ vỡ khi nhiều consumer cùng đọc |
| Scale | Ngang (thêm partition) | Chủ yếu dọc |
| Hợp với | Event streaming, event sourcing, log tổng hợp, đồng bộ dữ liệu (CDC) | Hàng đợi task, routing phức tạp, request-reply |

**Chọn Kafka khi:** cần replay, nhiều hệ thống cùng đọc 1 luồng, throughput rất cao, lưu lịch sử dài, xử lý luồng (stream processing).
**Chọn RabbitMQ khi:** routing linh hoạt, TTL/dead-letter theo từng message, hàng đợi việc không cần replay, mô hình request-reply.

# Ví dụ thực tế: đặt hàng online

- **Kiểu đồng bộ (không Kafka):** Checkout gọi tuần tự Payment → Inventory → Fraud → Notification qua HTTP. Một service chậm/chết là cả đơn hàng bị ảnh hưởng.
- **Kiểu Kafka:** Checkout chỉ ghi 1 sự kiện `OrderPlaced` rồi trả về ngay (vài mili-giây). Mỗi service (Payment, Inventory...) là **một consumer group riêng**, đọc độc lập. Notification chết 2 tiếng? Khi chạy lại nó đọc tiếp từ offset cuối — không mất dữ liệu, không ảnh hưởng service khác.

# 4 trụ cột giúp Kafka nhanh & bền

| Trụ cột | Cơ chế | Lợi ích |
|---|---|---|
| Ghi tuần tự xuống đĩa | Chỉ append vào cuối file `.log` | Băng thông đĩa rất cao, không cần seek ngẫu nhiên |
| Zero-copy | Syscall `sendfile()` | Đưa byte từ cache thẳng ra mạng, không qua JVM heap |
| Chia partition | 1 topic chia nhiều partition trên nhiều broker | Scale đọc/ghi gần như tuyến tính |
| Replication (ISR) | Nhóm bản sao đang bắt kịp leader | Độ bền có thể chỉnh (`acks=all` + `min.insync.replicas`) |

**Đánh đổi lớn nhất khi tinh chỉnh Kafka:** Latency ⇄ Durability (độ bền) ⇄ Throughput. Ví dụ `acks=all` bền nhất nhưng chậm hơn; `linger.ms`/`batch.size` lớn tăng throughput nhưng thêm chút độ trễ.

# Lộ trình đọc bộ note này

```
01 → Khái niệm cốt lõi: Cluster, Broker, Topic, Partition, Offset, Producer, Consumer
02 → Replication & độ bền: ISR, Leader/Follower, High Watermark
03 → Ai điều phối cụm: ZooKeeper → KRaft
04 → Dữ liệu vào partition nào: Partitioning, key, hot key
05 → Producer: ghi vào an toàn (acks, idempotence, transaction)
06 → Consumer: đọc ra đúng cách (offset, group, rebalance)
07 → Vận hành thực tế: lag, poison message, xử lý song song, exactly-once
08 → Tính năng nâng cao: Log Compaction, Schema Registry, Kafka Streams, Connect/MirrorMaker
09 → Hiệu năng, giám sát, bảo mật
10 → Tổng kết: cheat sheet cấu hình, câu hỏi phân biệt junior/senior
```
