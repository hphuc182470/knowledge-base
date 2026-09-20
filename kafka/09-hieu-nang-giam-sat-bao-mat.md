---
title: 09 - Hiệu năng, Giám sát & Bảo mật
---

# 1. Tinh chỉnh hiệu năng (Performance Tuning)

**Tư duy:** cấu hình mặc định của Kafka ưu tiên an toàn/tương thích hơn tốc độ. Luôn **xác định nút thắt trước** (dựa trên metric) rồi mới tinh chỉnh đúng chỗ, và luôn benchmark trước/sau khi đổi cấu hình.

## Producer — đòn bẩy lớn nhất: batching + nén

| Config | Mặc định | Ưu tiên throughput | Ưu tiên độ trễ thấp |
|---|---|---|---|
| `linger.ms` | 0 | 10–50 | 1 |
| `batch.size` | 16KB | 64–256KB | 16KB |
| `compression.type` | none | `lz4` / `zstd` | `snappy` |
| `acks` | — | `1` nếu chấp nhận rủi ro mất | `all` |

**Chọn thuật toán nén:** `lz4` — nhanh, tỷ lệ nén khá, mặc định nên dùng cho microservice độ trễ thấp. `zstd` — tỷ lệ nén tốt nhất, hợp cho analytics/log. `gzip` — nén cao nhưng tốn CPU, chỉ hợp lưu trữ lạnh.

## Broker

- Ổ đĩa: dùng SSD/NVMe, không dùng ổ cứng quay (HDD) hay ổ mạng.
- **Heap JVM nhỏ (~6–8GB), dành phần RAM còn lại cho page cache** — đây là nguyên tắc quan trọng nhất, quyết định cả tốc độ lẫn việc zero-copy hoạt động đúng.
- Dùng **quota** (`producer_byte_rate`, `consumer_byte_rate`) để chặn 1 client chiếm hết băng thông ("noisy neighbor").

## Consumer

- Tăng `fetch.min.bytes` (gom nhiều dữ liệu trước khi trả về) kết hợp `fetch.max.wait.ms` để giảm số lần gọi mạng khi xử lý theo batch lớn.
- Nguyên tắc: `số luồng xử lý song song ≤ số partition` (thừa luồng sẽ ngồi không).

## Tiered Storage (Kafka 3.6+)

Đẩy các segment dữ liệu cũ ("nguội") lên object storage (S3, GCS...) để giảm nhu cầu đĩa trên broker. Lưu ý: đọc dữ liệu cũ từ object storage sẽ chậm hơn đọc từ page cache, và **Log Compaction chỉ chạy trên dữ liệu còn ở local**, không chạy trên phần đã đẩy lên storage ngoài.

# 2. Giám sát (Monitoring)

## Bốn tầng cần theo dõi

| Tầng | Theo dõi gì |
|---|---|
| Hạ tầng | CPU, RAM, I/O đĩa, băng thông mạng |
| Broker | Trạng thái replication, ai đang là leader, tải request |
| Producer/Consumer | Lag, tỷ lệ lỗi, throughput |
| Ứng dụng | Độ trễ đầu-cuối, tỷ lệ lỗi xử lý |

## Các chỉ số (metric) quan trọng nhất của broker

| Metric | Bình thường | Cảnh báo khi | Mức độ |
|---|---|---|---|
| `UnderReplicatedPartitions` | 0 | > 0 | Cảnh báo (nguy cơ mất dữ liệu) |
| `OfflinePartitionsCount` | 0 | > 0 | **Nghiêm trọng** — partition không có leader, không đọc/ghi được |
| `ActiveControllerCount` | 1 (toàn cụm) | ≠ 1 | **Nghiêm trọng** — không có controller hoặc bị split-brain |
| `RequestHandlerAvgIdlePercent` | > 0.3 | < 0.2 | Cảnh báo — broker sắp quá tải |

**Về phía consumer**, chỉ số cần theo dõi nhất là `records-lag-max` — nên cảnh báo cả **giá trị lag** lẫn **tốc độ tăng của lag**.

## Lệnh vận hành hay dùng

```bash
# Tạo topic
kafka-topics.sh --bootstrap-server $B --create --topic orders --partitions 6 --replication-factor 3

# Tăng partition (chỉ tăng, không giảm được — xem file 04)
kafka-topics.sh --bootstrap-server $B --alter --topic orders --partitions 12

# Xem lag của 1 group
kafka-consumer-groups.sh --bootstrap-server $B --describe --group my-group

# Sửa cấu hình topic
kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=86400000
```

# 3. Bảo mật (Security)

## Authentication — "Bạn là ai?"

| Cơ chế | Hợp với |
|---|---|
| `SASL/PLAIN` | Chỉ dev/test (gửi mật khẩu dạng rõ, luôn phải kèm TLS) |
| **`SASL/SCRAM-SHA-512`** | **Đa số hệ thống production** |
| **mTLS** | Container, service mesh, mô hình zero-trust |
| **OAuth 2.0 / OIDC** | Kiến trúc cloud-native, microservices |

## Authorization — ACL ("Bạn được phép làm gì?")

- **ACL** = Ai (Principal) + Tài nguyên nào (Resource) + Được làm gì (Operation) + Cho phép hay từ chối.
- **Cấu hình quan trọng nhất:** `allow.everyone.if.no.acl.found=false` — nghĩa là **mặc định từ chối** truy cập nếu resource chưa có ACL nào (an toàn hơn nhiều so với mặc định cho phép).
- Nên gán quyền theo **prefix** (ví dụ `team-a.` bao trùm mọi topic bắt đầu bằng `team-a.`) thay vì liệt kê từng topic — giảm số lượng ACL cần quản lý.

## Checklist bảo mật production (rút gọn)

- Mã hoá đường truyền: TLS.
- Xác thực: SCRAM-SHA-512 / mTLS / OAuth (tuỳ hạ tầng).
- Phân quyền: ACL theo nguyên tắc **deny-by-default**.
- Không hardcode credential — dùng công cụ quản lý secret (Vault, Secrets Manager).
- Ghi log mọi lần xác thực/phân quyền thất bại vào hệ thống giám sát an ninh (SIEM).

## Data Governance — rộng hơn bảo mật

Broker Kafka chỉ phục vụ byte thô, không tự biết "team nào sở hữu topic nào", "field nào nhạy cảm". Đây là việc cần dựng thêm bên ngoài Kafka: quy tắc schema (Schema Registry + CI), quyền sở hữu topic (catalog/quy ước đặt tên), kiểm soát truy cập (ACL/RBAC), mã hoá/che dữ liệu nhạy cảm, ghi log truy vết (audit).
