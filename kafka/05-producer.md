---
title: 05 - Producer: Ghi dữ liệu an toàn
---

# 1. Việc của Producer

1. Serialize key/value → 2. Chọn partition (xem file 04) → 3. Gom batch → 4. Xử lý retry khi lỗi → 5. Quản lý mức cam kết giao hàng qua `acks`.

## Điều gì xảy ra khi gọi `send()`

```
send(record)
  → Serializer (chuyển key/value thành byte)
  → Partitioner (chọn partition)
  → Buffer trong RAM, gom theo từng (topic, partition)
  → Thread nền rút batch ra khi đầy batch.size HOẶC hết linger.ms
  → Gửi tới broker đang là leader
  → Broker ghi (và sao chép tới ISR nếu acks=all)
  → Trả kết quả về qua callback / Future
```

`send()` luôn **bất đồng bộ**: chỉ đưa message vào buffer trong RAM rồi trả về ngay, việc gửi thật diễn ra ở thread nền. Đây là lý do Kafka có thể gom nhiều message thành 1 batch để tăng throughput thay vì tốn 1 lần gọi mạng cho mỗi message.

# 2. Ba kiểu gửi

| Kiểu | Đặc điểm |
|---|---|
| Fire-and-forget | Nhanh nhưng có thể mất message khi lỗi mà không hay biết |
| Sync (chờ kết quả ngay) | Chặn luồng gửi tiếp theo → throughput rất thấp. **Nên tránh** |
| **Async + callback** (khuyến nghị) | Không chặn, nhưng **luôn phải xử lý lỗi trong callback**, nếu không sẽ mất dữ liệu mà không biết |

# 3. Cấu hình quan trọng

```properties
# Độ tin cậy
acks=all
retries=2147483647
delivery.timeout.ms=120000          # tổng thời gian cho phép cho 1 lần gửi (kể cả các lần retry)

# Throughput / batching
linger.ms=5           # đợi tối đa 5ms để gom batch (mặc định 0 = gửi ngay)
batch.size=32768      # kích thước batch tối đa cho mỗi partition (mặc định 16KB)
compression.type=lz4  # none | gzip | snappy | lz4 | zstd
```

- **Lỗi có thể retry**: lỗi mạng, chưa có leader, timeout, không đủ replica.
- **Lỗi không thể retry**: message quá lớn, lỗi serialize.

# 4. `acks` — 3 mức cam kết khi ghi

| `acks` | Cơ chế | Rủi ro mất dữ liệu | Dùng cho |
|---|---|---|---|
| `0` | Không đợi phản hồi từ broker | **Rất cao**, mất mà không biết | Metrics, clickstream — chấp nhận mất |
| `1` | Chỉ đợi **leader** ghi xong | Trung bình: leader báo "thành công" rồi chết trước khi follower kịp sao chép → mất | Log ứng dụng, sự kiện không quá quan trọng |
| `all` (`-1`) | Đợi **toàn bộ ISR** xác nhận | Không mất (nếu đi kèm `min.insync.replicas ≥ 2`) | Tài chính, đơn hàng, đồng bộ dữ liệu |

> Từ Kafka 3.0, mặc định của producer là `acks=all` kèm `enable.idempotence=true`.

# 5. Idempotent Producer — chống trùng do retry

**Vấn đề:** producer gửi batch → broker ghi thành công → nhưng phản hồi (ACK) bị thất lạc trên đường về → producer tưởng lỗi nên gửi lại → broker ghi lần 2 → **trùng dữ liệu**.

**Cách Kafka giải quyết** (`enable.idempotence=true`, mặc định từ Kafka 3.0):
- Producer được cấp 1 **Producer ID (PID)** khi khởi tạo.
- Mỗi batch gắn kèm số thứ tự tăng dần theo `(PID, partition)`.
- Broker nhớ vài số thứ tự gần nhất; nếu thấy lại đúng số cũ thì **bỏ qua bản trùng nhưng vẫn báo thành công** cho producer.

**Giới hạn cần nhớ:**
1. Chỉ chống trùng **trong 1 phiên chạy của producer**. Producer khởi động lại → được cấp PID mới → mất khả năng dedup của phiên trước. Muốn chống trùng xuyên qua việc restart → cần dùng **transaction** (mục 6).
2. Chỉ chống trùng do **Kafka tự động retry**, không chống được việc **ứng dụng chủ động gửi 2 lần**.

# 6. Transactions — ghi nguyên tử nhiều partition

**Vì sao cần thêm transaction** (idempotence không đủ) khi:
1. Cần ghi **nhiều partition/topic cùng lúc một cách nguyên tử** (hoặc tất cả thành công, hoặc không gì cả).
2. Pipeline kiểu **đọc → xử lý → ghi lại** (consume-process-produce) cần đúng 1 lần.
3. Cần chống trùng **xuyên qua cả việc producer bị restart**.

**Ý tưởng:** producer khai báo `transactional.id` cố định. Một broker được chọn làm **Transaction Coordinator** giữ trạng thái transaction (Empty/Ongoing/PrepareCommit/CompleteCommit...). Khi `commitTransaction()`, Kafka ghi các "marker" COMMIT vào mọi partition liên quan — consumer chỉ thấy dữ liệu khi transaction đã commit thành công.

- **Consumer phải chủ động bật:** `isolation.level=read_committed` (mặc định là `read_uncommitted` — vẫn đọc cả dữ liệu của transaction bị huỷ).
- **Producer Epoch:** mỗi lần producer khởi tạo lại transaction, epoch tăng lên → nếu có 1 bản instance cũ ("zombie", ví dụ pod cũ chưa kịp tắt hẳn) vẫn cố ghi với epoch cũ, nó sẽ bị chặn lại — tránh 2 instance cùng vai trò ghi đè lên nhau.
- **Lưu ý khi chạy trên Kubernetes:** `transactional.id` phải **ổn định theo vai trò logic** (ví dụ theo tên service), **không** lấy theo tên pod — vì pod mới sẽ có ID khác, không chặn được pod cũ.

# 7. Bẫy thường gặp của Producer

| Bẫy | Triệu chứng | Cách sửa |
|---|---|---|
| Gọi chờ kết quả (`.get()`) sau mỗi lần gửi | Throughput cực thấp | Dùng callback bất đồng bộ |
| Buffer bộ nhớ quá nhỏ | Lỗi tràn buffer | Tăng `buffer.memory` |
| Không xử lý exception trong callback | Mất dữ liệu mà không biết | Luôn xử lý lỗi trong callback |
| Không bật nén | Tốn băng thông mạng | Bật `lz4` hoặc `zstd` |
