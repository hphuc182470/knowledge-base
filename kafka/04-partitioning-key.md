---
title: 04 - Dữ liệu vào Partition nào? (Partitioning & Key)
---

# 1. Bốn chiến lược chọn partition

| Chiến lược | Khi nào dùng | Cách hoạt động |
|---|---|---|
| **Key-based** (mặc định khi có key) | Cần giữ thứ tự theo từng entity | `hash(key) % số_partition` |
| **Round-robin** (Kafka cũ, không key) | Ít dùng | Xoay vòng từng message |
| **Sticky** (mặc định khi không key, Kafka ≥ 2.4) | Ưu tiên throughput, không cần thứ tự | Dồn message vào 1 partition tới khi đầy batch rồi mới đổi |
| **Custom Partitioner** | Định tuyến theo nghiệp vụ (VIP, theo vùng) | Tự viết code; bắt buộc phải nhất quán (cùng input → cùng partition) |

**Ví dụ dễ hình dung — người phân loại thư:** không có key = phát tờ rơi chia đều ngẫu nhiên cho các xe giao hàng. Có key (ví dụ mã vùng ZIP) = mọi thư cùng mã ZIP luôn được xếp lên cùng 1 xe, theo đúng thứ tự đã gửi.

# 2. Vì sao có key thì giữ được thứ tự

- Cùng 1 key → luôn `hash` ra cùng 1 partition → luôn được ghi nối tiếp vào đúng partition đó → giữ đúng thứ tự.
- Kafka chỉ đảm bảo thứ tự **trong 1 partition**, nên: muốn thứ tự theo 1 entity nào đó (đơn hàng, user...) → **dùng ID của entity đó làm key**.

# 3. Chọn key thế nào

| Use case | Key nên dùng |
|---|---|
| Sự kiện đơn hàng | `orderId` |
| Hoạt động người dùng | `userId` |
| Thiết bị IoT | `deviceId` |
| Giao dịch thanh toán | `transactionId` |
| Multi-tenant | `tenantId + ":" + entityId` |

**Quy tắc:** key nên có **nhiều giá trị khác nhau** (ID, UUID). **Tránh** key có ít giá trị (status, quốc gia, tier) vì dễ gây "hot partition". **Không dùng key có thể thay đổi theo thời gian** (giá trị đổi → hash đổi → thứ tự bị vỡ).

# 4. Hot key / Partition skew (lệch tải)

**Triệu chứng:** 1 partition ôm phần lớn traffic → 1 broker và 1 consumer bị quá tải trong khi các partition khác rảnh rỗi. Phát hiện bằng cách theo dõi tốc độ ghi (bytes/s) theo từng partition, hoặc so lag giữa các partition.

| Cách xử lý | Ý tưởng | Lưu ý |
|---|---|---|
| **Key salting** | Thêm hậu tố ngẫu nhiên vào key (`key + "-" + random(0..N)`) để trải ra nhiều partition | **Phá vỡ thứ tự theo key gốc**; chỉ hợp khi downstream sẽ gộp lại kết quả sau |
| **Topic riêng cho key nóng** | Định tuyến các key traffic cao sang 1 topic khác, nhiều partition hơn | Thêm 1 topic để vận hành |
| **Custom partitioner** | Dành hẳn 1 partition riêng cho nhóm khách VIP | Phải đảm bảo nhất quán |
| **Tách nhỏ entity** | Chia 1 entity lớn thành nhiều entity con (ví dụ `accountId:debit`) | Cần thiết kế lại key từ đầu |

# 5. Chọn số lượng partition

**Công thức tham khảo:**
```
số partition = max( Thông_lượng_mục_tiêu / Thông_lượng_1_partition_ghi ,
                     Thông_lượng_mục_tiêu / Thông_lượng_1_consumer_xử_lý )
```

| Traffic | Gợi ý số partition |
|---|---|
| < 10 MB/s | 6–12 |
| 10–100 MB/s | 12–48 |
| > 100 MB/s | 48–200+ |

**Quy tắc đơn giản để bắt đầu:** số partition = `số broker × 2`, và ≥ `số consumer tối đa dự kiến × 2`; **không bao giờ dưới 3** cho topic production.

**Càng nhiều partition càng tốn:** số file mở trên mỗi broker, metadata phía controller và client, và **thời gian rebalance** khi consumer group thay đổi.

# 6. Tăng / giảm số partition — rất quan trọng

- **Không thể giảm số partition.** Giảm sẽ phá công thức `hash % N` và làm mất dữ liệu ở các partition bị xoá bỏ.
- **Tăng partition được, nhưng nguy hiểm nếu topic có key:**

```
Key "user_789" (hash = 412057)
5 partition:  412057 % 5  = Partition 2
10 partition: 412057 % 10 = Partition 7   ← khác hẳn partition trước!
```
Dữ liệu cũ nằm ở Partition 2, dữ liệu mới của cùng key lại vào Partition 7 → consumer đọc **sai thứ tự** giữa dữ liệu cũ và mới.

- Topic **không dùng key**: tăng partition an toàn (không có thứ tự theo key để mất).
- **Cách tăng an toàn cho topic có key** (không downtime): tạo topic mới nhiều partition hơn → tạm dừng producer ghi vào topic cũ → chờ hết lag → chuyển producer sang topic mới → chuyển consumer sang topic mới.
- **Khuyến nghị thực tế:** cấp số partition rộng rãi ngay từ đầu, vì tăng sau này rủi ro hơn nhiều so với có dư một chút.

# 7. Tóm tắt: đảm bảo thứ tự thế nào

| Nhu cầu | Cách làm |
|---|---|
| Mọi sự kiện của 1 entity đúng thứ tự | Dùng ID entity đó làm key |
| Thứ tự toàn cục (toàn topic) | Chỉ dùng **1 partition** (mất khả năng song song) |
| Không cần thứ tự | Dùng sticky/round-robin (không key) |
