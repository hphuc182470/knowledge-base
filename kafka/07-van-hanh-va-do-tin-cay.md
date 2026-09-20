---
title: 07 - Vận hành thực tế (Lag, Poison Message, Song song, Exactly-once)
---

# 1. Consumer Lag — metric quan trọng nhất

```
Lag của 1 partition = Log End Offset (LEO) − Offset đã commit
```

Lag tăng đều nghĩa là consumer không theo kịp tốc độ producer.

| Tín hiệu | Nguyên nhân khả dĩ | Cách xử lý |
|---|---|---|
| Lag lệch, chỉ 1 partition tăng | **Poison message** chặn đúng partition đó | Đẩy sang Dead Letter Queue, cho offset đi tiếp |
| Mọi partition đều tăng | Nút thắt xử lý (DB/API downstream chậm) | Thêm consumer (tối đa = số partition), xử lý theo batch |
| Lag tăng kèm rebalance liên tục | Xử lý vượt quá `max.poll.interval.ms` | Giảm `max.poll.records`, dùng assignor kiểu cooperative |
| 1 partition lag, producer cũng lệch | Hot key (xem file 04) | Xử lý hot key |

# 2. Poison Message & Dead Letter Queue (DLQ)

**Poison message**: 1 message hỏng/không parse được → consumer ném lỗi → không commit được → Kafka phát lại đúng message đó ở lần `poll()` tiếp theo → **lặp vô tận**, offset của partition đó đứng yên, lag tăng mãi (các partition khác vẫn chạy bình thường).

**Mẫu xử lý chuẩn:** retry có giới hạn số lần (kèm khoảng nghỉ tăng dần) → nếu hết số lần retry vẫn lỗi, đẩy message sang một **Dead Letter Topic** riêng, rồi cho offset gốc đi tiếp. Với lỗi chắc chắn không bao giờ tự hết (ví dụ lỗi định dạng dữ liệu), nên bỏ qua bước retry và đẩy thẳng vào DLQ.

- Phân biệt 3 loại lỗi: **tạm thời** (transient — nên retry), **poison** (đẩy DLQ), **sai hợp đồng dữ liệu** (schema sai).
- DLQ phải có **người chịu trách nhiệm theo dõi + hạn xử lý (SLA)**, tránh để phình to mà không ai để ý.

## Retry có thể phá thứ tự

```
Partition 0: [A] lỗi, để sau  →  [B] xử lý thành công  →  [A retry] xử lý SAU B → sai thứ tự
```
Ba cách xử lý (chọn theo mức độ cần giữ đúng thứ tự): (1) chặn hẳn partition khi có lỗi cho tới khi retry xong (an toàn nhưng chậm), (2) tạm dừng (`pause`) rồi tiếp tục (`resume`) partition đó, (3) đẩy DLQ rồi replay lại có kiểm soát sau.

# 3. Xử lý nhanh hơn mà vẫn giữ đúng thứ tự

**Mâu thuẫn cốt lõi:** mặc định Kafka xử lý theo mô hình 1 thread cho 1 partition. Muốn nhanh hơn mà tăng luồng xử lý một cách "ngây thơ" (không kiểm soát) sẽ **phá vỡ thứ tự**.

**Ví dụ dễ hình dung — quầy giao dịch ngân hàng:** thuê thêm 3 nhân viên phục vụ chung 1 hàng khách → giao dịch của **cùng một tài khoản** có thể bị 2 nhân viên xử lý chồng chéo nhau. Cách sửa đúng: định tuyến theo số tài khoản, để giao dịch của cùng 1 tài khoản luôn về tay cùng 1 nhân viên.

**Các lựa chọn xử lý song song (từ đơn giản đến phức tạp):**

| Tình huống | Nên chọn |
|---|---|
| Nút thắt là ghi DB từng dòng, cần giữ thứ tự | Xử lý theo batch (ghi hàng loạt) |
| Không cần giữ thứ tự | Tăng số partition + số consumer |
| Cần thứ tự theo key nhưng vẫn muốn throughput cao | Xử lý theo batch + khoá xử lý theo từng key |
| Cần join/aggregate/window (có trạng thái) | Kafka Streams (xem file 08) |
| Một message xấu chặn cả partition | Tạm dừng (pause) rồi tiếp tục (resume) partition đó |

**Nguyên tắc luôn đúng:** dù chọn cách nào, crash/retry/rebalance đều có thể khiến 1 message được xử lý nhiều lần → hệ thống downstream **luôn phải chống trùng (idempotent)**.

# 4. Ba mức đảm bảo giao nhận (Delivery Semantics)

| Mức | Nghĩa | Rủi ro | Ví dụ |
|---|---|---|---|
| **At-most-once** | 0 hoặc 1 lần | Có thể **mất** mà không biết | `acks=0`, commit trước khi xử lý |
| **At-least-once** | Từ 1 lần trở lên | Có thể **trùng** (không mất) | Mặc định phổ biến nhất: `acks=all` + commit sau xử lý |
| **Exactly-once** | Hiệu ứng cuối cùng xảy ra đúng 1 lần | Cần phối hợp chặt giữa producer–broker–consumer | Kafka EOS (chỉ trong phạm vi Kafka) |

**Sự thật cần nhớ:** đảm bảo giao đúng-1-lần qua bất kỳ mạng nào là **bất khả thi về mặt lý thuyết** (không phân biệt được "đã tới nhưng phản hồi bị mất" với "chưa tới"). Cái Kafka gọi là "exactly-once" (EOS) thực chất là: **giao ít nhất 1 lần + xử lý sao cho các bản trùng không làm thay đổi kết quả cuối cùng** (idempotent).

**EOS chỉ đúng trong phạm vi Kafka → Kafka.** Ghi ra DB ngoài, gọi API khác, gửi email... nằm **ngoài** đảm bảo này — cần tự chống trùng ở tầng ứng dụng.

# 5. Idempotent Consumer — mẫu nền tảng chống trùng

```java
@KafkaListener(topics = "orders")
public void consume(OrderEvent e) {
    try {
        processedEventRepo.save(new ProcessedEvent(e.getId()));   // cột UNIQUE/PRIMARY KEY
    } catch (DataIntegrityViolationException dup) {
        return;   // đã xử lý rồi → bỏ qua, offset vẫn tiến lên bình thường
    }
    orderService.apply(e);   // chỉ bản ghi "thắng" INSERT mới chạy tới đây
}
```

**Bug hay gặp nhất — kiểm tra rồi mới hành động (không nguyên tử):** 2 luồng cùng thấy "chưa tồn tại" rồi cùng xử lý → xử lý trùng (ví dụ: tính phí 2 lần).
- ❌ Sai: `if (!exists(id)) { charge(); save(id); }`
- ✅ Đúng: ghi bản ghi chống trùng **trước**, để ràng buộc UNIQUE của DB làm người gác cổng; hoặc dùng lệnh nguyên tử kiểu `SETNX` của Redis.

**Dedup state nên đặt ở đâu:**
1. **Ngay trong DB đang ghi** (ràng buộc UNIQUE) — đơn giản, nguyên tử nhất, chỉ dùng khi hiệu ứng phụ chính là ghi vào chính DB đó.
2. **Redis** — khi hiệu ứng phụ là hệ thống ngoài (gọi API, gửi email) hoặc nhiều service cần dùng chung trạng thái chống trùng.

**Thiết kế khoá chống trùng (idempotency key) tốt:** ổn định qua các lần retry, gắn với **thao tác nghiệp vụ** (không phải theo lần giao message). Ví dụ tốt: `"order-placed-" + orderId`. Ví dụ xấu: chỉ dùng offset Kafka (đổi khi replay), hoặc UUID sinh mới mỗi lần gửi (retry ra UUID khác → không dedup được).

# 6. Outbox Pattern — khi cần ghi DB và gửi Kafka cùng lúc

Ghi vào DB và gửi message lên Kafka là **2 thao tác tách rời** → có thể bị lệch nếu 1 trong 2 thất bại. **Giải pháp:** ghi cả dữ liệu nghiệp vụ **và** một bản ghi "outbox" trong **cùng 1 transaction của DB**; một tiến trình riêng (thường dùng CDC như Debezium) đọc bảng outbox rồi publish lên Kafka. Kết hợp Kafka EOS + transaction DB nội bộ + consumer chống trùng ⇒ đạt hiệu ứng exactly-once xuyên suốt hệ thống.
