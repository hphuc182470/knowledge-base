---
title: 02 - Replication & Độ bền dữ liệu
---

# 1. Vì sao cần Replication

Một broker có thể chết (crash, bảo trì, mất mạng). Nếu partition chỉ tồn tại trên 1 broker, broker đó chết = mất luôn quyền truy cập dữ liệu. **Replication** = sao chép mỗi partition ra nhiều broker để chịu lỗi.

## Replication Factor (RF)

| RF | Chịu được | Ghi chú |
|:--:|---|---|
| 1 | Không chịu lỗi nào | Broker chết = mất khả năng truy cập |
| 2 | 1 broker chết | |
| **3** | **2 broker chết** | **Chuẩn dùng cho production** |

- Mỗi bản sao (replica) của 1 partition phải nằm trên **broker khác nhau**.

# 2. Leader / Follower / ISR

- **Leader**: 1 trong số các replica của partition, chịu trách nhiệm xử lý **toàn bộ** ghi và đọc của partition đó.
- **Follower**: các replica còn lại, chỉ liên tục sao chép dữ liệu từ leader, sẵn sàng thay thế khi leader chết.
- **ISR (In-Sync Replicas)**: tập hợp các replica (gồm cả leader) đang **bắt kịp** leader trong khoảng thời gian `replica.lag.time.max.ms` (mặc định 30 giây).
  - Follower chậm quá thời gian này → bị **loại khỏi ISR**.
  - Bắt kịp trở lại → được **thêm lại vào ISR**.
- Khi leader chết, Kafka **chỉ bầu leader mới từ các replica đang trong ISR** — để đảm bảo leader mới không thiếu dữ liệu.

# 3. LEO và High Watermark (HW)

```
Leader (Broker 1):     [0][1][2][3]       LEO = 4
Follower A (trong ISR):[0][1][2][3]       LEO = 4
Follower B (chậm):     [0][1]             LEO = 2   ← đã rớt khỏi ISR

High Watermark (HW) = offset mà MỌI replica trong ISR đều đã có
Consumer chỉ đọc được message có offset < HW
```

- **LEO (Log End Offset)**: offset của message *tiếp theo sẽ được ghi* trên 1 replica cụ thể.
- **HW (High Watermark)**: offset cao nhất đã được sao chép tới **toàn bộ replica trong ISR**.
- **Vì sao consumer chỉ đọc tới HW**: để tránh đọc phải dữ liệu có thể **biến mất** — nếu leader chết ngay khi vừa ghi xong nhưng follower chưa kịp sao chép, phần dữ liệu đó (giữa HW và LEO của leader) có thể không tồn tại ở leader mới.

# 4. Cấu hình "không mất dữ liệu" (3 dòng này phải đi cùng nhau)

```properties
# Producer
acks=all
enable.idempotence=true

# Topic / Broker
replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
```

- **`acks=all` một mình chưa đủ**: nếu `min.insync.replicas=1` và ISR chỉ còn đúng leader thì `acks=all` thực chất suy biến thành `acks=1` (không còn bền như mong đợi).
- Khi số replica trong ISR nhỏ hơn `min.insync.replicas`: broker **từ chối ghi** với lỗi `NotEnoughReplicasException` — Kafka chọn **thà từ chối còn hơn ghi không an toàn**. Đây là lỗi có thể retry.
- `unclean.leader.election.enable=false`: nếu **toàn bộ ISR chết hết**, Kafka **không cho phép** một replica bị tụt hậu (ngoài ISR) lên làm leader thay thế. Partition tạm thời không dùng được, nhưng **không mất dữ liệu ngầm**. Bật `true` nghĩa là chấp nhận có thể mất dữ liệu để đổi lấy tính sẵn sàng (chỉ chấp nhận được với dữ liệu ít quan trọng, ví dụ log tổng hợp).

# 5. Đa vùng (Multi-AZ)

- `broker.rack=us-east-1a`: khai báo rack/AZ cho broker → Kafka rải các bản sao qua nhiều rack khác nhau → mất cả 1 AZ vẫn còn bản sao ở AZ khác.
- **Follower reads** (Kafka 2.4+): cho phép consumer đọc từ follower gần mình (cùng AZ) thay vì luôn phải đọc từ leader → tiết kiệm chi phí truyền dữ liệu chéo AZ trên cloud.

# 6. Sự cố thường gặp

| Sự cố | Nguyên nhân | Cách xử lý |
|---|---|---|
| Broker bị OOM kill | Heap JVM đặt quá lớn, chiếm hết RAM lẽ ra dành cho page cache | Giữ heap nhỏ (~6-8GB), để RAM còn lại cho hệ điều hành |
| ISR liên tục vào/ra (flapping) | GC pause dài, mạng/đĩa chậm vượt `replica.lag.time.max.ms` | Tinh chỉnh GC, kiểm tra tốc độ đĩa/mạng |
| Mất dữ liệu sau khi bầu lại leader | Đã bật unclean leader election | Đặt `unclean.leader.election.enable=false` |
