---
title: 01 - Khái niệm cốt lõi (Cluster, Broker, Topic, Partition, Offset...)
---

# 1. Bức tranh tổng

```
Producer ──► [ Broker cluster ] ──► Consumer (theo group)
              Topic "orders"
              ├─ Partition 0: [0][1][2][3][4]...   (leader ở Broker 1)
              ├─ Partition 1: [0][1][2][3]...      (leader ở Broker 2)
              └─ Partition 2: [0][1][2]...         (leader ở Broker 3)
```

**Ví dụ dễ hình dung — bưu điện:** `Topic` giống một **ô thư/danh mục thư** trong bưu điện (ví dụ "thư gửi từ vùng A"). `Producer` là người gửi thư bỏ vào đúng ô. `Consumer` là người tới lấy thư từ ô đó. Nhiều người gửi và nhiều người nhận có thể dùng chung 1 ô thư mà không giẫm chân nhau, vì mỗi lá thư có một số thứ tự (`offset`) rõ ràng.

## 5 thành phần

| Thành phần | Vai trò |
|---|---|
| **Cluster** | Toàn bộ nhóm broker chạy cùng nhau, hoạt động như 1 hệ thống Kafka duy nhất |
| **Broker** | 1 server trong cluster, chịu trách nhiệm lưu và phục vụ message |
| **Topic** | Kênh dữ liệu có tên, đặt kiểu `<domain>.<entity>.<event>` (ví dụ `order.orders.placed`) |
| **Partition** | Một log có thứ tự, bất biến, nằm trong 1 topic. **Đơn vị của song song, lưu trữ và replication** |
| **Producer** | Ứng dụng ghi message vào topic |
| **Consumer** | Ứng dụng đọc message từ topic |

# 2. Cluster & Broker

- **Cluster** = nhiều broker hợp lại, cùng chia sẻ tải và chịu lỗi cho nhau. Production luôn chạy nhiều broker (không chạy 1 broker đơn lẻ).
- **Broker** chịu trách nhiệm:
  1. Lưu message (append vào file `.log`).
  2. Với mỗi partition mà nó làm **leader**: xử lý 100% việc ghi/đọc của partition đó.
  3. Với partition mà nó làm **follower**: kéo dữ liệu từ leader để có đủ bản sao (xem file 02).
  4. Quản lý offset consumer group trong topic nội bộ `__consumer_offsets`.
  5. Phối hợp metadata với Controller (KRaft hoặc ZooKeeper — xem file 03).
- Một broker có thể vừa là leader của partition này, vừa là follower của partition khác.

# 3. Topic & Partition

- **Topic**: kênh logic, không tự nó lưu trữ — dữ liệu thật nằm trong các **partition** của nó.
- **Vì sao phải chia partition:** 1 partition chỉ được xử lý bởi 1 broker (leader) tại 1 thời điểm → nếu topic chỉ có 1 partition thì mọi ghi/đọc dồn vào đúng 1 broker, không scale được. Chia nhiều partition → rải ra nhiều broker → ghi/đọc song song → **partition là đơn vị song song hoá của Kafka**.
- **Partition là 1 log**: dữ liệu chỉ được **thêm vào cuối**, có thứ tự, không sửa/xoá từng phần tử.
- Kafka **chỉ đảm bảo thứ tự trong 1 partition**, không đảm bảo thứ tự giữa các partition khác nhau của cùng topic.

## Thuộc tính quan trọng của Topic

| Thuộc tính | Ý nghĩa | Ảnh hưởng |
|---|---|---|
| Partitions | Số log song song | **Trần** của mức song song (consumer parallelism) và write throughput |
| Replication Factor | Số bản sao mỗi partition | RF=3 chịu được 2 broker chết cùng lúc |
| Retention | `retention.ms` / `retention.bytes` | Dung lượng đĩa cần dùng + khoảng thời gian có thể replay |
| Cleanup policy | `delete` / `compact` / `compact,delete` | Xoá theo thời gian, hay giữ giá trị mới nhất theo key |

# 4. Offset

- Mỗi message trong 1 partition có một **offset**: số nguyên tăng dần, giống số thứ tự trang sách.
- **Offset chỉ có ý nghĩa trong phạm vi 1 partition**: offset 5 của Partition 0 là message hoàn toàn khác offset 5 của Partition 1.
- Offset không bao giờ bị đánh số lại (kể cả sau khi dọn dẹp dữ liệu cũ).
- Consumer tự nhớ (thông qua commit) mình **đã đọc tới offset nào** — đây là chìa khoá cho phép replay và cho phép nhiều consumer group đọc độc lập (xem file 06).

# 5. Producer & Consumer (tóm tắt nhanh — chi tiết ở file 05, 06)

- **Producer**: ứng dụng ghi message vào 1 topic. Producer quyết định message đi vào partition nào (dựa trên key).
- **Consumer**: ứng dụng đọc message từ 1 topic, theo mô hình **pull** (chủ động kéo dữ liệu về, tự kiểm soát tốc độ đọc) — khác với nhiều hệ khác dùng mô hình **push** (broker chủ động đẩy dữ liệu tới).
- **Consumer Group**: nhiều consumer hợp tác đọc chung 1 topic. Ví như một nhóm bạn cùng chia nhau đọc một chồng thư — mỗi lá thư (message) chỉ được **một người trong nhóm** đọc, để công việc được chia đều và không ai đọc trùng của ai. Nhiều group khác nhau có thể cùng đọc 1 topic một cách hoàn toàn độc lập (mỗi group nhớ offset riêng).

# 6. Broker lưu dữ liệu trên đĩa thế nào

```
/var/lib/kafka/data/orders-0/
├── 00000000000000000000.log        # dữ liệu message nhị phân
├── 00000000000000000000.index      # index THƯA: offset → vị trí byte
├── 00000000000000000000.timeindex  # timestamp → offset
├── 00000000000000001048.log        # active segment (đang ghi)
└── leader-epoch-checkpoint         # lịch sử leader epoch
```

- Một partition trên đĩa = 1 thư mục gồm nhiều **segment** (file `.log`). Tên file = offset đầu tiên của segment đó.
- Index là **thưa** (không index từng message, khoảng vài KB mới có 1 mốc) để tiết kiệm bộ nhớ.
- **Active segment**: segment cuối cùng, đang nhận ghi mới.

## Lệnh CLI hay dùng

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group   # xem lag
```

# 7. Vì sao Kafka nhanh

Năm quyết định thiết kế cộng hưởng với nhau:

1. **Ghi tuần tự (sequential I/O)**: chỉ append vào cuối file, không cập nhật tại chỗ → nhanh hơn ghi ngẫu nhiên rất nhiều lần trên cùng loại đĩa.
2. **Dùng OS page cache, không cache trong JVM heap**: tránh cache đôi và GC pause. Consumer đang bắt kịp thường đọc thẳng từ RAM (page cache). Hệ quả: **heap của broker nên để nhỏ** (~6-8GB) để dành RAM cho page cache.
3. **Zero-copy (`sendfile()`)**: I/O truyền thống phải copy dữ liệu qua lại 4 lần (đĩa → cache → heap ứng dụng → socket buffer → card mạng); Kafka dùng syscall để đưa byte thẳng từ page cache ra card mạng, gần như không tốn CPU copy.
4. **Batching**: producer gom nhiều message thành 1 batch, gửi 1 lần → giảm số lần gọi mạng.
5. **Nén theo batch**: producer nén cả batch, broker lưu nguyên dạng đã nén (không giải nén rồi nén lại) → tiết kiệm mạng, đĩa, cache.
