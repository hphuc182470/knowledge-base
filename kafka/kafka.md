# Kafka — Sổ tay cốt lõi (tiếng Việt)

> **Nguồn:** tổng hợp từ thư mục `docs/technical-knowledge/kafka/` của repo `minhkhuong2404/docusaurus-knowledge-base-template` (41 file: core, producer, consumer, advanced, interview).
> **Cách sắp xếp:** không theo cây thư mục gốc, mà theo **mạch tư duy** — đọc từ trên xuống dưới là hiểu Kafka từ gốc tới ngọn.
> Thuật ngữ kỹ thuật giữ nguyên tiếng Anh để dễ tra cứu.

---

## Mạch đọc (bản đồ tổng)

```
[1] Kafka là gì?            → một cuốn "sổ cái" ghi nối tiếp, không xoá khi đọc
[2] Dữ liệu nằm ở đâu?      → Topic → Partition → Offset → Segment → Broker
[3] Vì sao nhanh?           → ghi tuần tự, page cache, zero-copy, batching
[4] Hỏng thì sao?           → Replication, ISR, High Watermark
[5] Ai điều phối cụm?       → ZooKeeper → KRaft (Raft)
[6] Dữ liệu vào partition nào? → Partitioning, key, hot key
[7] Ghi vào an toàn         → Producer, acks, idempotence, transaction
[8] Đọc ra đúng cách        → Consumer, offset, group, rebalance
[9] Khi vận hành hỏng       → Lag, poison message, DLQ, thứ tự vs retry
[10] Xử lý nhanh mà vẫn đúng thứ tự → batch, parallel, virtual thread
[11] Giao đúng 1 lần        → Delivery semantics, EOS, dedup, outbox
[12] Giữ trạng thái mới nhất → Log compaction, tombstone
[13] Hợp đồng dữ liệu       → Schema Registry
[14] Xử lý luồng            → Kafka Streams
[15] Tích hợp hệ thống      → Kafka Connect, SMT, MirrorMaker 2
[16] Làm cho nhanh          → Performance tuning
[17] Vận hành               → Monitoring & Operations
[18] Bảo mật & quản trị     → Auth, ACL, Governance
[19] Tổng kết               → Cheat sheet, câu hỏi phỏng vấn, chỗ tài liệu gốc mâu thuẫn
```

---

# PHẦN 1 — Kafka là gì?

## 1.1 Định nghĩa cốt lõi

**Kafka = distributed commit log** (nhật ký commit phân tán) + nền tảng event streaming.

- Sinh ra ở LinkedIn để thay các message broker cũ.
- Cốt lõi là một **log ghi nối tiếp (append-only), bất biến (immutable)** lưu trên đĩa.
- Message **không bị xoá khi được đọc**. Nó bị xoá theo **thời gian** (`retention.ms`, mặc định 7 ngày) hoặc **dung lượng** (`retention.bytes`).
- Hệ quả quan trọng: **replay** (đọc lại quá khứ), **nhiều consumer group đọc độc lập cùng một dữ liệu**, debug "quay ngược thời gian".

## 1.2 Kafka khác queue truyền thống thế nào

| Tiêu chí | Kafka | RabbitMQ / ActiveMQ |
|---|---|---|
| Bản chất | Log phân tán (ghi đĩa) | Message broker (Exchange + Queue, AMQP) |
| Giữ message | Theo thời gian/dung lượng, hoặc mãi mãi (compaction) | Xoá ngay khi consumer ACK |
| Mô hình nhận | **Consumer pull** | Broker **push** |
| Replay | ✅ Có (đặt lại offset) | ❌ Không |
| Routing | Topic + partition key | Exchange: Direct / Fanout / Topic / Headers |
| Thứ tự | Đảm bảo **trong 1 partition** | Trong 1 queue (nhưng bị phá bởi competing consumers) |
| Scale | Ngang (thêm partition) | Chủ yếu dọc |
| Hợp với | Event streaming, event sourcing, log aggregation, CDC | Task queue, routing phức tạp, RPC, TTL từng message |

**Khi nào chọn Kafka:** cần replay, nhiều hệ thống đọc cùng một luồng (fan-out), throughput rất cao, lưu lịch sử dài, event sourcing, stream processing.
**Khi nào chọn RabbitMQ:** routing linh hoạt, TTL/dead-letter theo từng message, worker pool không cần replay, request-reply.

## 1.3 Bài toán Kafka giải quyết (ví dụ checkout)

- **Kiểu đồng bộ:** Checkout gọi lần lượt Payment → Inventory → Fraud → Notification qua HTTP. Một service chậm/chết là cả đơn hàng hỏng.
- **Kiểu Kafka:** Checkout chỉ ghi 1 event `OrderPlaced` rồi trả về ngay (vài ms). Mỗi service là **một consumer group riêng**. Notification chết 2 tiếng? Chạy lại sẽ đọc tiếp từ offset cuối, **không mất dữ liệu, không ảnh hưởng ai**.

## 1.4 Bốn trụ cột kiến trúc

| Trụ cột | Cơ chế | Lợi ích |
|---|---|---|
| Append-only disk I/O | Ghi tuần tự vào file `.log` | Băng thông đĩa rất cao, không seek ngẫu nhiên |
| Zero-copy | Syscall `sendfile()` | Đưa byte từ page cache thẳng ra card mạng, không qua JVM heap |
| Log partitioning | Topic chia partition trên nhiều broker | Scale đọc/ghi gần tuyến tính |
| ISR replication | Nhóm replica đang bắt kịp leader | Bền vững có thể tinh chỉnh (`acks=all` + `min.insync.replicas`) |

**Trade-off lớn nhất khi tuning:** **Latency ⇄ Durability ⇄ Throughput**.
Ví dụ: `acks=all` + idempotence = bền nhất nhưng chậm hơn; `linger.ms` + `batch.size` lớn = throughput cao nhưng thêm độ trễ nhỏ.

---

# PHẦN 2 — Dữ liệu nằm ở đâu? (Khối xây dựng)

## 2.1 Sơ đồ tổng

```
Producer ──► [ Broker cluster ] ──► Consumer (theo group)
              Topic "orders"
              ├─ Partition 0: [0][1][2][3][4]...   (leader ở Broker 1)
              ├─ Partition 1: [0][1][2][3]...      (leader ở Broker 2)
              └─ Partition 2: [0][1][2]...         (leader ở Broker 3)
```

## 2.2 Năm thành phần

| Thành phần | Vai trò |
|---|---|
| **Producer** | Ghi message vào topic |
| **Broker** | Server lưu và phục vụ message |
| **Topic** | Kênh có tên (đặt kiểu `<domain>.<entity>.<event>`) |
| **Partition** | Log có thứ tự, bất biến. **Đơn vị của song song, lưu trữ, replication** |
| **Consumer** | Đọc message từ topic |

## 2.3 Offset

- Mỗi message trong partition có một **offset**: số nguyên 64-bit tăng dần.
- **Offset là theo từng partition**: offset 5 của P0 khác hẳn offset 5 của P1.
- Offset không bao giờ bị đánh số lại, kể cả sau compaction.

## 2.4 Thuộc tính của Topic

| Thuộc tính | Ý nghĩa | Ảnh hưởng |
|---|---|---|
| Name | Định danh | Gắn với ACL, discovery |
| Partitions | Số log song song | **Trần** của consumer parallelism và write throughput |
| Replication Factor | Số bản sao | RF=3 chịu được 2 broker chết |
| Retention | `retention.ms` / `retention.bytes` | Dung lượng đĩa + cửa sổ replay |
| Cleanup policy | `delete` / `compact` / `compact,delete` | Xoá lịch sử hay giữ bản mới nhất theo key |

## 2.5 Broker lưu dữ liệu trên đĩa thế nào

```
/var/lib/kafka/data/orders-0/
├── 00000000000000000000.log        # RecordBatch nhị phân
├── 00000000000000000000.index      # index THƯA: offset → vị trí byte
├── 00000000000000000000.timeindex  # timestamp → offset (offsetsForTimes)
├── 00000000000000001048.log        # active segment (đang ghi)
└── leader-epoch-checkpoint         # lịch sử leader epoch (để fencing)
```

- Partition = thư mục gồm nhiều **segment**. Tên file = offset đầu tiên của segment.
- Index **thưa**: khoảng 4KB mới có một entry (`index.interval.bytes`), không index từng record.
- **Active segment** là segment đang nhận ghi.

## 2.6 Trách nhiệm của Broker

1. Lưu message (append vào `.log`).
2. **Leader** của partition: xử lý 100% ghi/đọc của partition đó.
3. **Follower**: kéo dữ liệu từ leader để đủ bản sao.
4. Quản lý offset của consumer group trong topic nội bộ `__consumer_offsets`.
5. Phối hợp metadata với Controller (KRaft hoặc ZooKeeper).

## 2.7 Lệnh CLI hay dùng

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group   # xem lag
```

> **Chuyển tiếp:** Ghi tuần tự nghe đơn giản, vậy vì sao Kafka nhanh đến thế? → Phần 3.

---

# PHẦN 3 — Vì sao Kafka nhanh?

Năm quyết định thiết kế cộng hưởng với nhau:

### 1) Sequential I/O
Chỉ **append vào cuối file**, không cập nhật tại chỗ. Đĩa ghi tuần tự nhanh gấp nhiều lần ghi ngẫu nhiên (hàng trăm MB/s trên SATA SSD, vài GB/s trên NVMe).

### 2) Dùng OS Page Cache, KHÔNG cache trong JVM heap
- Cache trong JVM gây: cache đôi (JVM + OS), GC pause nặng, object header phình bộ nhớ 2–4 lần.
- Kafka để **hệ điều hành cache**. Consumer đang bắt kịp thường đọc thẳng từ RAM.
- **Hệ quả cấu hình:** heap broker nhỏ (**~6–8GB**), phần RAM còn lại dành cho page cache. Máy 32GB → heap 6GB, ~24GB cho page cache.

### 3) Zero-copy `sendfile()`
- **I/O truyền thống:** 4 lần copy + 4 lần context switch (Đĩa → Page Cache → JVM heap → Socket buffer → NIC).
- **Kafka:** `FileChannel.transferTo()` → `sendfile()`. NIC dùng **scatter-gather DMA** lấy byte thẳng từ page cache. CPU gần như không copy.
- **Bẫy TLS:** TLS ở user-space (Java `SSLEngine`) phá zero-copy vì dữ liệu phải qua JVM để mã hoá. Giải pháp: **Kernel TLS (kTLS)**, Linux 4.17+, hoặc TLS offload phần cứng.

### 4) Batching
Producer gom nhiều record thành `RecordBatch`, broker phục vụ theo batch, consumer fetch theo batch → giảm số round-trip và overhead header TCP.

### 5) Compression theo batch
Producer nén cả batch → broker **lưu nguyên dạng nén, không giải nén/nén lại** → consumer mới giải nén. Tiết kiệm mạng, đĩa, và cả page cache.

> **Chuyển tiếp:** Nhanh mà broker chết thì mất dữ liệu sao? → Phần 4.

---

# PHẦN 4 — Replication: sống sót khi broker hỏng

## 4.1 Replication Factor (RF)

| RF | Chịu lỗi | Ghi chú |
|:--:|---|---|
| 1 | Không | Broker chết = mất khả năng truy cập |
| 2 | 1 broker | |
| **3** | **2 broker** | **Chuẩn production** |

- Mỗi replica của một partition phải nằm trên **broker khác nhau**. RF > số broker sống → lỗi `InvalidReplicationFactorException`.

## 4.2 Leader / Follower / ISR

- **Leader**: nhận mọi ghi/đọc của partition.
- **Follower**: chỉ sao chép từ leader, sẵn sàng thay thế.
- **ISR (In-Sync Replicas)**: tập replica (gồm leader) **bắt kịp leader trong `replica.lag.time.max.ms`** (mặc định 30s).
- Follower chậm quá → bị **loại khỏi ISR**; bắt kịp lại → được **thêm lại**.
- Khi leader chết, Controller **chỉ bầu leader mới từ ISR**.

## 4.3 LEO và High Watermark

```
Leader (B1):   [0][1][2][3]       LEO=4
Follower 1 (ISR): [0][1][2][3]    LEO=4
Follower 2 (chậm): [0][1]         LEO=2   ← đã rớt khỏi ISR

High Watermark (HW) = mức đã được MỌI replica trong ISR có
Consumer chỉ đọc được phần < HW
```

- **LEO (Log End Offset):** offset của message *sắp* được ghi tiếp trên một replica.
- **HW (High Watermark):** offset cao nhất đã replicate tới **toàn bộ ISR**.
- **Vì sao consumer chỉ đọc tới HW:** để không bao giờ đọc thứ có thể **biến mất** nếu leader chết trước khi replicate xong.

## 4.4 Cấu hình "zero data loss" (phải đi CÙNG NHAU)

```properties
# Producer
acks=all
enable.idempotence=true

# Topic/Broker
replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
```

- `acks=all` một mình **chưa đủ**: nếu `min.insync.replicas=1` và ISR chỉ còn leader thì `acks=all` suy biến thành `acks=1`.
- Khi ISR < `min.insync.replicas`: broker từ chối ghi với `NotEnoughReplicasException`. Kafka chọn **nhất quán/bền vững hơn sẵn sàng**. Đây là lỗi *retriable* (producer thử lại tới hết `delivery.timeout.ms`).
- `unclean.leader.election.enable=false`: nếu **toàn bộ ISR chết**, không cho replica lạc hậu lên làm leader. Partition tạm không dùng được, nhưng **không mất dữ liệu**. Bật `true` = có thể mất dữ liệu thầm lặng (chỉ chấp nhận với log aggregation kiểu "mất vài dòng log không sao").

## 4.5 Đa AZ: Rack-aware và Follower Reads

- `broker.rack=us-east-1a`: rải replica qua nhiều rack/AZ → mất cả AZ vẫn còn bản sao.
- **Follower reads** (Kafka 2.4+): consumer đặt `client.rack` để đọc từ follower cùng AZ → **tiết kiệm phí egress giữa các AZ**.

## 4.6 Lỗi thường gặp ở broker

| Sự cố | Nguyên nhân | Cách xử |
|---|---|---|
| Broker bị OOM kill | Heap JVM quá lớn, bóp nghẹt page cache | Heap ~6GB, để RAM cho OS |
| ISR flapping (ra vào liên tục) | GC pause, mạng/đĩa chậm vượt `replica.lag.time.max.ms` | Tune GC, kiểm tra `iostat` |
| Mất dữ liệu sau bầu leader | Unclean election | `unclean.leader.election.enable=false` |

> **Chuyển tiếp:** Ai quyết định leader là ai? Ai lưu metadata của cả cụm? → Phần 5.

---

# PHẦN 5 — Điều phối cụm: từ ZooKeeper đến KRaft

## 5.1 Vấn đề của ZooKeeper (kiến trúc cũ)

Phải vận hành **2 hệ phân tán riêng** (ZooKeeper + Kafka). Ba nỗi đau:

1. **Hạ tầng kép:** cài đặt, scale, vá lỗi, giám sát hai cụm.
2. **Nghẽn metadata:** topic/partition/ACL/replica state nằm ở ZNode; watch notification bão hoà quanh mức **~200.000 partition**.
3. **Failover chậm:** Controller mới phải **nạp lại toàn bộ cây ZNode vào RAM** rồi mới làm việc được → cụm bị đóng băng từ hàng chục giây tới lâu hơn nhiều với cụm cực lớn.

## 5.2 KRaft hoạt động thế nào

**Ý tưởng:** biến chính metadata thành một **topic nội bộ** (`__cluster_metadata`), quản lý theo **event sourcing** + thuật toán đồng thuận **Raft**.

- **Controller quorum:** một nhóm nhỏ node (thường 3 hoặc 5) được bầu ra **Active Controller (leader)** bằng Raft.
- **Metadata log:** mọi thay đổi (tạo topic, đổi leader, ACL, config…) được Active Controller ghi tuần tự vào log này.
- **Đồng bộ thời gian thực:** Standby Controllers và cả Broker đều **kéo (pull) stream** metadata và giữ **bản sao đầy đủ trong RAM**.
- **Kết quả:** khi Active Controller chết, controller khác lên leader trong **dưới 1 giây (cỡ vài trăm ms)**, không phải nạp lại từ đĩa.
- **Snapshot (`.checkpoint`):** định kỳ chụp toàn bộ metadata để log không phình vô hạn; broker mới vào chỉ nạp snapshot mới nhất + phần phát sinh sau đó.

## 5.3 So sánh nhanh

| Tiêu chí | ZooKeeper | KRaft |
|---|---|---|
| Hệ thống | 2 (ZK + Kafka) | **1 (một binary)** |
| Controller failover | Hàng chục giây → lâu hơn | **< 1s** |
| Giới hạn partition | ~200.000 | **> 1.000.000** |
| Lan truyền metadata | Controller **push** RPC | Broker **pull** stream |
| Giao thức đồng thuận | ZAB | **Raft** |
| Trạng thái | Bị **gỡ bỏ ở Kafka 4.0** | Chuẩn từ Kafka 3.3+ |

```properties
# server.properties (KRaft)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
```

## 5.4 Raft tóm gọn (để hiểu KRaft)

**Bài toán:** nhiều máy đồng ý về một chuỗi thao tác dù có máy chết/chậm/đứt mạng. Không giải quyết → **split-brain** (hai máy cùng tưởng mình là leader, lịch sử phân kỳ).

**Ví dụ quán cà phê 5 nhân viên:** bầu 1 trưởng ca; mọi đơn phải qua trưởng ca; trưởng ca chép đơn rồi đọc cho 4 người còn lại ghi; khi **≥ 3/5 người đã ghi** (quorum) thì đơn được **commit**. Trưởng ca ốm → mọi người bầu lại; ai sổ ít đơn hơn mình thì **không được bỏ phiếu cho**.

**Ba trạng thái:** `Follower` (thụ động) → `Candidate` (tranh cử) → `Leader` (điều hành, gửi heartbeat).

**Ba trụ cột:**

| Trụ cột | Ý chính |
|---|---|
| **Leader Election** | Thời gian logic chia theo **Term** (số nguyên tăng dần). Heartbeat ~50–100ms; election timeout **ngẫu nhiên** ~150–300ms để tránh split vote |
| **Log Replication** | Leader append → gửi `AppendEntries` → khi **đa số** xác nhận thì entry thành *Committed* |
| **Safety** | *Election safety* (mỗi term tối đa 1 leader), *Leader completeness* (entry đã commit không mất ở các leader sau), *Log matching* (cùng index+term ⇒ cùng nội dung và cùng lịch sử trước đó) |

**KRaft khác Raft chuẩn:** replicate theo kiểu **pull** (standby dùng `Fetch`, giống replication dữ liệu của Kafka), dữ liệu là **event-sourced metadata log**, thay đổi thành viên quorum ghi thẳng vào log.

## 5.5 Cạm bẫy vận hành KRaft

1. **Tách role trên production tải cao.** `process.roles=broker,controller` (combined) chỉ nên dùng cho dev/test. Nếu broker kiêm controller bị nghẽn I/O/GC, **heartbeat Raft bị gián đoạn → rớt leader liên tục → tê liệt cả cụm**. Production: **dedicated controller nodes**.
    - Trên Kubernetes với **Strimzi**: tách 2 `KafkaNodePool` — `controller-pool` (3 replica, đĩa nhỏ nhưng IOPS cao, role `controller`) và `broker-pool` (nhiều replica, đĩa lớn, role `broker`); bật `strimzi.io/kraft: "enabled"`.
2. **Luật quorum `N = 2F + 1`**, luôn dùng **số lẻ**:
    - 3 controller chịu 1 node chết; 5 controller chịu 2 node chết.
    - 2 controller: chết 1 là mất quorum. 4 controller: chỉ chịu 1 (như 3) mà tốn thêm node.
    - Multi-AZ: 5 controller chia 2-2-1 qua 3 AZ.
3. **Migrate ZK → KRaft** (KIP-866): pha dual-write → chuyển quyền controller → ngắt ZK. **Không thể đảo ngược** ở bước cuối, **phải backup ZNode trước**.

## 5.6 Chống split-brain bằng LeaderEpoch

- Mỗi nhiệm kỳ leader có `LeaderEpoch` tăng dần.
- Controller A bị partition mạng → quorum bầu B với epoch 6. Khi A hồi phục và gửi lệnh với epoch 5, broker thấy `5 < 6` → **từ chối (fence out)**.

## 5.7 Nếu 2/3 controller chết?

Mất quorum → metadata thành **read-only** (không tạo topic, không đổi config, không rebalance partition). **Data broker vẫn phục vụ producer/consumer bình thường** trên topic hiện có nhờ bản sao metadata trong RAM.

> **Chuyển tiếp:** Cụm đã bền và có điều phối. Bây giờ, một message cụ thể sẽ vào partition nào? → Phần 6.

---

# PHẦN 6 — Partitioning: message vào partition nào?

Partition key quyết định **3 thứ**: (1) broker nào lưu, (2) consumer nào xử lý, (3) phạm vi đảm bảo thứ tự.

## 6.1 Bốn chiến lược

| Chiến lược | Khi nào | Cách hoạt động |
|---|---|---|
| **Key-based** (mặc định khi có key) | Cần thứ tự theo entity | `toPositive(murmur2(keyBytes)) % numPartitions` |
| **Round-robin** (Kafka < 2.4, không key) | Cũ | Xoay vòng từng message → batch nhỏ, kém hiệu quả |
| **Sticky** (mặc định khi không key, ≥ 2.4) | Throughput, không cần thứ tự | Dồn vào một partition tới khi đầy `batch.size` hoặc hết `linger.ms`, rồi mới đổi partition |
| **Custom `Partitioner`** | Routing nghiệp vụ (VIP, theo vùng) | Tự cài; **phải deterministic và stateless** |

**Ví dụ "người phân thư":** không key = phát tờ rơi chia đều các xe. Có key (mã ZIP) = mọi thư cùng ZIP luôn lên cùng một xe, đúng thứ tự gửi.

- Dùng `Utils.toPositive()` (xoá bit dấu) chứ **không dùng `Math.abs()`** vì `Math.abs(Integer.MIN_VALUE)` vẫn âm.
- Kafka dùng **MurmurHash2**: phân bố đều, nhanh, cho kết quả giống nhau giữa các ngôn ngữ.

## 6.2 Chọn key thế nào

| Use case | Key nên dùng |
|---|---|
| Order events | `orderId` |
| Hoạt động người dùng | `userId` |
| IoT | `deviceId` |
| Thanh toán | `transactionId` |
| Multi-tenant | `tenantId + ":" + entityId` |

**Quy tắc:** key **high-cardinality** (ID, UUID). **Tránh** key ít giá trị (status, country, tier) vì sinh **hot partition**. **Không dùng key có thể thay đổi** (hash đổi = thứ tự vỡ).

## 6.3 Hot key / Partition skew

Triệu chứng: một partition ôm 80% traffic → 1 broker + 1 consumer quá tải, các cái khác rảnh. Phát hiện: `BytesInPerSec` theo partition, hoặc lag lệch giữa các partition.

| Cách xử | Ý tưởng | Lưu ý |
|---|---|---|
| **Key salting** | `key + "-" + random(0..N)` để trải ra N partition | **Phá thứ tự theo key**; chỉ dùng cho aggregate/stateless, downstream phải gộp lại |
| **Dedicated hot topic** | Định tuyến key nóng sang topic riêng, nhiều partition hơn | Thêm 1 topic để vận hành |
| **Custom partitioner** | Dành partition riêng cho VIP | Phải deterministic |
| **Application sharding** | Tách entity thành sub-entity (`accountId:debit`) | Cần thiết kế lại key |

## 6.4 Chọn số partition

```
partitions = max( T / Tp , T / Tc )
  T  = throughput mục tiêu
  Tp = throughput của 1 partition phía producer (đo bằng perf test)
  Tc = throughput xử lý của 1 consumer trên 1 partition
```

| Traffic | Gợi ý số partition |
|---|---|
| < 10 MB/s | 6–12 |
| 10–100 MB/s | 12–48 |
| > 100 MB/s | 48–200+ |

**Quy tắc ngón tay cái:** bắt đầu = `số broker × 2`; ≥ `số consumer đỉnh × 2`; **không bao giờ < 3** cho topic production; **đo, đừng đoán** (`kafka-producer-perf-test.sh`).

**Quá nhiều partition thì tốn:** file handle mỗi broker (mỗi partition ~3 handle), metadata ở controller, metadata phía client, **thời gian rebalance** tăng. Thực tế: ~4.000–10.000 partition/broker tuỳ phần cứng.

## 6.5 Tăng / giảm partition

- **Không thể giảm partition.** Giảm sẽ phá `hash % N` và mất dữ liệu ở partition bị xoá. Muốn ít hơn: tạo topic mới rồi migrate.
- **Tăng partition được, nhưng với topic có key thì NGUY HIỂM:**

```
Key "user_789" (hash=412057)
5 partition:  412057 % 5  = P2
10 partition: 412057 % 10 = P7   ← khác partition!
```
Dữ liệu cũ nằm ở P2, dữ liệu mới vào P7 → consumer xử lý **sai thứ tự** giữa cũ và mới.

- Topic **không key** thì tăng partition an toàn (không có thứ tự theo key để mất).
- **Cách tăng an toàn cho topic có key (không downtime):** tạo topic `orders-v2` nhiều partition hơn → tạm dừng producer ở v1 → chờ lag v1 về 0 → chuyển producer sang v2 → chuyển consumer sang v2.
- **Khuyến nghị:** cấp partition **hào phóng ngay từ đầu**, vì tăng thì phá mapping, còn giảm thì không được.

## 6.6 Đảm bảo thứ tự: tóm tắt

| Nhu cầu | Cách làm |
|---|---|
| Mọi event của entity X đúng thứ tự | Dùng entity ID làm key |
| Thứ tự toàn cục | **1 partition** (mất song song) |
| Không cần thứ tự | Sticky/round-robin |
| Thứ tự chéo giữa entity | Không làm được bằng Kafka thuần; dùng 1 partition hoặc sequencer ngoài / Lamport timestamp |

> **Chuyển tiếp:** Biết message đi đâu rồi. Giờ nói về việc **ghi** cho an toàn: Producer. → Phần 7.

---

# PHẦN 7 — Producer: ghi vào an toàn

## 7.1 Việc của producer

1. Serialize key/value → 2. Chọn partition → 3. Gom batch → 4. Xử lý retry/lỗi → 5. Quản lý cam kết giao hàng qua `acks`.

## 7.2 Điều gì xảy ra khi gọi `send()`

```
send(record)
  → Serializer (key + value)
  → Partitioner (chọn partition)
  → RecordAccumulator  (buffer trong RAM, 1 hàng đợi batch cho mỗi TopicPartition)
  → Sender thread (nền) rút batch khi đầy batch.size HOẶC hết linger.ms
  → gom batch theo broker-leader, gửi ProduceRequest
  → broker append (và replicate tới ISR nếu acks=all)
  → ProduceResponse → callback / Future
```

- `send()` **luôn bất đồng bộ**: chỉ đưa vào buffer rồi return ngay.
- **RecordAccumulator tồn tại để tăng throughput** — gửi từng message một thì mỗi message tốn một round-trip.

## 7.3 Ba kiểu gửi

| Kiểu | Đặc điểm |
|---|---|
| Fire-and-forget | Nhanh, có thể mất message khi lỗi |
| Sync (`.get()`) | Chặn luồng → throughput có thể < 100 msg/s. **Tránh** |
| **Async + callback** | Khuyến nghị. **Luôn xử lý tham số exception** (không thì mất dữ liệu thầm lặng) |

## 7.4 Cấu hình quan trọng

```properties
# Độ tin cậy
acks=all
retries=2147483647
delivery.timeout.ms=120000          # tổng ngân sách thời gian cho 1 lần gửi (kể cả retry)
max.in.flight.requests.per.connection=5

# Throughput / batching
linger.ms=5           # đợi tối đa 5ms để gom batch (mặc định 0)
batch.size=32768      # trần kích thước batch cho mỗi partition (mặc định 16KB)
buffer.memory=33554432
compression.type=lz4  # none|gzip|snappy|lz4|zstd
```

- `delivery.timeout.ms >= linger.ms + request.timeout.ms`.
- **Lỗi retriable** (thử lại): `NetworkException`, `LeaderNotAvailableException`, `TimeoutException`, `NotEnoughReplicasException`.
- **Lỗi không retriable:** `RecordTooLargeException`, `SerializationException`.

## 7.5 Bẫy phổ biến của producer

| Bẫy | Triệu chứng | Sửa |
|---|---|---|
| `.get()` mỗi lần send | Throughput cực thấp | Dùng callback |
| `buffer.memory` quá nhỏ | `BufferExhaustedException` | Tăng `buffer.memory`/`max.block.ms` |
| Không xử lý exception trong callback | Mất dữ liệu thầm lặng | Luôn xử lý |
| Không nén | Tốn băng thông | `lz4` hoặc `zstd` |

## 7.6 `acks` — 3 mức cam kết

| `acks` | Cơ chế | Rủi ro mất dữ liệu | Dùng cho |
|---|---|---|---|
| `0` | Không đợi phản hồi | **Rất cao**, mất thầm lặng | Telemetry, metrics, clickstream chấp nhận mất |
| `1` | Đợi **leader** ghi xong | Vừa: leader ack rồi chết **trước khi follower kịp lấy** → mất dù producer thấy "thành công" | Log ứng dụng, event không quan trọng |
| `all` (`-1`) | Đợi **toàn bộ ISR** | **Không mất** (khi kèm `min.insync.replicas ≥ 2`) | Tài chính, đơn hàng, CDC |

> Từ Kafka 3.0 mặc định của producer là `acks=all` + `enable.idempotence=true` (xem Phần 19 về mâu thuẫn tài liệu gốc).

## 7.7 Idempotent Producer — chống trùng do retry

**Vấn đề:** producer gửi batch → broker ghi xong → **ACK bị mất trên đường về** → producer retry → broker ghi **lần 2** (trùng).

**Giải pháp (`enable.idempotence=true`, mặc định từ Kafka 3.0):**

- Producer xin **PID** (Producer ID, 64-bit) từ broker.
- Mỗi batch gắn `(PID, Epoch, SequenceNumber)`; sequence tăng đơn điệu theo từng `(PID, partition)`.
- Broker nhớ vài sequence gần nhất (cửa sổ **5**); thấy lại `(PID=42, Seq=7)` thì **bỏ qua bản trùng nhưng vẫn trả ACK thành công**.
- Kéo theo ràng buộc: `acks=all`, `retries=MAX`, `max.in.flight ≤ 5` (vượt cửa sổ 5 thì có thể gặp `OutOfOrderSequenceException`).
- Chi phí gần như không đáng kể (< ~3% throughput).

**Giới hạn cần nhớ:**
1. Chỉ chống trùng **trong 1 phiên producer**. Producer **restart → PID mới → dedup reset**. Muốn xuyên qua restart → cần **transaction với `transactional.id`**.
2. Chỉ chống trùng do **retry**, không chống việc **ứng dụng tự gửi 2 lần**.
3. Không giúp gì cho trùng lặp ở phía consumer hay hệ thống ngoài Kafka.

**`max.in.flight` và thứ tự:** không idempotence + `max.in.flight > 1` + retry → batch lỗi retry sau batch thành công → **đảo thứ tự**. Có idempotence: an toàn tới 5. Không idempotence mà cần đúng thứ tự → đặt = 1 (đắt về throughput).

## 7.8 Transactions — ghi nguyên tử nhiều partition

**Vì sao cần:** idempotence không giúp khi (1) ghi **nhiều partition/topic phải nguyên tử**, (2) pipeline **consume → process → produce** phải exactly-once, (3) producer **restart**.

**Các thành phần:**

| Thành phần | Vai trò |
|---|---|
| **Transaction Coordinator** | Một broker giữ trạng thái transaction (chọn theo hash của `transactional.id`) |
| `__transaction_state` | Topic nội bộ (mặc định 50 partition) lưu trạng thái: `Empty`, `Ongoing`, `PrepareCommit`, `CompleteCommit`, `PrepareAbort`, `CompleteAbort` |
| **Transaction markers** | Bản ghi đặc biệt COMMIT/ABORT ghi vào từng partition liên quan |

**Vòng đời (2-Phase Commit):**

```
1. initTransactions()        → coordinator tăng Epoch, fence producer cũ cùng transactional.id
2. beginTransaction()        → chỉ đổi trạng thái phía client
3. send(...) nhiều partition → AddPartitionsToTxn; broker giữ lại record với read_committed
4. sendOffsetsToTransaction  → offset consumer trở thành CÙNG một transaction
5. commitTransaction()
     Pha 1: ghi PrepareCommit vào __transaction_state (điểm không quay đầu)
     Pha 2: ghi COMMIT marker vào mọi partition → CompleteCommit
```

**Consumer phải opt-in:** `isolation.level=read_committed` (mặc định `read_uncommitted` → vẫn đọc cả record của transaction bị abort).

**LSO (Last Stable Offset):** consumer `read_committed` không đọc vượt LSO = offset mà mọi transaction phía trước đã có kết quả cuối (commit/abort). **Một transaction treo có thể chặn cả partition** ngay cả với consumer không đọc record của nó → `transaction.timeout.ms` (mặc định 60s) là knob quan trọng: hạ thấp để transaction chết bị abort sớm; quá thấp thì abort oan transaction chậm.

**Zombie fencing (ba con số khác nhau, đừng nhầm):**

| Định danh | Phạm vi | Chống cái gì |
|---|---|---|
| **PID** | Mỗi phiên producer | Xác định ai ghi record |
| **Sequence number** | Theo `(PID, partition)` | Trùng do retry mạng trong cùng instance |
| **Producer Epoch** | Tăng mỗi lần `initTransactions()` cho 1 `transactional.id` | **Hai instance cùng vai** (pod cũ + pod mới) ghi cùng lúc |

Producer cũ ("zombie") gửi với epoch thấp hơn → bị `ProducerFencedException`, phải **đóng lại, không retry**.

**Bẫy Kubernetes:** nếu `transactional.id` lấy theo **tên pod** thì pod mới có ID mới → **không fence được pod cũ**. `transactional.id` phải **ổn định theo vai trò logic**, không theo tiến trình.

**Consume-process-produce nguyên tử:** `sendOffsetsToTransaction` đưa offset input vào cùng transaction với output → commit thì cả hai hiện ra, abort thì cả hai biến mất.

**Chi phí:** vài ms mỗi commit (coordinator round-trip). Gom nhiều record vào 1 transaction để chia đều chi phí. Đặt `transaction.timeout.ms` hơi thấp hơn `max.poll.interval.ms` của consumer.

> **Chuyển tiếp:** Ghi xong. Bây giờ đọc ra: Consumer. → Phần 8.

---

# PHẦN 8 — Consumer: đọc ra đúng cách

## 8.1 Mô hình cốt lõi

- Consumer **pull** theo nhịp của mình → tự kiểm soát throughput, backpressure, replay; broker không cần theo dõi trạng thái từng consumer.
- Đánh đổi: hơi trễ khi ít dữ liệu (giảm nhẹ bằng `fetch.max.wait.ms`).
- Consumer giữ **một offset cho mỗi partition được gán**.

## 8.2 Vòng poll

```java
while (running) {
    ConsumerRecords<K,V> records = consumer.poll(Duration.ofMillis(100));
    for (var r : records) process(r);
    consumer.commitSync();   // hoặc commitAsync
}
```

`poll()` phải được gọi đều đặn vì nó: (1) lấy record, (2) là nhịp sống của consumer với coordinator, (3) kích hoạt rebalance khi thành viên thay đổi. **Chặn lâu trong vòng poll → bị đá khỏi group.**

## 8.3 Offset và commit — nơi quyết định at-least-once / at-most-once

| Kiểu | Hành vi | Hệ quả |
|---|---|---|
| **Commit TRƯỚC khi xử lý** | Xử lý hỏng sau commit → message mất | **At-most-once** (mất dữ liệu) |
| **Commit SAU khi xử lý** | Crash giữa xử lý và commit → xử lý lại | **At-least-once** (có thể trùng) → downstream phải **idempotent** |
| Dùng transaction | Offset + output cùng nguyên tử | Exactly-once (trong Kafka) |

- `enable.auto.commit=false` + **commit thủ công sau khi xử lý** là khuyến nghị.
- `commitSync()`: chặn, có retry, an toàn nhưng thêm độ trễ. `commitAsync()`: không chặn, **không retry** (retry có thể ghi đè offset mới hơn bằng offset cũ). **Best practice:** `commitAsync` trong vòng poll, `commitSync` trong khối `finally`/shutdown để chốt offset cuối.
- **Spring Kafka `AckMode`:** `BATCH`, `RECORD`, `MANUAL`, `MANUAL_IMMEDIATE`, `COUNT`, `TIME`, `COUNT_TIME`. Phổ biến: `MANUAL_IMMEDIATE`.

**Crash mà chưa commit?** Restart → đọc **offset đã commit cuối** từ `__consumer_offsets` → xử lý lại phần giữa lần commit cuối và lúc crash.

## 8.4 `auto.offset.reset`

| Giá trị | Khi group mới / offset đã bị xoá |
|---|---|
| `earliest` | Đọc từ đầu topic |
| `latest` | Bỏ lịch sử, chỉ đọc mới |
| `none` | Ném `NoOffsetForPartitionException` (buộc dev xử lý tường minh — dùng khi bắt đầu sai vị trí là nghiêm trọng) |

## 8.5 Hai đồng hồ liveness — hay bị nhầm

| Tham số | Đo cái gì | Hết hạn thì |
|---|---|---|
| `session.timeout.ms` | **Heartbeat** từ thread nền | Coordinator coi consumer chết → rebalance |
| `heartbeat.interval.ms` | Nhịp gửi heartbeat (phải < `session.timeout/3`) | — |
| `max.poll.interval.ms` (mặc định 5 phút) | Khoảng cách tối đa giữa **2 lần `poll()`** (xử lý quá chậm) | Bị đá khỏi group → rebalance |

**Vòng luẩn quẩn (rebalance storm phía consumer):** xử lý > `max.poll.interval.ms` → bị đá → rebalance → nhận lại partition → lại quá chậm → lại rebalance. **Sửa:** tăng `max.poll.interval.ms` hoặc giảm `max.poll.records`, đẩy việc chậm ra khỏi luồng poll.

Các cấu hình fetch: `fetch.min.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes`, `max.poll.records`.

## 8.6 `subscribe()` vs `assign()`

| | `subscribe(topics)` | `assign(partitions)` |
|---|---|---|
| Ai chia partition | Group coordinator, động | Tự chỉ định tay |
| Rebalance | Có | Không |
| Dùng cho | Consumer production | Job replay một lần, debug |

## 8.7 Consumer Group

- **Một partition tại một thời điểm chỉ thuộc đúng 1 consumer trong group.** Đây là đảm bảo chống xử lý trùng trong group.
- **Nhiều group đọc độc lập cùng topic** (mỗi group có offset riêng) → fan-out.
- **Trần song song = số partition:**

```
6 partition, 3 consumer → mỗi consumer 2 partition   (tối ưu)
6 partition, 6 consumer → mỗi consumer 1 partition   (tối đa)
6 partition, 8 consumer → 2 consumer NGỒI KHÔNG
```
Muốn scale hơn → tăng partition (xem 6.5).

- Spring: `concurrency` × số instance ≤ số partition.

## 8.8 Rebalance

**Kích hoạt khi:** consumer vào/ra (chủ động hoặc timeout), số partition đổi, subscription đổi.

| Giao thức | Hành vi |
|---|---|
| **Eager (cũ, stop-the-world)** | **Mọi** consumer thu hồi **mọi** partition, dừng xử lý → gán lại → chạy tiếp |
| **Cooperative (khuyến nghị)** | Chỉ thu hồi partition **thực sự cần chuyển**; consumer không bị ảnh hưởng cứ chạy tiếp |

**Các assignor:** `RangeAssignor` (theo topic, dễ lệch), `RoundRobinAssignor` (trải đều), `StickyAssignor` (giữ nguyên gán cũ tối đa), **`CooperativeStickyAssignor`** (sticky + incremental).

**Quy trình:** consumer gửi `JoinGroup` → Coordinator chờ (`rebalance.timeout.ms`) → chọn **Group Leader** (thành viên đầu) → Leader chạy assignor → gửi kết quả qua `SyncGroup` → Coordinator phát lại → consumer nhận partition.

**Group Coordinator:** broker được chọn bằng `hash(group.id) % số partition của __consumer_offsets`. Việc: nhận `JoinGroup`, kích hoạt rebalance, lưu offset.
Trạng thái group: `Empty → PreparingRebalance → CompletingRebalance → Stable → Dead`.

**Static membership (`group.instance.id`):** consumer có danh tính cố định; restart trong `session.timeout.ms` thì **lấy lại đúng partition cũ, không rebalance**. Rất hợp Kubernetes (pod restart thường xuyên) và consumer có state (Kafka Streams).

## 8.9 `__consumer_offsets`

- Topic nội bộ, **compacted**, mặc định **50 partition**.
- Key = `(groupId, topic, partition)`; Value = offset + metadata + timestamp.
- Partition đích của một group = `hash(groupId) % 50`.

## 8.10 Đặt lại offset để replay

```bash
# NHỚ dừng consumer của group trước
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group order-service --topic orders \
  --reset-offsets --to-earliest --execute
# hoặc: --to-offset 5000 | --to-datetime 2025-01-01T00:00:00.000 | --shift-by -100
```
Các cách replay khác: `consumer.seek()` / `offsetsForTimes()`, tạo **consumer group mới** từ earliest (không ảnh hưởng group cũ), mirror sang topic replay.

## 8.11 Spring Boot: mẫu tối thiểu

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      properties: { enable.idempotence: true }
    consumer:
      group-id: order-processor-group
      auto-offset-reset: earliest
      enable-auto-commit: false
    listener:
      ack-mode: manual_immediate
```

```java
// Producer: key = orderId → cùng đơn hàng luôn vào cùng partition
kafkaTemplate.send("order-events", orderId, payload)
    .whenComplete((res, ex) -> { if (ex != null) /* retry / DLQ / alert */ ; });

// Consumer: ack SAU khi xử lý thành công
@KafkaListener(topics = "order-events", groupId = "order-processor-group")
public void handle(ConsumerRecord<String, String> rec, Acknowledgment ack) {
    process(rec.value());
    ack.acknowledge();
}
```

> **Chuyển tiếp:** Chạy trên giấy thì đẹp. Trong thực tế consumer hay hỏng ở đâu? → Phần 9.

---

# PHẦN 9 — Khi consumer hỏng: Lag, Poison message, DLQ

## 9.1 Consumer lag

```
Partition Lag = Log End Offset (LEO) − Committed Offset
```
Lag tăng đều = consumer không theo kịp producer. **Đây là metric consumer quan trọng nhất.**

## 9.2 Bảng chẩn đoán

| Tín hiệu | Nguyên nhân | Xử lý |
|---|---|---|
| **Lag lệch, chỉ 1 partition tăng** | **Poison message** chặn consumer của partition đó | Đẩy sang DLQ + cho offset đi tiếp |
| **Mọi partition đều tăng** | Nút thắt xử lý (DB/API chậm) | Thêm consumer (tới số partition), batch DB (`saveAll`) |
| **Lag tăng + rebalance liên tục** | Xử lý vượt `max.poll.interval.ms` | Giảm `max.poll.records`, dùng `CooperativeStickyAssignor` |
| **Một partition lag nhưng producer cũng lệch** | Hot key | Xem 6.3 |

## 9.3 Poison message và DLQ

**Poison message:** record hỏng/không parse được → consumer ném exception → **không commit** → poll lại đúng record đó → lặp vô tận → **offset partition đó đứng yên, lag tăng mãi**, các partition khác vẫn chạy.

**Mẫu chuẩn (Spring):** retry có giới hạn + backoff → hết retry thì **Dead Letter Topic** (`<topic>.DLT`) → **đánh dấu không retry** cho lỗi chắc chắn không thể thành công (`JsonParseException`, `IllegalArgumentException`).

```java
DefaultErrorHandler handler = new DefaultErrorHandler(
    new DeadLetterPublishingRecoverer(template,
        (r, e) -> new TopicPartition(r.topic() + ".DLT", r.partition())),
    new ExponentialBackOffWithMaxRetries(3));
handler.addNotRetryableExceptions(JsonParseException.class, IllegalArgumentException.class);
```
Phân biệt lỗi **transient** (retry), **poison** (DLQ), **contract** (schema sai). DLQ phải có **chủ sở hữu + SLA**, đừng để nó phình thầm lặng.

## 9.4 Retry có thể phá thứ tự!

```
P0: [A] lỗi, retry sau
P0: [B] thành công
P0: [A retry] → xử lý SAU B → sai thứ tự
```

Ba cách xử (chọn theo mức độ cần thứ tự):
1. **Chặn partition khi lỗi** (không ack → không nhận message mới; dùng `BackOff`) — chậm nhưng an toàn.
2. **Pause partition**: `consumer.pause(...)` → xử lý DLT/sửa nguyên nhân → `consumer.resume(...)`.
3. **DLT + replay thủ công** có kiểm soát thứ tự.

## 9.5 Lỗi consumer hay gặp

| Bẫy | Triệu chứng | Sửa |
|---|---|---|
| Xử lý chậm vượt `max.poll.interval.ms` | Rebalance vô tận | Tăng interval / giảm `max.poll.records` |
| `enable.auto.commit=true` + xử lý thủ công | Trùng hoặc mất khi crash | Tắt auto-commit, ack thủ công |
| Quên `consumer.close()` khi shutdown | Rebalance chậm (chờ `session.timeout.ms`) | Shutdown hook + `wakeup()` |
| Chặn trong vòng poll | Heartbeat timeout, bị đá | Đưa việc chặn ra khỏi luồng poll |

> **Chuyển tiếp:** Muốn xử lý nhanh hơn mà không phá thứ tự, và không tăng partition? → Phần 10.

---

# PHẦN 10 — Xử lý nhanh hơn mà vẫn đúng thứ tự

## 10.1 Mâu thuẫn cốt lõi

- Mặc định: **1 thread / 1 partition**. Xử lý 100ms/message → trần **10 msg/s/partition**.
- Tăng tốc bằng đa luồng **ngây thơ** → phá thứ tự.
- Thêm partition để scale có cái giá thật: file handle, metadata, rebalance lâu, **không thể giảm lại**, tail latency tăng.

**Ví dụ quầy giao dịch ngân hàng:** thuê 3 giao dịch viên cho 1 hàng khách → giao dịch của cùng một tài khoản có thể xử lý chéo nhau. Sửa: **định tuyến theo số tài khoản** (cùng key → cùng người).

## 10.2 Công thức throughput

```
Throughput (msg/s) ≈ số luồng đồng thời / độ trễ xử lý (giây)
```
Ví dụ I/O-bound (API call 50ms):

| Cách | Luồng | ~msg/s |
|---|---|---|
| Consumer chuẩn, 6 partition | 6 | ~120 |
| `concurrency=18` | 18 | ~360 |
| Virtual thread dispatch | 150+ | ~3.000 |
| Reactor `flatMap` | event loop | ~3.000+ |

- **I/O-bound** (HTTP, DB): virtual thread/reactive rất hợp.
- **CPU-bound**: chỉ giới hạn ≈ số core, virtual thread không giúp gì.

## 10.3 Bài toán "commit contiguous offset"

Kafka commit offset `N` ngầm nói "mọi thứ < N đã xong". Khi chạy song song, record hoàn thành **không theo thứ tự**:

```
offset 100 xong | 101 ĐANG CHẠY | 102 xong | 103 xong
→ Chỉ được commit tới 100. 102, 103 dù xong cũng chưa commit được.
```
Commit sớm rồi crash = **mất 101**. Giải pháp: chỉ commit **offset liền mạch cao nhất đã hoàn thành** (high-water mark), hoặc lưu **bitmap hoàn thành** vào metadata của commit (cách Confluent Parallel Consumer làm).

**Bẫy DIY:** tự `CompletableFuture.runAsync()` trong `@KafkaListener` rồi `ack.acknowledge()` trong từng task → (1) race khi commit, có thể mất offset 101; (2) không giữ thứ tự theo key; (3) không có backpressure → OOM.

## 10.4 Các lựa chọn (từ đơn giản đến phức tạp)

| Tình huống | Chọn |
|---|---|
| Nút thắt là ghi DB từng dòng, cần thứ tự | **Batch processing** (bulk insert) — đơn giản, không đa luồng |
| Không cần thứ tự | Tăng partition + consumer |
| Cần thứ tự **theo key** + throughput cao, Java 21 / Spring Boot 3.2+ | **Batch listener + virtual thread + `Striped<Semaphore>` khoá theo key**; commit cả batch sau khi `allOf()` xong |
| Đã dùng reactive (WebFlux) | Reactor: `groupBy(key)` + `concatMap` (tuần tự trong key) + `flatMap` (song song giữa key) |
| Stateful (join, aggregate, window) | **Kafka Streams** |
| Một topic khổng lồ | **Router topic (fan-out)**: consumer nhanh tách theo key sang các sub-topic |
| Một message xấu chặn cả partition | Pause/resume partition |
| Tương lai | **Share Groups (KIP-932)**: nhiều consumer đọc **cùng một partition**, ack từng record (còn sớm — chưa nên đưa vào production) |

**Confluent Parallel Consumer:** thư viện có 3 chế độ `UNORDERED` / `KEY` / `PARTITION` (KEY là khuyến nghị), nhưng **tài liệu gốc ghi là không còn được bảo trì** → chỉ giữ để hiểu kiến trúc (poller thread + work queue có backpressure + worker pool + offset manager bitmap). Với dự án mới dùng các lựa chọn trên.

**Cảnh báo virtual thread:** vòng poll của Spring Kafka dùng `synchronized` có thể **pin** virtual thread → để listener chạy trên platform thread, **chỉ đẩy phần xử lý** sang virtual thread.

## 10.5 Luôn luôn thêm idempotency

Dù chọn cách nào: crash, retry, rebalance đều có thể làm xử lý trùng → downstream phải **idempotent** (unique constraint, upsert, dedup check). Xem Phần 11.

> **Chuyển tiếp:** Vậy Kafka có thực sự "exactly-once" không? → Phần 11.

---

# PHẦN 11 — Giao đúng 1 lần: Delivery semantics, EOS, Dedup

## 11.1 Vì sao trùng lặp là điều không tránh khỏi

> **Một câu tóm tắt:** hệ thống đáng tin nào cũng phải retry; đã retry thì có thể giao > 1 lần; giao > 1 lần thì phải xử lý trùng — nếu không sẽ làm hỏng dữ liệu nghiệp vụ.

Producer không nhận được ACK sẽ **không phân biệt được**: (A) message đã tới nhưng ACK mất, hay (B) message chưa tới. Muốn không mất → buộc phải retry → có thể trùng. Đây là gốc rễ, đến từ chính mạng.

Nguồn trùng lặp thực tế: producer retry; **consumer rebalance** (xử lý xong, crash trước commit); at-least-once redelivery; **DLQ redrive** (replay sau nhiều ngày); client/upstream tự retry.

## 11.2 Ba mức đảm bảo

| Mức | Nghĩa | Rủi ro | Ví dụ |
|---|---|---|---|
| **At-most-once** | 0 hoặc 1 lần | **Mất thầm lặng** | `acks=0`, commit trước xử lý |
| **At-least-once** | ≥ 1 lần | **Trùng** (không mất) | Mặc định của consumer; `acks=all` + retry |
| **Exactly-once** | *Hiệu ứng* xảy ra đúng 1 lần | Cần phối hợp producer–broker–consumer | Kafka EOS (chỉ trong ranh giới Kafka) |

**Sự thật cần thuộc:** exactly-once *delivery* qua mạng bất kỳ là **bất khả thi** (bài toán Two Generals). Cái Kafka gọi là EOS thực chất = **at-least-once delivery + xử lý idempotent/transactional** để bản trùng bị hấp thụ, không đổi kết quả.

## 11.3 Ba trụ cột EOS trong Kafka

1. **Idempotent Producer** — chặn trùng do retry (trong 1 phiên).
2. **Transactional Producer** (`transactional.id` + 2PC) — ghi nguyên tử nhiều partition + gắn offset input (`sendOffsetsToTransaction`).
3. **Transactional Consumer** (`isolation.level=read_committed`) — chỉ thấy record của transaction đã commit, bị chặn bởi **LSO**.

Chi tiết cơ chế đã ở Mục 7.7 – 7.8.

### Kafka Streams EOS (`EXACTLY_ONCE_V2`)
- Bật bằng `processing.guarantee=exactly_once_v2`; không cần gọi begin/commit thủ công.
- Mỗi chu kỳ commit là **một transaction** bao trọn: offset input đã đọc + mọi record output + **ghi changelog của state store**.
- `commit.interval.ms` mặc định **30s (at-least-once) / 100ms (EOS)**. Giá trị nhỏ → độ trễ end-to-end thấp cho consumer downstream nhưng thêm chi phí coordinator.
- Đánh đổi: giảm throughput khoảng vài % tới ~20–30% (tuỳ tài liệu) so với at-least-once.
- V2 dùng **1 producer transactional cho mỗi stream thread** (V1: mỗi task) → ít producer hơn, throughput tốt hơn; cần Kafka 2.5+.
- Hiện tượng lạ nhưng bình thường: lag luôn = 1 dù đã bắt kịp, vì **transaction marker chiếm 1 offset** → loại khỏi cảnh báo lag.

### Các kịch bản lỗi
| Tình huống | Điều gì xảy ra |
|---|---|
| Producer crash giữa transaction | Transaction ở `Ongoing`; sau `transaction.timeout.ms` coordinator tự **abort**. Trong lúc chờ, consumer `read_committed` có thể bị kẹt ở LSO |
| Coordinator broker chết | Replica của `__transaction_state` lên thay, tiếp tục từ trạng thái đã bền (`PrepareCommit` ghi trước khi sang pha 2 nên an toàn). `transaction.state.log.replication.factor` ≥ 3 |
| `send()` bị retry do mất ACK | Idempotence (luôn bật khi có `transactional.id`) loại bản trùng, ứng dụng không thấy khác biệt |
| Consumer đọc khi transaction chưa xong | Bị giữ ở LSO — bình thường, không phải lỗi |

## 11.4 Ranh giới của EOS

**EOS chỉ đúng trong ranh giới Kafka** (Kafka → Kafka). Ghi DB, gọi Stripe, gửi email **không thể tham gia transaction của Kafka** → lúc đó cần **dedup ở tầng ứng dụng**.

> **Câu trả lời cấp senior:** "EOS đảm bảo ghi output + cập nhật state store + commit offset là nguyên tử **trong Kafka**. Bước ra ngoài (DB, API, email) là hết đảm bảo, phải dùng idempotency key + unique constraint hoặc Redis `SETNX`."

## 11.5 Idempotent consumer — mẫu nền tảng

```java
@KafkaListener(topics = "orders")
@Transactional
public void consume(OrderEvent e) {
    try {
        processedEventRepo.save(new ProcessedEvent(e.getId()));   // PRIMARY KEY / UNIQUE
    } catch (DataIntegrityViolationException dup) {
        return;                                                    // trùng → bỏ qua, offset vẫn tiến
    }
    orderService.apply(e);                                         // chỉ "người thắng INSERT" mới tới đây
}
```

**Bug phổ biến nhất — TOCTOU** (check-then-act không nguyên tử): hai luồng cùng thấy "chưa có" rồi cùng xử lý → **charge 2 lần**.
- ❌ `if (!exists(id)) { charge(); save(id); }`
- ✅ **Ghi bản ghi dedup TRƯỚC**, để unique constraint làm người gác.
- ✅ Redis: dùng **một lệnh nguyên tử** `setIfAbsent` (SETNX + TTL), không phải `hasKey` rồi `set`.

## 11.6 Dedup state nên nằm ở đâu?

1. **Chính database bạn đang ghi** (`UNIQUE` + `ON CONFLICT DO NOTHING`) — đơn giản nhất, nguyên tử; chỉ dùng khi side-effect *là* ghi vào DB đó.
2. **Kafka Streams State Store (RocksDB)** — khi pipeline **thuần Kafka → Kafka**.
3. **Redis** — khi side-effect là **hệ ngoài** (REST, email, DB dịch vụ khác) hoặc nhiều service cần chung trạng thái dedup.

### State Store vs Redis

| Tiêu chí | State Store | Redis |
|---|---|---|
| Nhất quán với offset | ✅ **Nguyên tử** (cùng transaction) | ❌ **Dual-write** (2 thao tác tách rời) |
| Độ trễ lookup | Dưới ms (local) | ~1–5ms (mạng) |
| Hạ tầng | Không cần thêm | Cần cụm Redis |
| Dedup chéo instance/service | ❌ Chỉ trong partition | ✅ Dùng chung |
| Restart/rebalance | ❌ Phải rebuild state | ✅ Tức thì |
| TTL | Tự làm | ✅ Có sẵn theo key |
| Cửa sổ dài (30 ngày+) | ❌ Nặng đĩa local | ✅ Hợp |
| Dedup cho hệ ngoài | ❌ | ✅ |
| Bộ nhớ | Trên đĩa | Toàn RAM (đắt) |

**Vì sao Redis có "dual-write hazard":** `SETNX` thành công → consumer crash **trước khi ack** → Kafka giao lại → Redis thấy key đã tồn tại → **bỏ nhầm message chưa từng xử lý**. Giảm nhẹ bằng trạng thái `PROCESSING → COMPLETED` thay vì boolean, nhưng không đạt độ nguyên tử như transaction Kafka.

**Kiến trúc nhiều lớp (production tài chính):** (1) **business idempotency key** nằm trong payload → (2) **Redis SETNX** làm cổng nhanh → (3) **DB unique constraint** làm chốt chặn cuối. Có thể thêm Streams State Store để lọc trùng ngay trong Kafka.

## 11.7 Chọn cửa sổ dedup

`cửa sổ ≥ độ trễ redelivery hợp lý tối đa` (thời gian rebalance/restart, thời gian rebuild state, **thời gian giữ DLQ trước khi ai đó redrive**, replay thủ công khi sự cố).
**Lỗi hay gặp:** TTL 24h nhưng DLQ redrive sau 26h → key hết hạn → **xử lý trùng**. Cách xử: TTL > thời gian giữ DLQ, luôn có **DB unique constraint** làm lớp cuối, và kiểm tra trước khi redrive.

## 11.8 Thiết kế idempotency key

- ✅ Ổn định qua retry; duy nhất theo **thao tác nghiệp vụ** (không theo lần giao); có ngữ cảnh domain; **không đổi khi redrive**.
- ❌ Chỉ dùng Kafka offset (đổi khi redrive), chỉ SQS MessageId, UUID sinh lúc gửi (retry ra UUID mới → không dedup), chỉ timestamp.
- Ví dụ: `"order-placed-" + orderId`, `"payment-" + paymentTransactionId`, header `Idempotency-Key: charge-<orderId>-attempt-1` cho API ngoài.

**RabbitMQ/SQS:** không có dedup broker-level thật sự. SQS FIFO chỉ có cửa sổ 5 phút theo `MessageDeduplicationId` → DLQ redrive nằm ngoài cửa sổ.

## 11.9 Outbox pattern (bài toán dual-write DB + Kafka)

Ghi DB và gửi Kafka là 2 thao tác độc lập → có thể lệch. **Giải pháp:** ghi **cả dữ liệu nghiệp vụ và bản ghi `outbox` trong cùng 1 transaction DB**; một relay (thường **Debezium CDC**) đọc outbox rồi publish lên Kafka (at-least-once); consumer dedup bằng idempotency key. Chuỗi hoàn chỉnh: **Kafka EOS + ACID local + CDC + idempotent consumer ⇒ hiệu ứng exactly-once end-to-end.**

> **Chuyển tiếp:** Nhìn lại topic: có loại topic không "quên" mà giữ bản mới nhất của mỗi key → Phần 12.

---

# PHẦN 12 — Log Compaction: giữ trạng thái mới nhất

## 12.1 Khái niệm

Thay vì xoá theo thời gian, `cleanup.policy=compact` giữ **ít nhất giá trị mới nhất của mỗi key** trong partition → topic gần như một **bảng trạng thái hiện tại**.

- **Offset không bị đánh số lại.**
- **Active segment không bao giờ bị compact.**
- **Chỉ hoạt động với message có key.** Không key → không bao giờ compact được.
- Compaction **không tức thì**: cleaner thread chạy nền khi tỷ lệ dirty vượt `min.cleanable.dirty.ratio` (mặc định 0.5).

## 12.2 Cách hoạt động

Log chia hai vùng: **Clean** (đã compact, mỗi key 1 giá trị) và **Dirty** (còn trùng key). Cleaner thread dựng hash map "offset mới nhất của mỗi key" rồi ghi lại các segment chỉ chứa bản mới nhất.

## 12.3 Tombstone

- Message có **key + value = null** = tín hiệu **xoá key**.
- Được giữ thêm `delete.retention.ms` (mặc định **24h**) để consumer đang chậm kịp thấy lệnh xoá trước khi bị dọn.

## 12.4 Cấu hình chính

| Config | Mặc định | Ý nghĩa |
|---|---|---|
| `cleanup.policy` | `delete` | `compact` hoặc `compact,delete` |
| `min.cleanable.dirty.ratio` | 0.5 | Ngưỡng dirty để bắt đầu compact |
| `min.compaction.lag.ms` | 0 | Thời gian tối thiểu message chưa bị compact |
| `max.compaction.lag.ms` | vô hạn | Trễ tối đa trước khi bắt buộc compact |
| `delete.retention.ms` | 1 ngày | Giữ tombstone |
| `segment.ms` / `segment.bytes` | 7 ngày / 1GB | Độ mịn compaction |

## 12.5 `delete` vs `compact`

| | delete | compact |
|---|---|---|
| Giữ | Mọi message trong cửa sổ retention | Giá trị mới nhất mỗi key |
| Hợp với | Event stream, audit, clickstream | State store, CDC changelog, config topic |
| Tombstone | Không | Có |
| Dung lượng | Bị chặn | Không chặn (trừ khi `compact,delete`) |

## 12.6 Dùng ở đâu

- **CDC (Debezium):** topic luôn chứa trạng thái hiện tại của mỗi row.
- **Phân phối config/reference data.**
- **Kafka Streams:** state store được backup bằng **changelog topic compacted**; **KTable** về ngữ nghĩa = topic compacted.
- **Nội bộ Kafka:** `__consumer_offsets`, `__transaction_state`, `__connect-*`.

## 12.7 Kết hợp với Tiered Storage (Kafka 3.6+)

Compaction chỉ chạy trên **segment local**. Segment đã đẩy lên object storage (S3/GCS…) **không còn được compact** → giữ mọi phiên bản cũ. Cách xử: đặt `local.retention.ms` đủ lớn để compact xong rồi mới tier; topic state-store nên compact-only hoặc giữ local rất lâu.

## 12.8 Sự cố thường gặp

| Vấn đề | Nguyên nhân/cách sửa |
|---|---|
| Không bao giờ compact | Message không có key |
| Key biến mất bất ngờ | Có tombstone (value null) ngoài ý muốn |
| Compaction chậm | Tăng `log.cleaner.threads`, giảm `min.cleanable.dirty.ratio` |

> **Chuyển tiếp:** Dữ liệu chảy qua Kafka phải có "hợp đồng" hình dạng → Phần 13.

---

# PHẦN 13 — Schema Registry: hợp đồng dữ liệu

> ⚠️ File gốc về Schema Registry trong repo khá ngắn/thiếu nội dung; phần dưới có **[bổ sung]** là kiến thức chuẩn ngoài repo.

## 13.1 Vấn đề & vai trò

Không có schema governance, **schema drift** vô hình cho tới khi vỡ production. **Schema Registry** = kho tập trung để **lưu, đánh phiên bản, kiểm tra tương thích** schema cho message Kafka.

## 13.2 Cơ chế

- Message ghi ra có **magic byte `0x00`** ở đầu để phân biệt với byte thô; consumer nhận message không có magic byte sẽ ném `SerializationException`.
- **[bổ sung]** Sau magic byte là **4 byte schema ID**, rồi mới tới payload. Schema **không nhúng vào mỗi message**, chỉ ID.
- Confluent Schema Registry lưu schema trong một **topic Kafka** (`_schemas`).

## 13.3 Quy tắc tiến hoá schema

| Tình huống | Khuyến nghị |
|---|---|
| Dự án mới | **Avro + `FULL_TRANSITIVE`**, đăng ký schema qua CI/CD |
| Nhiều ngôn ngữ (Java/Python/Go) | **Protobuf** (sinh code đa ngôn ngữ tốt hơn) |
| Cần đọc được bằng mắt | JSON Schema (kém gọn) |
| Thêm field | Luôn **optional** (`["null","type"]`) + `default: null` |
| Đổi tên field | Dùng `aliases` hoặc topic mới; **không đổi tên trực tiếp** |
| Thay đổi phá vỡ | **Topic mới + giai đoạn chuyển tiếp**; không sửa phá schema topic cũ |
| Reset topic + schema mới | Tạm đặt compatibility `NONE`, xoá version cũ sau khi drain, rồi khôi phục |
| `auto.register.schemas` | **`false`** ở staging/prod; `true` chỉ cho dev local |
| HA | ≥ 3 instance dùng chung topic `_schemas` + load balancer |

**Loại bỏ field:** deprecate trước (ngừng produce), chỉ xoá khi mọi consumer đã cập nhật.

## 13.4 Vận hành

Schema Registry **chết = mọi producer/consumer Avro hỏng** → cảnh báo `up == 0`, theo dõi số subject, và cảnh báo "consumer fetch nhưng không consume" (dấu hiệu lỗi deserialize). MirrorMaker 2 **không replicate schema** — phải sao chép registry riêng.

---

# PHẦN 14 — Kafka Streams: xử lý luồng ngay trong ứng dụng

## 14.1 Bản chất

> **Kafka Streams là một engine xử lý luồng có trạng thái, chịu lỗi, chạy nhúng trong tiến trình ứng dụng của bạn.**

- Không có cụm riêng (không Spark master, không Flink JobManager). Import thư viện, viết topology → ứng dụng của bạn *trở thành* stream processor. Deploy như microservice thường.
- **Công thức tư duy:**
```
Kafka Streams = Kafka topic (nguồn sự thật)
              + RocksDB (state cục bộ, truy cập nhanh)
              + Topology (đồ thị xử lý)
              = Event Sourcing + CQRS + Materialized View, nhúng trong app
```
- Scale theo partition; failover dựa trên consumer group protocol.

## 14.2 Ba abstraction

| | Ý nghĩa | Ví dụ | Dùng cho |
|---|---|---|---|
| **KStream** | Chuỗi sự kiện **độc lập**, vô hạn, append-only. Cùng key = các sự kiện riêng biệt | order placed, click | Từng sự kiện có ý nghĩa riêng |
| **KTable** | **Changelog** → view "trạng thái mới nhất" theo key. Bản ghi mới **thay thế** bản cũ | số dư tài khoản, profile | Trạng thái entity |
| **GlobalKTable** | KTable **nhân bản đầy đủ tới MỌI instance** | danh mục sản phẩm, mã quốc gia | Reference data nhỏ, ít đổi; join **không cần co-partitioning** |

Chọn GlobalKTable khi: dữ liệu nhỏ (< ~1GB/instance), ít thay đổi, stream không co-partitioned. Bảng lớn/ghi nhiều → dùng KTable + đảm bảo co-partition.

## 14.3 Topology và bẫy đặt tên (rất quan trọng khi deploy)

Tên tự sinh kiểu `KSTREAM-FILTER-0000000002` được dùng làm **tên internal topic, thư mục RocksDB, tên changelog**. Thêm/bớt/đổi thứ tự một operator → các tên phía sau **dịch chuyển** → changelog mới, **rebuild toàn bộ state** (phút tới giờ), topic cũ mồ côi; rolling deploy có thể gây **rebalance vô tận** vì task map v1/v2 không khớp.

✅ **Luôn đặt tên tường minh:** `Named.as(...)`, `Grouped.as(...)`, `Materialized.as(...)`.

**Sub-topology reordering:** sub-topology được đánh số theo **thứ tự khai báo**. Đổi thứ tự → `TaskId(0_0)` đổi từ đọc Topic A sang Topic B → instance v1 và v2 hiểu khác nhau → từ chối/tranh nhau → bão rebalance, **không xử lý được gì**. Phòng: blue-green deploy, kiểm tra topology ổn định trước khi rolling.

## 14.4 Mô hình thực thi

- **Task = 1 partition nguồn.** Task độc lập: có offset riêng, **state store riêng (thư mục RocksDB riêng)**, buffer riêng.
- **Trần song song = số partition nguồn.** Thêm instance thứ 4 khi chỉ có 3 partition → nó ngồi không.
- **Stream thread** (`num.stream.threads`): mỗi thread quản lý vài task, chạy event loop riêng, không chia sẻ mutable state.
- **Vòng xử lý:** `poll` → xử lý qua topology (bản ghi output vào buffer producer) → tới `commit.interval.ms`: flush cache → RocksDB → flush đĩa → gửi output → commit offset (nguyên tử với EOS v2).

```java
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-app");   // = group id = prefix mọi internal topic
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
props.put(StreamsConfig.STATE_DIR_CONFIG, "/var/kafka-streams/state");   // NVMe
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
```
> Đổi `application.id` = tạo một ứng dụng **hoàn toàn mới** (offset mới).

## 14.5 Thao tác

- **Stateless:** `filter`, `map`, `mapValues`, `flatMapValues`, `peek`, `branch/split`.
- **Stateful:** `groupByKey`/`groupBy` + `count`/`aggregate`/`reduce`, windowing, join.

### Repartitioning — chi phí ẩn
Bất kỳ thao tác **đổi key** (`map`, `selectKey`, `groupBy`, join chưa co-partition) → Streams ghi lại qua **repartition topic** để mọi bản ghi cùng key về cùng task.
Chi phí: thêm 1 topic, thêm 1 lần ghi + đọc Kafka mỗi record, thêm độ trễ (~5–50ms).
**Giảm thiểu:** đặt key đúng từ nguồn; dùng `mapValues` thay `map`; dùng `groupByKey` thay `groupBy` khi key đã đúng; thiết kế input join đã co-partition.

## 14.6 State Store và Changelog

- **State store** = database key-value cục bộ. Mặc định **RocksDB** (LSM-tree): dữ liệu có thể **vượt RAM**, hỗ trợ range query (cho window), crash-safe nhờ WAL, tinh chỉnh block cache/nén.
- Loại: `KeyValueStore`, `WindowStore`, `SessionStore`, và in-memory store.
- **Changelog topic:** mỗi state store bền vững có 1 topic **compacted** ghi mọi thay đổi → **nguồn sự thật để khôi phục**; đĩa local chỉ là **cache**. Kích thước changelog bị chặn bởi số key duy nhất.
- Tắt changelog (`withLoggingDisabled`) = chuyển task sang instance khác là **mất toàn bộ state** — chỉ dùng cho state tái tạo được.

## 14.7 Khôi phục sự cố

- Instance chết → coordinator phát hiện qua heartbeat (`session.timeout.ms`) → rebalance → task chuyển sang instance khỏe → **nạp lại state từ changelog**.
- **Checkpoint file (`.checkpoint`)** ghi offset changelog mà RocksDB đã chắc chắn bền → chỉ phải replay **phần sau checkpoint**, không phải từ đầu.
- **Thời gian khôi phục ≈ kích thước state / tốc độ replay** (100GB state không standby có thể 15–30 phút). ⇒ **Kích thước state là mối quan tâm thiết kế hạng nhất.** Đòn bẩy: windowing, TTL, aggregate chỉ field cần thiết.
- **Standby replicas (`num.standby.replicas`)**: task "bóng" liên tục đọc changelog (không xử lý input) → failover **trong vài giây**. Đổi lại tốn thêm instance/bộ nhớ.

| | Không standby | 1 standby | 2 standby |
|---|---|---|---|
| Khôi phục | Phút (replay đầy đủ) | Giây | Gần như 0 |
| Chi phí | N | ~2N | ~3N |
| Dùng khi | State nhỏ | Production | Yêu cầu RTO cực thấp |

Cũng giảm rebalance: **static membership** + standby.

## 14.8 Windowing

Chia luồng vô hạn thành các khung thời gian hữu hạn để aggregate.

| Loại | Đặc điểm |
|---|---|
| **Tumbling** | Cố định, **không chồng**; mỗi record thuộc đúng 1 cửa sổ |
| **Hopping** | Cố định, **chồng lên nhau** (size 1h, hop 15 phút); 1 record thuộc nhiều cửa sổ |
| **Session** | Theo **hoạt động**, độ dài thay đổi; cửa sổ mới khi im lặng vượt `inactivityGap` |

- Mặc định phát kết quả **mỗi lần aggregate đổi** (nhiều kết quả trung gian). `suppress(untilWindowCloses)` giữ tới khi cửa sổ **đóng** rồi phát đúng 1 lần (tốn RAM buffer; đầy thì `shutDownWhenFull` hoặc `emitEarlyWhenFull`).
- **Grace period** cho phép event đến muộn (`TimeWindows.ofSizeAndGrace`).

## 14.9 Join

| Loại join | Cần co-partition? |
|---|---|
| KStream–KStream (**windowed**) | ✅ |
| KStream–KTable | ✅ |
| KTable–KTable | ✅ |
| KStream–**GlobalKTable** | ❌ (dùng key extractor) |

**Co-partition = cùng số partition + cùng logic partition (cùng key, cùng partitioner).**

## 14.10 Interactive Queries

Truy vấn trực tiếp state store cục bộ (không cần DB ngoài); khi state phân tán nhiều instance thì phải **hỏi đúng instance đang giữ key** (hoặc gom từ tất cả).

## 14.11 Exactly-once ra hệ ngoài

`EXACTLY_ONCE_V2` chỉ bao Kafka. Ghi DB ngoài → dùng **Transactional Outbox** (xem 11.9).

## 14.12 Khi nào dùng / không dùng

**Dùng khi:** cần stateful (aggregate/join/window), state vừa đĩa local (RocksDB scale tới TB), topology ổn định, muốn vận hành đơn giản (không cụm riêng), đã có Kafka, cần EOS trong Kafka, cần state có thể query.

**Không dùng khi:** phải join với DB ngoài lớn/đổi liên tục; topology thay đổi runtime; cần SQL ad-hoc (→ ksqlDB/Flink); state khổng lồ mỗi partition; cần xử lý **batch lịch sử** (→ Spark/Flink); cần độ chính xác event-time dưới ms trên nhiều producer.

| | Kafka Streams | Flink | Spark SS | ksqlDB |
|---|---|---|---|---|
| Triển khai | **Thư viện nhúng** | Cụm riêng | Cụm riêng | Server riêng |
| Vận hành | Tối thiểu | Cao | Cao | Trung bình |
| SQL | ❌ | ✅ | ✅ | ✅ (SQL-first) |
| Topology động | ❌ | ✅ | ✅ | ❌ |

## 14.13 Ma trận sự cố

| Tình huống | Hệ quả | Giảm thiểu |
|---|---|---|
| Instance crash | Rebalance + rebuild state | Standby replicas |
| Rebalance (instance mới) | Tạm dừng | Static membership + standby |
| Cold restore state lớn | Phút → giờ | Windowing/TTL, NVMe |
| Topology đổi tên khi deploy | Rebuild + topic mồ côi | **Đặt tên tường minh** |
| Rolling deploy topology lệch | Rebalance vô tận | Blue-green |
| Zombie task | Ghi trùng | EOS v2 fence bằng epoch |
| Repartition topic phình | Đầy đĩa | Retention + giám sát |
| RocksDB compaction stall | Giật latency | Tune `rocksdb.config.setter`, NVMe riêng |

## 14.14 Bốn quy tắc vàng

1. **State size = recovery time** (giữ state bị chặn).
2. **Partition count = max parallelism.**
3. **Changelog = nguồn sự thật**, đĩa local chỉ là cache.
4. **Thiết kế cho lỗi, không phải cho thành công** — rebalance/restore là chuyện bình thường.

> *"Design your state before your topology."*

> **Chuyển tiếp:** Còn việc đưa dữ liệu vào/ra Kafka từ DB, S3, Elasticsearch… → Phần 15.

---

# PHẦN 15 — Tích hợp: Kafka Connect, SMT, MirrorMaker 2

## 15.1 Kafka Connect là gì

Framework để **di chuyển dữ liệu giữa Kafka và hệ ngoài một cách tin cậy mà không viết code riêng**.

| Khái niệm | Ý nghĩa |
|---|---|
| **Connector** | Plugin di chuyển dữ liệu |
| **Task** | Đơn vị công việc (1 connector nhiều task song song) |
| **Worker** | Tiến trình JVM chạy connector/task |
| **Standalone / Distributed** | 1 worker (dev) / nhiều worker, HA, chia tải |
| **Converter** | Serialize (JSON/Avro/Protobuf/String/ByteArray) |
| **SMT** | Biến đổi từng message ngay trong luồng |

## 15.2 Source vs Sink

| | **Source** (ngoài → Kafka) | **Sink** (Kafka → ngoài) |
|---|---|---|
| Client nội bộ | `KafkaProducer` | `KafkaConsumer` |
| Hàm chính | `poll()` trả `SourceRecord` | `put()` nhận `SinkRecord`, `flush()` định kỳ |
| Lưu offset | Topic compacted `connect-offsets` | `__consumer_offsets`, group `connect-<tên>` |
| Commit offset | **Sau khi broker ACK** | **Sau khi `flush()` thành công** |
| Giới hạn song song | Số "partition" của hệ nguồn (bảng/file/shard) | Số partition của topic |
| EOS | Có (KIP-618, Connect 3.3+) | Phụ thuộc hệ đích (idempotent/transactional) |
| DLQ | Không | Có |

- `tasks.max` lớn hơn giới hạn thì task thừa **ngồi không**.
- **Debezium (CDC) chỉ `tasks.max=1`:** đọc binlog/WAL là **một luồng tuần tự duy nhất**; nhiều task sẽ tranh nhau, trùng và sai thứ tự. CDC đọc log thay vì polling nên **bắt được cả DELETE**, độ trễ thấp, không tải DB.

## 15.3 Kiến trúc trong lòng Connect

- Worker trong cụm phối hợp qua **Group Membership Protocol** của Kafka (giống consumer group).
- **3 topic nội bộ (compacted):** `__connect-configs` (config connector/task), `__connect-offsets` (vị trí source), `__connect-status` (RUNNING/FAILED/PAUSED). Worker restart phải **replay cả 3** để dựng lại trạng thái → cụm phi trạng thái ngoài Kafka.
- **Plugin isolation:** `PluginClassLoader` nạp class **child-first** từ thư mục plugin, trừ các package chung (`org.apache.kafka.connect.*`, `java.*`, `org.slf4j.*`…). Mỗi plugin **một thư mục riêng** trong `plugin.path`; **không** nhét jar vào `CLASSPATH`.
- **Converter Avro:** schema không nhúng vào từng message mà qua Schema Registry.

### Rebalance của Connect (khác rebalance của consumer group!)
- **Eager:** mọi task dừng → tính lại toàn bộ → chạy lại. Thêm 1 worker cũng đóng băng cả cụm.
- **Incremental Cooperative (Kafka 2.3+, Connect 2.6+):** chỉ task cần di chuyển bị dừng. Bật bằng `connect.protocol=compatible`. **Chỉ có hiệu lực khi TẤT CẢ worker đã bật** — còn 1 worker eager thì cả cụm rơi về eager.

### Rebalance storm & rolling restart
**Storm:** worker restart nhanh hơn/nhanh dồn dập → mỗi lần một rebalance → connector lỗi lại kích rebalance nữa…
**Cách phòng:**
- `connect.protocol=compatible` trên mọi worker.
- `scheduled.rebalance.max.delay.ms` (ví dụ 300000) **> thời gian restart đo được + 20%** để cụm chờ worker quay lại thay vì chia lại task.
- **Restart từng worker một**, kiểm tra tất cả task `RUNNING` rồi mới sang worker kế.
- Giảm thời gian replay topic nội bộ (compact `__connect-configs`/`__connect-status`).
- Task treo khi dừng: giảm `task.shutdown.graceful.timeout.ms`, thêm query timeout cho JDBC.
- Đang bão: **pause mọi connector → sửa gốc → resume**.

### REST API nhanh
```bash
curl -X POST localhost:8083/connectors -H "Content-Type: application/json" -d '{...}'
curl localhost:8083/connectors/<name>/status
curl -X PUT  localhost:8083/connectors/<name>/pause     # / resume
curl -X POST "localhost:8083/connectors/<name>/restart?includeTasks=true&onlyFailed=true"
```

### Giám sát
Cảnh báo ngay khi task `FAILED`; lag sink > SLA; `source-record-poll-rate` về 0 (source kẹt); `put-batch-avg-time-ms` tăng vọt (hệ đích chậm); **DLQ tăng**.

## 15.4 SMT — Single Message Transform

- Biến đổi **từng message, không trạng thái**, chạy đồng bộ trong thread của task, cấu hình thẳng trong connector, **xếp chuỗi** được.
- Source: chạy **sau khi đọc nguồn, trước khi ghi Kafka**. Sink: chạy **sau khi đọc Kafka, trước khi ghi đích**.
- Built-in: `InsertField`, `ReplaceField`, `MaskField` (che PII), `ExtractField`, `Cast`, `Flatten`, `RegexRouter`/`TimestampRouter` (đổi tên topic)…
- **Predicate** (Connect 2.6+): áp SMT có điều kiện — `TopicNameMatches`, `HasHeaderKey`, `RecordIsTombstone`; `negate=true` để đảo điều kiện.
- **Dùng SMT** cho biến đổi đơn giản, stateless (mask, route, đổi kiểu, đổi tên). **Dùng Streams/Flink** khi cần aggregate, join, lookup ngoài, retry/DLQ phức tạp.
- Nguyên tắc: mỗi SMT một việc; **thứ tự quan trọng** (mask PII trước khi log/route); **không I/O ngoài**; giám sát `transformation-time-ms-avg`; SMT tự viết phải **thread-safe**.

## 15.5 Exactly-once với Connect

- **Source (Kafka 3.3+):** `exactly.once.source.support=enabled` → gửi record + ghi `connect-offsets` trong **một transaction**.
- **Sink:** tuỳ đích — nếu đích transactional (RDBMS) thì bọc `put()` trong transaction DB; nếu đích idempotent (Elasticsearch với document ID) thì retry an toàn.

## 15.6 MirrorMaker 2 — sao chép giữa cụm

- Dùng cho **DR**, tuân thủ dữ liệu, truy cập vùng gần, gom dữ liệu. Có từ Kafka 2.4 (KIP-382), **xây trên Kafka Connect**.
- **3 connector:** `MirrorSourceConnector` (sao chép dữ liệu, giữ key/value/header/timestamp), `MirrorCheckpointConnector` (đồng bộ **offset consumer group** giữa cụm), `MirrorHeartbeatConnector` (theo dõi sức khoẻ đường sao chép).
- **Đặt tên topic:** thêm tiền tố alias cụm nguồn (vd `us-west.orders`).
- **Offset translation:** offset ở cụm đích **khác** cụm nguồn (nguồn 1000 có thể là 998 ở đích) → Checkpoint connector giữ ánh xạ trong `*.checkpoints.internal` để consumer failover đọc đúng vị trí.
- **Active-active:** MM2 gắn header nguồn để **chống vòng lặp** (bỏ qua message vốn từ cụm đích). **Xung đột ghi** là việc của ứng dụng: timestamp-based, version, quyền vùng, CRDT.
- **Topologies:** Active-passive (DR), Active-active, Hub-and-spoke (gom về trung tâm), Fan-out (phân phối ra vùng).
- **Quy trình failover (active-passive):** phát hiện sự cố → kiểm tra cụm đích + lag sao chép → dừng producer ở cụm chính → chờ bắt kịp → **truy vấn checkpoint** lấy offset đã dịch → **reset consumer group** theo offset đó → chạy consumer ở đích → chuyển producer → xác minh dữ liệu.
- **Lưu ý vận hành:** config topic có thể lệch (`sync.topic.configs.enabled=true`); **Schema Registry không được MM2 sao chép**; chi phí egress liên vùng (dùng `zstd`, loại topic debug, đường truyền riêng).
- **MM1 vs MM2:** MM1 chỉ là cặp consumer–producer (không tự tạo topic, không đồng bộ offset/ACL). MM2 có tự tạo topic/partition, offset checkpoint, sao chép ACL/config, hai chiều có chống vòng, hỗ trợ EOS, khả năng chịu lỗi/scale của Connect.

---

# PHẦN 16 — Performance tuning: làm cho nhanh

## 16.1 Tư duy

Mặc định của Kafka **thận trọng** (ưu tiên đúng đắn/tương thích), tuning tốt có thể tăng throughput gấp nhiều lần. **Xác định nút thắt trước** (metric broker/producer/consumer), rồi mới chỉnh đúng tầng. Luôn **benchmark trước và sau** (`kafka-producer-perf-test.sh`, `kafka-consumer-perf-test.sh`).

Năm hệ con quyết định throughput: **mạng** (batching/nén), **đĩa** (tuần tự + page cache), **bus bộ nhớ CPU** (zero-copy), **GC JVM** (heap nhỏ), **concurrency phía consumer** (số partition).

## 16.2 Producer — đòn bẩy lớn nhất: batching + nén

| Config | Mặc định | Tối ưu throughput | Tối ưu latency |
|---|---|---|---|
| `linger.ms` | 0 | 10–50 | 1 |
| `batch.size` | 16KB | 64–256KB | 16KB |
| `buffer.memory` | 32MB | 64MB | — |
| `compression.type` | none | `lz4`/`zstd` | `snappy` |
| `acks` | (xem 7.6) | `1` nếu chấp nhận mất | `all` |
| `max.in.flight` | 5 | 5 (an toàn với idempotence) | — |

**Nén (thực hiện theo batch ở producer, broker giữ nguyên):**

| Thuật toán | Đặc điểm | Dùng cho |
|---|---|---|
| `lz4` | Rất nhanh, tỷ lệ ~2–2.5× | **Mặc định nên dùng** (microservice, latency thấp) |
| `snappy` | Nhanh, ~2× | Pipeline cũ |
| `zstd` | **Tỷ lệ tốt nhất** (~3.5–5×) | Analytics, log, telemetry, truyền WAN |
| `gzip` | Tỷ lệ cao, CPU nặng | Lưu trữ lạnh, không hợp real-time |

Đánh đổi: `linger.ms` cao = thêm độ trễ; chấp nhận được cho pipeline batch, không hợp giao dịch tương tác.

## 16.3 Broker

- **Đĩa:** NVMe SSD, không HDD/NFS; nhiều thư mục `log.dirs` trên nhiều đĩa; tách khỏi đĩa OS; **XFS** + `noatime`.
- **Heap nhỏ, page cache lớn:** ~6–8GB heap (`-Xms6g -Xmx6g`). **Không bao giờ** đặt heap 32GB+ (GC pause dài, bóp page cache, phá zero-copy).
- **Thread:** `num.network.threads` (~ số CPU/2), `num.io.threads` (~ số CPU). Nếu `RequestHandlerAvgIdlePercent < 0.2` → broker đang CPU-bound.
- **Replication:** `replica.fetch.max.bytes` tăng, `num.replica.fetchers` tăng cho nhiều đĩa/replication cao.
- **Quotas** để chặn noisy neighbor: `producer_byte_rate`, `consumer_byte_rate` theo client.

## 16.4 Consumer

- `fetch.min.bytes` ~1MB + `fetch.max.wait.ms=500`: gom fetch (consumer-side batching).
- `max.partition.fetch.bytes`, `max.poll.records` tăng cho tải batch.
- Quy tắc: `concurrency × số instance ≤ số partition`.

## 16.5 Hệ điều hành & JVM

```ini
vm.swappiness = 1                 # Kafka ghét swap
vm.dirty_background_ratio = 5
vm.dirty_ratio = 10               # (một tài liệu gốc đề xuất 80 — xem Phần 19)
fs.file-max = 1000000             # mỗi partition = nhiều file handle
net.core.rmem_max / wmem_max = 16777216  (hoặc lớn hơn cho link BDP cao)
```
- **GC:** G1GC (mặc định cổ điển, pause 20–200ms) hoặc ZGC (pause < 10ms) cho latency thấp. Pause dài → ISR flapping.
- Rule vàng: máy 64GB → heap 6GB, **58GB cho page cache**.

## 16.6 Tiered Storage (Kafka 3.6+)

Đẩy segment nguội lên object storage → giảm nhu cầu đĩa broker (`remote.log.storage.system.enable`, `local.retention.ms`, `retention.ms`). Consumer đọc dữ liệu cũ sẽ **chậm hơn** (đọc từ object storage); consumer đọc dữ liệu mới vẫn dùng page cache. Nhớ: compaction chỉ chạy trên segment local.

## 16.7 Checklist throughput

- [ ] Broker Linux, heap nhỏ (6–8GB), zero-copy hoạt động
- [ ] `compression.type=lz4` (hoặc `zstd` cho batch lớn)
- [ ] `linger.ms=10..50`, `batch.size=64–256KB`
- [ ] `fetch.min.bytes=1MB`, `fetch.max.wait.ms=500`
- [ ] Tuning sysctl (TCP buffer, dirty ratio, swappiness)
- [ ] Số partition ≥ tổng số consumer thread

> **Chuyển tiếp:** Chạy production thì phải nhìn được cụm — Phần 17.

---

# PHẦN 17 — Monitoring & Operations

## 17.1 Bốn tầng giám sát

| Tầng | Theo dõi | Công cụ |
|---|---|---|
| Hạ tầng | CPU, RAM, đĩa I/O, mạng | node_exporter, CloudWatch |
| Broker | Replication, leadership, request | JMX Exporter, Kafka Exporter |
| Producer/Consumer | Lag, error rate, throughput | Micrometer, metric consumer group |
| Ứng dụng | **Độ trễ end-to-end**, lỗi xử lý | APM, metric tự đo |

## 17.2 Metric broker cần thuộc

| Metric | Bình thường | Cảnh báo khi | Mức |
|---|---|---|---|
| `UnderReplicatedPartitions` | 0 | > 0 | Warning (nguy cơ mất dữ liệu) |
| `UnderMinIsrPartitionCount` | 0 | > 0 | **Critical** |
| `ActiveControllerCount` | **1** (toàn cụm) | ≠ 1 | **Critical** (không có controller / split-brain) |
| `OfflinePartitionsCount` | 0 | > 0 | **Critical** (không có leader → đọc/ghi lỗi) |
| `RequestHandlerAvgIdlePercent` | > 0.3 | < 0.2 | Warning (handler thread cạn) |
| `BytesIn/OutPerSec` | Baseline | > 90% băng thông NIC | Warning |

**KRaft controller:** `EventQueueTimeMs`, `EventQueueSize`, `MetadataErrorCount`; log `__cluster_metadata` thay cho ZooKeeper.

**Producer:** `record-error-rate` > 0, `record-retry-rate`, `request-latency-avg`, `buffer-available-bytes` thấp, `batch-size-avg` quá nhỏ (under-batching), `record-queue-time-avg`.
**Consumer:** `records-lag-max`, `records-lag` (xu hướng tăng), `fetch-rate` tụt, `records-consumed-rate`, `commit-latency-avg`.

## 17.3 Theo dõi lag

Công cụ: Burrow (LinkedIn), Kafka Exporter + Prometheus + Grafana, Confluent Control Center, Datadog/CloudWatch, Conduktor. Nên cảnh báo cả **giá trị lag** lẫn **tốc độ tăng** (`rate(lag[10m]) > N`).

**Độ trễ end-to-end:** nhúng `produce-time` vào header, consumer tính `now − produce-time` rồi ghi metric. SLA tham khảo: thanh toán p99 < 200ms; tồn kho p99 < 1s; analytics p99 < 10s.

## 17.4 Lệnh vận hành thường dùng

```bash
# Topic
kafka-topics.sh --bootstrap-server $B --create --topic orders --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $B --alter --topic orders --partitions 12     # chỉ tăng!

# Xem message
kafka-console-consumer.sh --bootstrap-server $B --topic orders --from-beginning \
  --property print.key=true --property key.separator=":"

# Config
kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=86400000

# Quota
kafka-configs.sh --bootstrap-server $B --entity-type clients --entity-name batch-importer \
  --alter --add-config producer_byte_rate=1048576

# Di chuyển partition giữa broker
kafka-reassign-partitions.sh --bootstrap-server $B --topics-to-move-json-file topics.json --broker-list "1,2,3" --generate
kafka-reassign-partitions.sh --bootstrap-server $B --reassignment-json-file reassign.json --execute
kafka-reassign-partitions.sh --bootstrap-server $B --reassignment-json-file reassign.json --verify
```

## 17.5 Tình huống hay hỏi

- **Partition under-replicated:** xem `--describe` (ISR vs replica list) → tìm broker lag → điều tra GC/đĩa/mạng → nếu hồi phục sẽ tự bắt kịp; nếu chết hẳn thì `kafka-reassign-partitions.sh` sang broker khỏe.
- **Xoá topic đang có consumer:** consumer gặp `UnknownTopicOrPartitionException`, thực chất ngừng xử lý.
- **Preferred leader election:** `auto.leader.rebalance.enable=true` — sau khi broker sập/hồi phục, leader bị lệch; Kafka định kỳ trả leader về broker "ưu tiên" ban đầu để cân tải.

---

# PHẦN 18 — Bảo mật & Quản trị dữ liệu

## 18.1 Authentication ("Bạn là ai?")

| Cơ chế | Hợp với | Ghi chú |
|---|---|---|
| `SASL/PLAIN` | Dev/test | Gửi user/pass **dạng rõ** → luôn kèm TLS |
| **`SASL/SCRAM-SHA-512`** | **Đa số production** | Challenge-response, không gửi mật khẩu; credential lưu trong metadata KRaft |
| `SASL/GSSAPI` (Kerberos) | Doanh nghiệp có AD/KDC | SSO nhưng vận hành phức tạp (đồng hồ, DNS, KDC) |
| **mTLS** | Container, service mesh, zero-trust | Principal lấy từ CN/DN của cert (`ssl.principal.mapping.rules`); không quản lý mật khẩu nhưng cần tự động hoá cert |
| **OAuth 2.0 / OIDC** (`OAUTHBEARER`) | Cloud-native, microservices | JWT từ IdP; broker kiểm chữ ký (JWKS), hạn, issuer/audience; principal từ claim `sub`; token thường 15–60 phút, client tự refresh |

Mọi thành phần phải xác thực: Connect, Schema Registry, ksqlDB, Streams/Flink, admin tools.

## 18.2 Authorization — ACL ("Bạn được làm gì?")

**ACL = Principal + Resource + Operation + Permission (Allow/Deny).** Trong KRaft, ACL lưu ở metadata log (lan truyền cỡ ms).

- **Bẫy #1:** hầu hết thao tác cần thêm **`Describe`**.
    - Producer: `Write` + `Describe` (topic) [+ `IdempotentWrite` trên cluster nếu idempotent].
    - Consumer: `Read` + `Describe` (topic) + `Read` (consumer group).
    - Streams: input/output topic + group + **internal topic theo prefix** (`--operation All --resource-pattern-type prefixed --topic <app-id>-`).
- **Pattern:** `LITERAL` (khớp chính xác), `PREFIXED` (`team-a.` phủ cả `team-a.orders`, `team-a.payments`), `WILDCARD` (`*`, hạn chế). Prefixed giảm mạnh ACL sprawl.
- **Mẫu thiết kế:** theo team (prefix), theo service (least privilege — mỗi microservice 1 principal), tách môi trường bằng prefix (`dev.` / `staging.` / `prod.`).
- Nâng cao: OAuth + ACL, Open Policy Agent (OPA).
- **Cấu hình quan trọng nhất:** `allow.everyone.if.no.acl.found=false` (**deny-by-default**). Nếu để mặc định, resource không có ACL thì ai cũng dùng được.

## 18.3 Checklist bảo mật production

| Lớp | Biện pháp |
|---|---|
| Authentication | SCRAM-SHA-512 / mTLS / OAuth |
| Authorization | ACL **deny-by-default** |
| Mã hoá đường truyền | **TLS 1.3** (bỏ cipher yếu, handshake 1-RTT; overhead ~<5% với AES-NI) |
| Mã hoá lưu trữ | Mã hoá đĩa/filesystem |
| Mạng | Subnet riêng, firewall/security group chỉ cho subnet ứng dụng |
| Credential | Vault/Secrets Manager (có thể cấp **dynamic secret TTL ngắn**), **không hardcode** |
| Audit | Log authorizer → SIEM |
| Zero Trust | "Không tin ai, luôn xác minh" — kể cả broker-to-broker, không tin theo IP |
| Kiểm thử | Thử deny-by-default (`TopicAuthorizationException`), thử kết nối plaintext (phải thất bại), chaos: cert hết hạn, ACL bị thu hồi |

**Giám sát an ninh:** auth fail > 10/phút; authorization fail > 5/phút; TLS handshake fail; kết nối từ IP lạ; hoạt động superuser bất thường. Đẩy vào SIEM: đăng nhập/thất bại, deny, thao tác admin (tạo/xoá topic, đổi ACL/config), throttle quota.

**Trong KRaft (Kafka 4.0+):** credential SCRAM và ACL nằm trong `__cluster_metadata`, lan truyền nhanh, không còn ZooKeeper.

## 18.4 Data Governance — rộng hơn bảo mật

> Broker Kafka **chỉ phục vụ byte**, không có khái niệm "team", "chủ sở hữu topic", "field nhạy cảm", "hợp đồng schema", "lưu vết audit". Governance phải dựng **bên ngoài broker**.

| | Bảo mật | Governance |
|---|---|---|
| Câu hỏi | Người **sai** có chạm tới dữ liệu không? | Người **đúng** có chạm đúng dữ liệu, **đúng hình dạng** không? |
| Phạm vi | Encrypt, authn, authz | Bảo mật + schema + ownership + quality + lineage |

Bảo mật là **tập con** của governance.

**Sáu primitive:**

| Primitive | Trả lời | Công cụ |
|---|---|---|
| Schema policy | Schema nào được phép, quy tắc tiến hoá, sai hợp đồng thì sao | Schema Registry + CI |
| Topic ownership | Ai chịu trách nhiệm, topic mồ côi thế nào | Catalog/GitOps |
| Access control | Ai (người/service) được produce/consume/tạo/xoá | ACL/RBAC, prefixed ACL, IaC |
| Encryption & masking | Field nào nhạy cảm, mã hoá, che ở môi trường thấp | TLS + KMS + field-level encryption/gateway |
| Audit & lineage | Ai làm gì, khi nào, từ đâu | Authorizer log → SIEM |
| Data quality | Message phải qua luật nào, hỏng thì reject/DLQ | Gateway/DLQ |

**4 mức trưởng thành:** (1) Ad hoc → (2) Discoverable (có Schema Registry, quy ước đặt tên, audit về SIEM) → (3) Owned (catalog, workflow xin quyền, chặn thay đổi schema phá vỡ khi produce) → (4) Programmable (governance as code — Terraform/GitOps).

**Khi nào thấy đau:** > 50 topic / > 10 team → ACL sprawl; nhiều producer/topic → schema drift; nhân sự luân chuyển → topic mồ côi; dữ liệu nhạy cảm → cần field-level encryption; yêu cầu tuân thủ → cần audit truy vấn được.

---

# PHẦN 19 — Tổng kết

## 19.1 Ba bộ cấu hình "công thức" cần nhớ

```properties
# (A) Không mất dữ liệu / tài chính / đơn hàng
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
linger.ms=5                      # 1 nếu cần độ trễ thấp
compression.type=lz4
# broker/topic:
replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
# consumer:
enable.auto.commit=false
partition.assignment.strategy=CooperativeStickyAssignor
isolation.level=read_committed   # nếu topic có transaction
```

```properties
# (B) Throughput cao, chấp nhận mất chút (analytics/telemetry)
acks=1
linger.ms=20
batch.size=65536      # hoặc lớn hơn
compression.type=lz4  # hoặc zstd
buffer.memory=67108864
# consumer: fetch.min.bytes=1048576, max.poll.records=2000
```

```properties
# (C) Exactly-once trong Kafka (consume-process-produce)
# producer: enable.idempotence=true, acks=all, transactional.id=<ổn định theo vai trò>
# consumer: isolation.level=read_committed, enable.auto.commit=false
# Streams:  processing.guarantee=exactly_once_v2
# ra hệ ngoài: + idempotency key + unique constraint / outbox
```

## 19.2 Bảng quyết định nhanh

| Câu hỏi | Trả lời |
|---|---|
| Cần thứ tự theo entity? | **Key = entity ID** |
| Cần thứ tự toàn cục? | 1 partition (mất song song) — cân nhắc có thật sự cần không |
| Consumer không theo kịp? | Chẩn đoán trước (poison? DB chậm? rebalance?) → thêm consumer ≤ số partition / batch / parallel |
| Muốn scale hơn số partition? | Tăng partition (cẩn thận với key) hoặc parallel dispatch theo key |
| Cần replay? | Reset offset / group mới / `seek` |
| Cần "bảng trạng thái hiện tại"? | `cleanup.policy=compact` (+ key!) |
| Ghi ra hệ ngoài mà cần đúng 1 lần? | Idempotency key + unique constraint (+ Redis/outbox) |
| Pipeline thuần Kafka→Kafka cần đúng 1 lần? | Kafka Streams `exactly_once_v2` |
| Đưa data DB ↔ Kafka? | **Kafka Connect** (CDC = Debezium, `tasks.max=1`) |
| Biến đổi đơn giản trong Connect? | **SMT** |
| Join/aggregate/window? | **Kafka Streams** |
| DR đa vùng? | **MirrorMaker 2** + offset translation |
| Bảo mật production? | SCRAM/mTLS/OAuth + TLS 1.3 + ACL deny-by-default |

## 19.3 Những câu "phân biệt junior/senior" đáng thuộc

1. **Kafka có đảm bảo thứ tự toàn cục không?** Không — chỉ **trong 1 partition**.
2. **Vì sao không giảm được partition? Vì sao tăng partition phá thứ tự?** `hash(key) % N` đổi khi N đổi; dữ liệu cũ không được di chuyển.
3. **`acks=all` có đủ để không mất dữ liệu không?** Không — cần thêm `min.insync.replicas ≥ 2`, `RF=3`, `unclean.leader.election=false`.
4. **Idempotence có chặn mọi trùng lặp không?** Không — chỉ trong 1 phiên producer; xuyên restart cần `transactional.id`.
5. **Kafka có "exactly-once" thật không?** Chỉ **trong ranh giới Kafka**. Ra ngoài cần idempotent consumer / outbox.
6. **Vì sao consumer chỉ đọc tới High Watermark?** Để không đọc dữ liệu có thể biến mất khi leader chết.
7. **Vì sao một transaction treo chặn cả consumer khác?** Vì `read_committed` bị chặn bởi **LSO**.
8. **Vì sao heap broker phải nhỏ?** Để nhường RAM cho **page cache** (và giữ zero-copy).
9. **`session.timeout.ms` vs `max.poll.interval.ms`?** Heartbeat thread vs khoảng cách giữa hai lần `poll()`.
10. **Vì sao KRaft nhanh hơn ZooKeeper khi failover?** Standby controller và broker **đã giữ metadata trong RAM** nhờ pull log liên tục, không phải nạp lại từ đâu cả.
11. **Vì sao Debezium chỉ 1 task?** Binlog/WAL là **một luồng tuần tự**.
12. **Kafka Streams: vì sao state size là vấn đề thiết kế hạng nhất?** Thời gian recovery ∝ kích thước state.
13. **Vì sao tên topology phải tường minh?** Tên tự sinh dịch chuyển khi sửa code → rebuild state / rebalance vô tận.
14. **Kafka vs RabbitMQ?** Log có thể replay, pull, throughput cao vs broker push, routing linh hoạt, xoá khi ACK.
15. **Vì sao KTable/`__consumer_offsets`/changelog dùng compaction?** Chỉ cần giá trị mới nhất mỗi key để dựng lại trạng thái.

## 19.4 Chỗ tài liệu gốc mâu thuẫn hoặc đáng kiểm chứng

Khi tổng hợp tôi thấy vài điểm các file trong repo **không thống nhất với nhau** (hoặc với tài liệu chính thức). Ghi lại để bạn không bị lệch khi học:

| # | Điểm | Chi tiết |
|---|---|---|
| 1 | **`acks` mặc định** | File `kafka-performance-tuning` ghi mặc định `acks=1`. Nhưng `kafka-producers-consumers` (và tài liệu Kafka chính thức) nói từ **Kafka 3.0** mặc định là `enable.idempotence=true` kéo theo `acks=all`. Tôi theo cách nói của 3.0+. |
| 2 | **EOS V2: producer theo task hay theo thread?** | `exactly-once.md` mô tả "mỗi task có producer riêng", còn `kafka-streams-deep-dive` và `interview-advanced` nói **V1 = mỗi task, V2 = mỗi stream thread**. Đúng theo KIP-447 là **V2 = mỗi thread**. |
| 3 | **`session.timeout.ms` mặc định** | Các file ghi 10s, 30s, 45s khác nhau. Từ Kafka 3.0 mặc định của consumer là **45s**; hãy kiểm tra phiên bản bạn dùng. |
| 4 | **Thời gian failover controller ZooKeeper** | Nơi ghi 15–30s, nơi ghi 30s–30 phút. Thực tế phụ thuộc số partition/ZNode. Ý chính không đổi: **KRaft nhanh hơn nhiều bậc**. |
| 5 | **Chi phí transaction** | Có nơi ghi ~2–5ms/commit và 5–15% latency; nơi khác ghi vài % tới ~20–30% throughput. Nên **tự đo** trên workload thật. |
| 6 | **`vm.dirty_ratio`** | Một file đề xuất 80, file khác 10. Đây là điểm cần benchmark; tránh copy mù. |
| 7 | **Khẳng định Kafka 4.0** | Một file nói ZGC là GC mặc định và `zstd` mặc định cho internal topic ở Kafka 4.0. Tôi **không xác nhận được** hai điều này — nên kiểm tra release notes trước khi tin. |
| 8 | **Share Groups (KIP-932)** | Tài liệu gốc mô tả Early Access/Preview và tính năng còn thiếu (key ordering, EOS, DLQ). Trạng thái có thể đã đổi — kiểm tra phiên bản Kafka hiện tại. |
| 9 | **Confluent Parallel Consumer** | Tài liệu gốc ghi là **không còn bảo trì** — chỉ dùng để hiểu kiến trúc. |
| 10 | **`rebalance-storms.md`** | Nói về **rebalance của Kafka Connect**, không phải rebalance của consumer group thường. |
| 11 | **`schema-registry.md`** | File thiếu nhiều đoạn nội dung (các phần trống trong bản gốc). Tôi đã đánh dấu **[bổ sung]** chỗ dùng kiến thức chuẩn ngoài repo. |
| 12 | **Auto-commit** | `consumer-overview` gắn nhãn "at-most-once risk" cho auto-commit rồi lại mô tả cả at-least-once. Thực tế: auto-commit có thể **trùng hoặc mất** tuỳ thời điểm commit so với xử lý — tốt nhất tắt và commit thủ công. |

---

*Hết. Đọc theo thứ tự Phần 1 → 19 là đủ để nắm mạch Kafka từ nền tảng tới vận hành production.*