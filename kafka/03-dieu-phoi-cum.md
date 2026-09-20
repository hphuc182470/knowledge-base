---
title: 03 - Ai điều phối cụm? ZooKeeper → KRaft
---

# 1. Vấn đề cần giải quyết

Một cluster nhiều broker cần một "người" biết: topic nào có bao nhiêu partition, ai đang là leader của partition nào, ACL ra sao... Đó là công việc của **Controller**. Trước đây Kafka dùng ZooKeeper để làm việc này; từ Kafka 3.3+ chuyển sang KRaft.

# 2. ZooKeeper (kiến trúc cũ) — vì sao bị thay thế

Phải vận hành **2 hệ phân tán riêng biệt** (ZooKeeper + Kafka). Ba nỗi đau chính:

1. **Hạ tầng kép**: phải cài đặt, scale, vá lỗi, giám sát cả hai cụm riêng.
2. **Nghẽn ở metadata**: thông tin topic/partition/ACL nằm trong các ZNode của ZooKeeper; cơ chế thông báo thay đổi (watch) bị quá tải quanh mốc **~200.000 partition**.
3. **Failover chậm**: khi Controller cũ chết, Controller mới phải **nạp lại toàn bộ dữ liệu ZooKeeper vào RAM** rồi mới hoạt động được → cụm bị "đóng băng" từ vài chục giây tới lâu hơn với cụm rất lớn.

# 3. KRaft — cách hoạt động

**Ý tưởng cốt lõi:** biến chính metadata của cluster thành **một topic Kafka nội bộ** (`__cluster_metadata`), quản lý bằng thuật toán đồng thuận **Raft**.

- **Controller quorum**: một nhóm nhỏ node (thường 3 hoặc 5) bầu ra 1 **Active Controller** bằng Raft.
- **Metadata log**: mọi thay đổi (tạo topic, đổi leader, ACL, cấu hình...) được Active Controller ghi tuần tự vào log này.
- Các Controller dự phòng và mọi Broker đều **liên tục kéo (pull)** stream metadata này và giữ **bản sao đầy đủ trong RAM**.
- **Kết quả**: khi Active Controller chết, controller khác lên thay trong **dưới 1 giây**, vì đã có sẵn dữ liệu trong RAM — không cần nạp lại từ đầu như ZooKeeper.

## So sánh nhanh

| Tiêu chí | ZooKeeper | KRaft |
|---|---|---|
| Số hệ thống cần vận hành | 2 (ZK + Kafka) | **1 (một binary duy nhất)** |
| Thời gian failover controller | Hàng chục giây → lâu hơn | **Dưới 1 giây** |
| Giới hạn số partition | ~200.000 | Hơn 1.000.000 |
| Cách lan truyền metadata | Controller chủ động đẩy | Broker chủ động kéo |
| Thuật toán đồng thuận | ZAB | Raft |
| Tình trạng | Đã bị gỡ bỏ từ Kafka 4.0 | Chuẩn mặc định từ Kafka 3.3+ |

```properties
# server.properties (chạy KRaft)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
```

# 4. Raft tóm gọn (để hiểu vì sao KRaft đáng tin)

**Bài toán Raft giải quyết:** nhiều máy phải đồng ý về **một chuỗi thao tác duy nhất**, dù có máy chết/chậm/mất mạng. Không giải quyết được sẽ dẫn tới **split-brain** — hai máy cùng nghĩ mình là leader, lịch sử dữ liệu rẽ nhánh.

**Ví dụ dễ hình dung — quán cà phê 5 nhân viên:** cả nhóm bầu ra 1 trưởng ca. Mọi đơn hàng phải qua trưởng ca; trưởng ca ghi đơn rồi đọc cho 4 người còn lại chép lại. Khi **từ 3/5 người trở lên đã chép** (đa số — "quorum") thì đơn hàng mới được coi là chốt (committed). Nếu trưởng ca vắng mặt, cả nhóm bầu lại; ai có sổ ghi ít đơn hơn thì không được người khác bỏ phiếu cho làm trưởng ca (tránh mất dữ liệu).

- **3 trạng thái của 1 node**: `Follower` (thụ động, chỉ nghe) → `Candidate` (đang tranh cử) → `Leader` (điều hành, gửi tín hiệu heartbeat đều đặn).
- **Leader Election**: thời gian chia theo các "nhiệm kỳ" (Term, số tăng dần). Nếu không nhận được heartbeat trong một khoảng thời gian ngẫu nhiên, 1 node sẽ tự ứng cử.
- **Log Replication**: leader nhận thao tác, gửi cho các node khác; khi **đa số** đã xác nhận ghi thì thao tác được coi là chốt (committed) — không thể bị đảo ngược nữa.
