---
title: 08 - Tính năng nâng cao (Log Compaction, Schema Registry, Streams, Connect)
---

# 1. Log Compaction — giữ trạng thái mới nhất

Thay vì xoá theo thời gian, `cleanup.policy=compact` chỉ giữ lại **giá trị mới nhất của mỗi key** trong partition → topic hoạt động gần như một **bảng trạng thái hiện tại**.

- Chỉ hoạt động với message **có key**. Không key = không bao giờ compact.
- **Tombstone**: message có key nhưng value = null → tín hiệu "xoá key này". Được giữ thêm một khoảng thời gian (`delete.retention.ms`, mặc định 24h) để consumer chậm vẫn kịp thấy lệnh xoá.
- **Dùng ở đâu:** đồng bộ dữ liệu (CDC) — topic luôn chứa trạng thái hiện tại của mỗi dòng dữ liệu; phân phối config; nội bộ Kafka Streams dùng để backup state (KTable, xem mục 3).

# 2. Schema Registry — hợp đồng dữ liệu

**Vấn đề:** không kiểm soát schema, các thay đổi cấu trúc dữ liệu ("schema drift") sẽ âm thầm tích tụ tới khi vỡ production. **Schema Registry** là kho tập trung để lưu, đánh phiên bản và kiểm tra tính tương thích của schema.

- Mỗi message được gắn kèm 1 ID schema (không nhúng nguyên schema vào từng message) → tiết kiệm băng thông.
- **Quy tắc tiến hoá an toàn:** thêm field mới → luôn để **optional** (có giá trị mặc định); đổi tên field → dùng alias thay vì đổi trực tiếp; thay đổi phá vỡ tương thích → tạo topic mới thay vì sửa topic cũ.
- Định dạng phổ biến: **Avro** (gọn, có Schema Registry chuẩn), **Protobuf** (tốt cho đa ngôn ngữ), JSON Schema (dễ đọc bằng mắt nhưng kém gọn).

# 3. Kafka Streams — xử lý luồng ngay trong ứng dụng

> Kafka Streams là một **thư viện** (không phải hệ thống cụm riêng) giúp ứng dụng của bạn xử lý luồng dữ liệu có trạng thái, chịu lỗi — chỉ cần import thư viện và deploy như một microservice bình thường.

## Ba khái niệm cốt lõi

| | Ý nghĩa | Ví dụ |
|---|---|---|
| **KStream** | Chuỗi sự kiện độc lập, vô hạn, chỉ thêm vào | Sự kiện đặt hàng, click |
| **KTable** | "View" trạng thái mới nhất theo key — bản ghi mới **thay thế** bản cũ | Số dư tài khoản, hồ sơ người dùng |
| **GlobalKTable** | KTable được nhân bản đầy đủ tới **mọi** instance | Danh mục sản phẩm, dữ liệu tham chiếu ít đổi |

- **Task = 1 partition nguồn** → trần song song của Kafka Streams = số partition.
- **State Store**: database key-value cục bộ (mặc định RocksDB) để lưu trạng thái tính toán (aggregate, join). Được backup bằng 1 **changelog topic** (dạng compacted) — đây mới là "nguồn sự thật", đĩa local chỉ là cache. Instance chết → task chuyển instance khác → nạp lại state từ changelog.
- **Windowing**: chia luồng vô hạn thành khung thời gian hữu hạn để tính toán tổng hợp — kiểu **Tumbling** (không chồng lấn), **Hopping** (có chồng lấn), **Session** (theo khoảng thời gian hoạt động).
- **Khi nào dùng:** cần tính toán có trạng thái (aggregate/join/window) hoàn toàn trong hệ sinh thái Kafka, muốn vận hành đơn giản (không cần cụm riêng như Spark/Flink).
- **Khi nào không dùng:** cần join với DB ngoài rất lớn, cần SQL ad-hoc (nên dùng ksqlDB/Flink), cần xử lý dữ liệu lịch sử dạng batch lớn.

# 4. Kafka Connect — tích hợp với hệ thống ngoài

Framework di chuyển dữ liệu giữa Kafka và hệ thống ngoài (DB, S3, Elasticsearch...) mà không cần tự viết code producer/consumer riêng.

| Khái niệm | Ý nghĩa |
|---|---|
| **Connector** | Plugin phụ trách 1 loại nguồn/đích dữ liệu |
| **Task** | Đơn vị công việc thực thi thật; 1 connector có thể chạy nhiều task song song |
| **Source connector** | Đưa dữ liệu từ hệ ngoài **vào** Kafka |
| **Sink connector** | Đưa dữ liệu từ Kafka **ra** hệ ngoài |
| **SMT (Single Message Transform)** | Biến đổi từng message ngay trong luồng (mask dữ liệu nhạy cảm, đổi tên topic...) |

- **CDC (Change Data Capture, ví dụ Debezium):** đọc trực tiếp binlog/WAL của database để bắt mọi thay đổi (kể cả DELETE) mà không cần polling — độ trễ thấp, không tạo thêm tải cho DB. Loại connector này thường chỉ chạy **1 task duy nhất** vì log giao dịch của DB là một luồng tuần tự.

# 5. MirrorMaker 2 — sao chép giữa các cluster

Dùng khi cần: dự phòng thảm hoạ (Disaster Recovery), tuân thủ quy định vùng dữ liệu, hoặc gom dữ liệu từ nhiều vùng về trung tâm. Xây dựng trên nền Kafka Connect, tự động: sao chép dữ liệu, đồng bộ offset của consumer group, và theo dõi sức khoẻ đường truyền giữa 2 cluster.
