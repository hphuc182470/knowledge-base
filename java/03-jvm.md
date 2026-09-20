---
title: 03 - JVM (JVM, Stack vs Heap, Virtual Threads, Diagnostics)
---

# 1. JVM là gì

- JVM (Java Virtual Machine) nạp bytecode (.class), kiểm tra (verify), rồi thực thi.
- **Class Loader**: nạp class vào bộ nhớ theo 3 bước: Loading (đọc .class) → Linking (verify, prepare, resolve) → Initialization (chạy static block/static field).
- **Runtime Data Area** gồm: Method Area (lưu metadata class, static field — dùng chung mọi thread), Heap (lưu object — dùng chung mọi thread), Stack (mỗi thread 1 stack riêng, lưu local variable & frame method), PC Register, Native Method Stack.
- **Execution Engine**: gồm Interpreter (dịch từng dòng bytecode, chậm) và JIT Compiler (Just-In-Time — biên dịch phần code chạy nhiều lần "hot code" thành mã máy native để chạy nhanh hơn).

# 2. Stack vs Heap

| | Stack | Heap |
|---|---|---|
| Lưu gì | Local variable, primitive, tham chiếu object, frame method | Object thực sự, array |
| Phạm vi | Riêng theo từng thread | Dùng chung toàn bộ ứng dụng |
| Vòng đời | Method kết thúc → tự động giải phóng | Object tồn tại đến khi không còn ai tham chiếu → GC dọn |
| Tốc độ | Nhanh (LIFO, cấp phát/giải phóng đơn giản) | Chậm hơn (quản lý phức tạp hơn) |
| Lỗi thường gặp | StackOverflowError (đệ quy vô hạn / quá sâu) | OutOfMemoryError (rò rỉ bộ nhớ, giữ tham chiếu không cần thiết) |

- Primitive khai báo local (`int x = 5`) nằm trên stack; object (`new Foo()`) — bản thân object nằm trên heap, biến tham chiếu tới nó nằm trên stack.

# 3. Virtual Threads (Java 21+, Project Loom)

- Thread truyền thống (platform thread) ánh xạ 1-1 với OS thread → tốn tài nguyên, số lượng tạo được có giới hạn (vài nghìn).
- Virtual thread: do JVM quản lý, rất nhẹ (có thể tạo hàng triệu), nhiều virtual thread chia sẻ chung 1 nhóm nhỏ OS thread (carrier thread).
- Khi virtual thread bị block (I/O, chờ lock...), JVM tự "unmount" nó khỏi carrier thread để carrier thread đó phục vụ virtual thread khác — không lãng phí OS thread khi chờ I/O.
- Rất phù hợp cho ứng dụng I/O-bound (web server xử lý nhiều request đồng thời), không cải thiện gì cho tác vụ CPU-bound thuần túy.

# 4. Diagnostics & Troubleshooting

- **Heap dump**: chụp lại toàn bộ object trong heap tại 1 thời điểm — dùng để tìm memory leak (công cụ: `jmap`, Eclipse MAT).
- **Thread dump**: chụp trạng thái tất cả thread tại 1 thời điểm — dùng để tìm deadlock, thread bị treo (`jstack`).
- **Công cụ cơ bản đi kèm JDK**:
  - `jps`: liệt kê tiến trình Java đang chạy.
  - `jstat`: xem số liệu GC theo thời gian thực.
  - `jstack`: lấy thread dump.
  - `jmap`: lấy heap dump / thống kê bộ nhớ.
  - `jconsole` / `VisualVM` / `JFR (Java Flight Recorder)`: giám sát trực quan (CPU, heap, GC, thread) theo thời gian.
- Quy trình chẩn đoán chung: xác định triệu chứng (CPU cao? memory tăng dần? request bị treo?) → chụp dump tương ứng (heap dump nếu nghi leak, thread dump nếu nghi deadlock/treo) → phân tích bằng công cụ (MAT, VisualVM).
