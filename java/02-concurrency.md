---
title: 02 - Concurrency (Fundamentals, Threads & Locks)
---

# 1. Concurrency vs Parallelism

- **Concurrency**: nhiều task tiến triển cùng lúc (có thể xen kẽ trên 1 CPU core, không nhất thiết chạy đồng thời thật sự).
- **Parallelism**: nhiều task chạy thật sự cùng lúc trên nhiều CPU core.
- Concurrency là về cách *cấu trúc* chương trình để xử lý nhiều việc; parallelism là về cách *thực thi* nhiều việc cùng lúc bằng phần cứng.

# 2. Java Memory Model (JMM)

- JMM định nghĩa quy tắc: khi nào một thread thấy được thay đổi dữ liệu do thread khác ghi.
- Mỗi thread có thể cache biến vào local (CPU cache/register) → thread khác không thấy giá trị mới ngay → gây race condition.
- **`volatile`**: đảm bảo đọc/ghi biến luôn từ main memory (visibility), không cache riêng ở từng thread. Không đảm bảo tính nguyên tử (atomicity) cho thao tác kiểu i++.
- **happens-before**: quy tắc thứ tự đảm bảo thao tác ghi trước một điểm mốc (unlock, volatile write, thread start...) thì thread sau chắc chắn thấy được.
- **`synchronized`**: đảm bảo cả visibility lẫn atomicity (mutual exclusion) cho block code, nhưng tốn chi phí hơn `volatile`.

# 3. Thread Pool & Connection Pooling

- **Vì sao cần pool**: tạo thread mới tốn tài nguyên (OS phải cấp phát stack, context switch). Pool tái sử dụng thread có sẵn thay vì tạo/hủy liên tục.
- `ExecutorService` (Java) quản lý pool thread: `newFixedThreadPool`, `newCachedThreadPool`, `newScheduledThreadPool`...
- Thông số quan trọng của thread pool: core pool size, max pool size, queue chứa task chờ, chính sách reject khi quá tải (RejectedExecutionHandler).
- **Connection Pooling** (DB): tương tự nhưng cho kết nối DB — mở/đóng connection tốn chi phí, pool giữ sẵn connection để tái sử dụng (HikariCP là pool phổ biến nhất hiện nay).

# 4. Threads

- Tạo thread: extend `Thread` hoặc implement `Runnable` (khuyến khích Runnable vì Java không hỗ trợ đa kế thừa class).
- Vòng đời thread: `NEW → RUNNABLE → BLOCKED/WAITING/TIMED_WAITING → TERMINATED`.
- `start()` tạo thread mới thật sự chạy; gọi nhầm `run()` chỉ chạy như method bình thường trên thread hiện tại (không tạo thread mới).
- Race condition: nhiều thread cùng đọc/ghi 1 biến chia sẻ mà không đồng bộ → kết quả sai, không dự đoán được.
- Deadlock: 2+ thread chờ lock của nhau vô thời hạn. 4 điều kiện gây deadlock: mutual exclusion, hold and wait, no preemption, circular wait — phá vỡ 1 trong 4 điều kiện này sẽ tránh được deadlock.

# 5. Locks

- `synchronized`: lock ngầm (intrinsic lock), tự động unlock khi thoát block/method, đơn giản nhưng kém linh hoạt.
- `ReentrantLock` (java.util.concurrent.locks): lock tường minh, linh hoạt hơn — có thể `tryLock()` (không chờ vô hạn), lock có thể ngắt (interruptible), hỗ trợ fairness (thread chờ lâu được ưu tiên).
- `ReadWriteLock`: tách lock đọc và lock ghi — nhiều thread được đọc cùng lúc, nhưng ghi thì phải độc quyền. Phù hợp khi đọc nhiều, ghi ít.
- Reentrant (khả năng tái nhập): 1 thread đã giữ lock có thể lock lại chính nó nhiều lần (ví dụ gọi method khác cũng synchronized trên cùng object) mà không bị deadlock với chính mình.

# 6. AQS — AbstractQueuedSynchronizer

- Là nền tảng bên dưới của hầu hết cơ chế đồng bộ nâng cao trong Java: `ReentrantLock`, `Semaphore`, `CountDownLatch`, `ReentrantReadWriteLock`.
- Cơ chế: dùng 1 biến `state` (int, quản lý bằng CAS - compare-and-swap) để biểu diễn trạng thái lock (0 = free, >0 = đang giữ), và một hàng đợi (FIFO queue) chứa các thread đang chờ.
- Thread không lấy được lock sẽ bị đưa vào queue và "park" (ngủ), khi lock được release thì thread đầu hàng đợi được "unpark" (đánh thức).
- Hiểu AQS giúp hiểu vì sao các lock nâng cao của Java hiệu quả hơn synchronized thô: tránh busy-wait, dùng CAS thay vì lock hệ điều hành khi có thể (giảm context switch).
