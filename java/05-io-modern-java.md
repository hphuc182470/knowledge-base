---
title: 05 - I/O & Modern Java (I/O, FFM API, New Features)
---

# 1. Java I/O

- **I/O truyền thống (java.io)**: dựa trên Stream, blocking — thread gọi đọc/ghi sẽ bị chặn (block) cho đến khi xong. Đơn giản, dễ dùng cho ứng dụng nhỏ hoặc số kết nối ít.
  - `InputStream`/`OutputStream`: xử lý dữ liệu dạng byte (file nhị phân, ảnh...).
  - `Reader`/`Writer`: xử lý dữ liệu dạng ký tự (text), có xử lý encoding.
  - `BufferedReader`/`BufferedInputStream`: bọc thêm buffer để giảm số lần gọi thật xuống đĩa/mạng → nhanh hơn nhiều so với đọc trực tiếp từng byte/ký tự.
- **NIO (java.nio, Java 4+)**: dựa trên Buffer + Channel, hỗ trợ non-blocking I/O và `Selector` — 1 thread có thể theo dõi nhiều channel cùng lúc, phù hợp server xử lý hàng ngàn kết nối (thay vì 1 thread/1 kết nối như I/O truyền thống).
- **NIO.2 (java.nio.file, Java 7+)**: API `Path`/`Files` hiện đại hơn `java.io.File` — thao tác file dễ hơn, hỗ trợ symbolic link, watch service (theo dõi thay đổi thư mục).

# 2. FFM API — Foreign Function & Memory API (Java 22+, chính thức)

- Mục đích: cho phép Java code gọi thẳng thư viện native (C/C++) và truy cập vùng nhớ ngoài heap (off-heap) **an toàn hơn**, thay thế dần JNI (Java Native Interface) vốn phức tạp, dễ lỗi và không an toàn.
- **Off-heap memory**: vùng nhớ nằm ngoài heap do JVM quản lý → GC không đụng tới, giúp tránh áp lực lên GC khi làm việc với dữ liệu rất lớn (buffer mạng, cache lớn), nhưng phải tự quản lý vòng đời (dùng `Arena` để cấp phát/giải phóng có kiểm soát).
- `MemorySegment` + `Arena`: khai báo vùng nhớ off-heap, `Arena` đảm bảo vùng nhớ được giải phóng đúng lúc (tránh leak vĩnh viễn ngoài heap).

# 3. Tính năng mới đáng chú ý qua các bản Java (LTS gần đây)

- **Java 8**: Lambda expression, Stream API, `Optional`, default method trong interface — nền tảng lập trình hàm (functional) trong Java.
- **Java 9**: module system (Jigsaw) — chia ứng dụng lớn thành module rõ ràng, kiểm soát encapsulation ở cấp package.
- **Java 10/11**: `var` (local variable type inference — trình biên dịch tự suy luận kiểu, không phải kiểu động).
- **Java 14/16**: `record` — class bất biến (immutable) gọn nhẹ để lưu dữ liệu, tự sinh constructor/equals/hashCode/toString.
- **Java 17 (LTS)**: sealed class — giới hạn class nào được phép kế thừa/implement mình.
- **Java 21 (LTS)**: Virtual Threads chính thức (xem file 03-jvm.md), Pattern Matching cho `switch`, Record Pattern.
- Xu hướng chung: Java ngày càng ngắn gọn hơn (record, var, pattern matching), đồng thời mạnh hơn ở mảng hiệu năng và concurrency (virtual thread, FFM API).
