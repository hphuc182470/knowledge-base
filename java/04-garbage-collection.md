---
title: 04 - Garbage Collection (GC)
---

# 1. Vì sao cần GC

- Java tự động dọn các object không còn được tham chiếu tới nữa (unreachable) để giải phóng bộ nhớ heap, tránh phải `free()` thủ công như C/C++.
- Object được coi là "rác" khi không còn đường tham chiếu nào từ GC Root (local variable đang chạy, static field, thread đang sống...) đến nó.

# 2. Heap được chia vùng (Generational GC)

- **Young Generation**: nơi object mới sinh ra. Chia nhỏ thành Eden + 2 vùng Survivor (S0, S1).
  - Object mới → Eden. Khi Eden đầy → GC nhỏ chạy (Minor GC) → object còn sống chuyển qua Survivor.
  - Object sống sót qua nhiều lần Minor GC → được "thăng cấp" (promote) lên Old Generation.
- **Old Generation (Tenured)**: chứa object sống lâu. GC ở đây gọi là Major GC/Full GC, chạy ít hơn nhưng tốn thời gian hơn nhiều (quét vùng lớn hơn).
- Ý tưởng nền tảng ("weak generational hypothesis"): đa số object chết rất trẻ (dùng xong bỏ ngay) → tách vùng trẻ ra dọn thường xuyên, nhanh, hiệu quả hơn dọn chung cả heap.

# 3. Các thuật toán / bộ GC phổ biến

- **Serial GC**: 1 thread duy nhất dọn rác, dừng toàn bộ ứng dụng khi chạy (stop-the-world) — chỉ hợp ứng dụng nhỏ/heap nhỏ.
- **Parallel GC**: nhiều thread dọn song song, vẫn stop-the-world nhưng nhanh hơn Serial — ưu tiên throughput (thông lượng), chấp nhận pause lâu hơn để tổng thời gian dọn ít hơn.
- **CMS (Concurrent Mark Sweep)**: dọn phần lớn song song với ứng dụng đang chạy (giảm pause) — đã bị loại bỏ từ Java 14, thay bằng G1/ZGC.
- **G1 (Garbage First)**: mặc định từ Java 9+. Chia heap thành nhiều vùng (region) nhỏ, ưu tiên dọn vùng có nhiều rác nhất trước, cân bằng giữa throughput và độ trễ (pause time có thể cấu hình mục tiêu).
- **ZGC / Shenandoah**: GC độ trễ cực thấp (pause chỉ vài mili-giây) dù heap rất lớn (hàng trăm GB) — đánh đổi bằng việc dùng nhiều CPU hơn để xử lý song song với ứng dụng.

# 4. Khái niệm cần nhớ

- **Stop-the-world**: thời điểm GC tạm dừng toàn bộ thread ứng dụng để dọn rác an toàn — mục tiêu tối ưu GC hiện đại là giảm tối đa thời gian này.
- **Memory leak trong Java**: vẫn xảy ra được dù có GC — khi object không dùng nữa nhưng vẫn bị 1 tham chiếu "sống" nào đó giữ lại (ví dụ: static collection cứ add mà không remove, listener quên unregister).
- **`System.gc()`**: chỉ là gợi ý (hint) cho JVM chạy GC, JVM có quyền bỏ qua, không nên phụ thuộc vào nó trong code thực tế.
