---
title: 06 - Interview Questions (tổng hợp)
---

Dùng để tự kiểm tra sau khi đã đọc hết 5 file trước. Trả lời ngắn gọn trước khi xem đáp án gợi ý.

1. **`==` khác `equals()` thế nào?** → `==` so sánh địa chỉ, `equals()` so sánh nội dung (nếu override đúng).
2. **Vì sao override `equals()` thì phải override `hashCode()`?** → Nếu không, 2 object equals=true nhưng hashCode khác nhau → HashMap/HashSet coi là 2 phần tử khác nhau, sai logic.
3. **String có immutable không, vì sao?** → Có. Giúp an toàn khi dùng chung String Pool, an toàn thread, làm key HashMap ổn định (hashCode không đổi).
4. **`ArrayList` vs `LinkedList` dùng khi nào?** → ArrayList khi cần truy cập ngẫu nhiên nhiều (đọc nhiều); LinkedList khi thêm/xóa đầu-cuối nhiều.
5. **`HashMap` không an toàn thread, giải pháp?** → Dùng `ConcurrentHashMap` (khóa từng phần) thay vì `Hashtable`/`synchronizedMap` (khóa toàn bộ, chậm).
6. **`volatile` giải quyết vấn đề gì, không giải quyết được gì?** → Giải quyết visibility (thread thấy giá trị mới nhất). Không giải quyết atomicity (i++ vẫn có race condition).
7. **`synchronized` vs `ReentrantLock`?** → synchronized đơn giản, tự unlock; ReentrantLock linh hoạt hơn (tryLock, interruptible, fairness) nhưng phải tự unlock trong `finally`.
8. **4 điều kiện gây deadlock?** → Mutual exclusion, hold and wait, no preemption, circular wait.
9. **Stack và Heap khác nhau ở đâu?** → Stack: theo từng thread, lưu local var/frame, tự dọn khi method kết thúc. Heap: dùng chung, lưu object thật, GC dọn.
10. **GC dọn rác dựa trên nguyên tắc gì?** → Object không còn được tham chiếu từ GC Root (unreachable) thì bị coi là rác.
11. **Vì sao chia Young/Old Generation?** → Đa số object chết sớm (weak generational hypothesis) → dọn vùng trẻ thường xuyên, nhanh; vùng già dọn ít hơn vì object ở đó sống lâu, ít khi thành rác.
12. **Virtual thread khác platform thread ở đâu?** → Virtual thread rất nhẹ, do JVM quản lý, nhiều virtual thread chia sẻ ít carrier (OS) thread, tự "nhường chỗ" khi bị block I/O.
13. **Checked vs Unchecked exception?** → Checked bắt buộc xử lý (try-catch/throws) lúc compile; unchecked (RuntimeException) thì không bắt buộc.
14. **Interface vs Abstract class, chọn khi nào?** → Interface: cần đa kế thừa hành vi, chỉ định nghĩa "làm được gì". Abstract class: các class con có phần logic dùng chung, chỉ đơn kế thừa được.
15. **Fail-fast vs Fail-safe iterator?** → Fail-fast ném lỗi nếu sửa collection khi đang duyệt (ArrayList, HashMap); Fail-safe duyệt trên bản sao, không lỗi nhưng có thể không thấy dữ liệu mới nhất.
