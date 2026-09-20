---
title: 01 - Core Language (Fundamentals, OOP, Collections, Object)
---

# 1. Java Fundamentals

- **Kiểu dữ liệu**: primitive (int, long, double, boolean, char, byte, short, float) sống trên stack, không null được; reference type (object) sống trên heap, có thể null.
- **Autoboxing/unboxing**: primitive ↔ wrapper class (int ↔ Integer) tự động, cẩn thận NullPointerException khi unbox biến null.
- **String**: immutable, lưu trong String Pool (nếu tạo bằng literal `"abc"`). Dùng `new String()` sẽ tạo object mới ngoài pool. Nên dùng `StringBuilder` khi nối chuỗi nhiều lần trong vòng lặp (String immutable → nối = tạo object mới liên tục, tốn hiệu năng).
- **== vs equals()**: `==` so sánh reference (địa chỉ), `equals()` so sánh nội dung (phải override đúng).
- **final**: biến thì không đổi được giá trị (constant); method thì không override được; class thì không kế thừa được.
- **Exception**: 
  - Checked exception (IOException...) bắt buộc try-catch hoặc throws.
  - Unchecked exception (RuntimeException, NullPointerException...) không bắt buộc.
  - `finally` luôn chạy trừ khi JVM thoát (System.exit) hoặc crash.
- **Generics**: kiểm tra kiểu dữ liệu tại compile-time, tránh ClassCastException lúc runtime. Type erasure: generic bị xóa thông tin kiểu khi biên dịch xong (chỉ tồn tại lúc compile).

# 2. OOP (4 tính chất)

- **Encapsulation (đóng gói)**: giấu dữ liệu nội bộ (private field), chỉ cho truy cập qua getter/setter → bảo vệ tính toàn vẹn dữ liệu.
- **Inheritance (kế thừa)**: class con dùng lại thuộc tính/method của class cha (`extends`). Java chỉ hỗ trợ đơn kế thừa class, đa kế thừa qua interface.
- **Polymorphism (đa hình)**:
  - Compile-time (overloading): cùng tên method, khác tham số.
  - Runtime (overriding): class con định nghĩa lại method của class cha, JVM quyết định gọi bản nào lúc chạy (dynamic dispatch).
- **Abstraction (trừu tượng)**: che chi tiết hiện thực, chỉ lộ hành vi qua `interface` hoặc `abstract class`.
  - Abstract class: có thể có method đã implement + method trừu tượng, dùng khi các class con có chung một phần logic.
  - Interface: chỉ khai báo hành vi (Java 8+ cho phép default method), dùng khi cần đa kế thừa hành vi.

# 3. Java Collections Framework

- **List**: có thứ tự, cho phép trùng.
  - `ArrayList`: mảng động, truy cập theo index nhanh O(1), thêm/xóa giữa mảng chậm O(n).
  - `LinkedList`: danh sách liên kết đôi, thêm/xóa đầu-cuối nhanh O(1), truy cập theo index chậm O(n).
- **Set**: không trùng.
  - `HashSet`: dựa trên HashMap, không thứ tự.
  - `LinkedHashSet`: giữ thứ tự chèn.
  - `TreeSet`: tự sắp xếp (theo Comparable/Comparator), dựa trên cây đỏ-đen.
- **Map**: cặp key-value, key không trùng.
  - `HashMap`: không đảm bảo thứ tự, cho phép 1 key null. Từ Java 8: bucket dùng linked list, nếu 1 bucket quá nhiều phần tử (≥8) sẽ chuyển sang cây đỏ-đen để tra cứu nhanh hơn.
  - `LinkedHashMap`: giữ thứ tự chèn.
  - `TreeMap`: tự sắp xếp theo key.
  - `ConcurrentHashMap`: an toàn luồng, hiệu năng tốt hơn `Hashtable`/`synchronizedMap` vì chỉ khóa từng phần (segment/bucket) chứ không khóa toàn bộ map.
- **Queue/Deque**: `ArrayDeque`, `PriorityQueue` (heap, lấy phần tử nhỏ/lớn nhất trước).
- **So sánh phần tử**: `Comparable` (定義 thứ tự tự nhiên, method `compareTo`, implement trong chính class) vs `Comparator` (định nghĩa thứ tự bên ngoài, dùng khi cần nhiều cách sắp xếp khác nhau).
- **Fail-fast vs Fail-safe iterator**: 
  - Fail-fast (ArrayList, HashMap): ném `ConcurrentModificationException` nếu sửa collection khi đang duyệt.
  - Fail-safe (CopyOnWriteArrayList, ConcurrentHashMap): duyệt trên bản sao/snapshot, không ném lỗi nhưng có thể không thấy thay đổi mới nhất.

# 4. Class Object (cha của mọi class trong Java)

- Mọi class đều ngầm kế thừa `Object` nếu không `extends` class nào khác.
- Các method quan trọng cần biết cách override đúng:
  - `equals(Object o)`: mặc định so sánh địa chỉ, cần override khi muốn so sánh theo nội dung.
  - `hashCode()`: **luôn override cùng với equals()** — 2 object bằng nhau (equals = true) thì hashCode phải giống nhau (bắt buộc, nếu không HashMap/HashSet sẽ hoạt động sai).
  - `toString()`: mặc định trả về `ClassName@hashcode`, nên override để log/debug dễ đọc.
  - `clone()`: tạo bản sao object, cần implement `Cloneable`, mặc định là shallow copy (copy nông — field reference vẫn trỏ chung object cũ).
  - `wait()/notify()/notifyAll()`: dùng để đồng bộ hóa giữa các thread (xem file 02-concurrency.md).
- Nguyên tắc: nếu 2 object equals() = true, bắt buộc hashCode() phải bằng nhau (ngược lại không bắt buộc).
