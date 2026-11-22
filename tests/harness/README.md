# tests/harness/

**Mục đích:** Test harness và oracle: đọc ground truth/expected, tính trạng thái kỳ vọng theo as_of_time, lệnh `validate --against-ground-truth`.

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần II mục 5.3, 6, 7

**Quy tắc:** Chỉ test được import module này. `src/n06` tuyệt đối không import `tests/`.
