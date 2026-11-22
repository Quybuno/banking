# contracts/

**Mục đích:** Hợp đồng dữ liệu 1.0.0: schema, cấu hình generator, fixture nghiệm thu, đáp án, golden vector, công cụ kiểm tra. Nguồn duy nhất trong repo.

**Người sở hữu:** A (Mạnh)

**Đặc tả liên quan (docs/spec.md):** Phần II, Phần III

**Quy tắc:** Chỉ đổi qua PR có nhãn `contract-change`, cả nhóm duyệt, kèm version mới theo Phần II mục 10. Agent không sửa thư mục này. Chạy `python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py` phải ra 14/14 PASS.
