# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Lê Văn Tài / 2A202602464  
**Repo:** `https://github.com/<TEN_GITHUB>/K4-Track02-Day17-LeVanTai-2A202602464-DataPipelineEngineering` *(thay `<TEN_GITHUB>` sau khi tạo repo public)*  
**Commit mã nguồn dùng để kiểm tra:** `5e3b0460faad717913712f074843d1960770bab2`  
**AI đã dùng và phạm vi hỗ trợ:** Codex hỗ trợ đọc code, tái hiện lỗi, đề xuất/sửa code, chạy kiểm tra và soạn REPORT; em đã review các thay đổi và output.  
**Nguồn tham khảo khác:** README, docs/RUBRIC.md và docs/SUBMISSION.md của repository bài lab.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Verify ban đầu báo 24 hàng/12 ticket; T-91 có ba trạng thái thay vì `high/closed/bug`. | `gold_feature_daily` không khớp full recompute; event offline u05 ngày 08-12 giao ngày 08-15 không được tính về 08-12. | Contract tombstone T-97 fail: delete không tạo được hàng `is_deleted=true`, nên ticket còn hiện ở Gold. |
| **Nguyên nhân gốc** | Code `INSERT` batch mới, không ghi theo `ticket_id` và không chặn batch LSN cũ ghi đè trạng thái mới. | `LOOKBACK_DAYS=0`, trong khi Bronze đo được P99 lateness 3 ngày. | Staging chỉ lấy `ticket_id` từ `after`; Debezium delete có `after=null`, còn khóa nằm ở `before`. |
| **Cách sửa** | `pipeline/silver.py`: dùng `MERGE` theo `ticket_id`, chỉ `UPDATE` khi `s._lsn > t._lsn`. | `pipeline/config.py`: đặt `LOOKBACK_DAYS=3`; Gold overwrite các partition event-time trong cửa sổ lookback. | `pipeline/staging.py`: `coalesce(after.ticket_id, before.ticket_id)`; MERGE ghi tombstone với các cột PII là `NULL`, rồi Gold bỏ ticket deleted. |
| **Khái niệm trên slide** | Silver: một hàng một thực thể; keyed upsert/MERGE, LSN ordering và idempotency. | Late-arriving data: đo lateness ở Bronze, chọn `ceil(P99)`, recompute theo event time. | CDC log-based: đọc Debezium envelope; delete phải lan thành tombstone tới dữ liệu Gold sử dụng. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`.
- `submission/checksums.txt`: **PASS** — combined Gold checksum: `39e115c510ecdf526800eac227158a4f`.
- dbt build: **PASS=19**; `scripts.parity`: **PARITY**.

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE theo khóa cho `silver_tickets` giữ đúng một trạng thái thực thể và LSN guard làm replay an toàn; Gold feature được overwrite theo partition vì aggregate có thể thay đổi khi event đến muộn.
- Tombstone giữ bằng chứng xóa và thứ tự LSN để delete không bị bản tin cũ “hồi sinh”, đồng thời xoá PII ở trạng thái live.
- Snapshot training được dựng “as of” Bronze theo version nên tái lập được và late feedback chỉ tạo snapshot mới, không sửa snapshot đã phát hành.
- DuckDB phù hợp dữ liệu cục bộ nhỏ, zero-key và test nhanh; dbt mô tả tương đương bằng model SQL, contract/test và incremental strategy rõ ràng.

## 4. Hai câu hỏi suy ngẫm

1. Tách snapshot bất biến phục vụ reproducibility khỏi dữ liệu có thể nhận diện. Khi nhận delete, tombstone chặn T-97 ở Silver/Gold live và RAG; với snapshot cũ, lưu delete ledger, chặn quyền truy cập/retrieval theo ticket, xóa hoặc mã hóa bằng per-subject key để crypto-shred khi chính sách yêu cầu. Nếu luật yêu cầu xóa vật lý, rebuild snapshot version thay thế và lưu audit/lineage thay vì lặng lẽ sửa version cũ.
2. Đặt PII gate chính ở Silver trước mọi bảng/embedding Gold: ngoài regex, dùng DLP + NER nhận diện tên, tenant policy và quarantine/redaction review. Đo precision/recall trên tập gán nhãn, tỷ lệ phát hiện/chặn, false positive/negative theo nguồn/ngôn ngữ, số bản ghi quarantine và kiểm quét định kỳ toàn bộ Silver/Gold.

## 5. Output (thực tế)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest --disable-warnings
..................................                                       [100%]
34 passed in 8.59s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12
fresh build             8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ Push-Location dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
