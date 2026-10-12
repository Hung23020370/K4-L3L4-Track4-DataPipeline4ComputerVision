# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Đồng Mạnh Hùng` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602412` |
| GitHub | `https://github.com/Hung23020370/K4-L3L4-Track4-DataPipeline4ComputerVision` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04237` |

## Kết quả cuối

- `dataset_version` của bạn: `d4cc4614711a`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `763 / 101 / 155`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Từ kết quả bước 1 (nguồn A có 3021 dòng, nguồn B có 3234 dòng), mình thấy 5 khác biệt giữa A và B:

| Khía cạnh | Nguồn A | Nguồn B |
|---|---|---|
| Tên cột | `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer` | `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering` |
| Đơn vị tốc độ | m/s (ví dụ `1.213`) | km/h (ví dụ `55.11`) |
| Kiểu timestamp | chuỗi ISO 8601 UTC (`2025-10-07T08:00:00.014Z`) | epoch mili giây (`1759933801509`) |
| Thang bàn đạp | tỉ lệ 0–1 (`gas` = 0.028) | phần trăm (`throttle_pct` = 2.0) |
| Độ lệch đồng hồ | `telemetry_clock_offset_ms` = 0 | `telemetry_clock_offset_ms` = 1500 |

Pipeline giữ **mỗi nguồn một bảng bronze** vì schema hai nguồn khác nhau; gộp ngay sẽ phải chọn một schema và làm mất thông tin gốc. Mọi cột để ở dạng **chữ** vì bronze là bản ghi nguyên bản, chỉ thêm: nếu ép kiểu ngay, một giá trị lỗi (ô trống, tốc độ vô lý) có thể làm hỏng cả lô nạp hoặc bị mất im lặng. Để nguyên dạng chữ thì mọi dòng đều được nạp, rồi việc ép kiểu, đổi đơn vị, trừ độ lệch đồng hồ và cách ly dòng lỗi làm ở silver, nơi có thể sửa luật và chạy lại từ dữ liệu gốc. Các cột `_source_file`, `_batch_id`, `_row_no`, `_ingested_at` giúp truy vết mỗi dòng về file nguồn.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**Cách đưa hai nguồn về một schema** (theo `stg_telemetry_a.sql` và `stg_telemetry_b.sql`): nguồn B được đổi tên cột về schema của A, tốc độ chia 3.6 (km/h → m/s), bàn đạp chia 100 (% → 0–1), và `timestamp_ms` đổi sang `timestamptz` UTC. B còn bị **trừ độ lệch đồng hồ 1500 ms** lấy từ `telemetry_clock_offset_ms` trong `bronze.video_files`.

**Bằng chứng đã đúng:**
- Thời điểm bắt đầu của B sau chuẩn hoá là `2025-10-08 14:30:00.009`, khớp thời điểm bắt đầu video (14:30:00). Nếu không trừ 1,5 giây thì B lệch so với khung hình.
- Kiểm tra tự động "drive B clock offset removed" báo PASS.
- Tốc độ tối đa hợp lý: A 71 km/h, B 100 km/h.

**Bảng quarantine của mình:**

| drive | duplicate | speed_missing | speed_out_of_range |
|---|---|---|---|
| drive_a | 20 | 30 | 3 |
| drive_b | 20 | 30 | 3 |

Cân đối số dòng: A có 2968 dòng sạch + 53 cách ly = 3021 (đúng số dòng bronze); B có 3181 + 53 = 3234. Tổng `silver.telemetry_rejects` là 106 dòng.

**Vì sao vào `rejects` thay vì xoá:** dòng lỗi là bằng chứng về chất lượng nguồn. Giữ lại kèm `reject_reason` cho phép đếm tỉ lệ lỗi theo từng loại, truy ngược về cảm biến, và xét lại luật nếu luật quá chặt hoặc quá lỏng. Xoá im lặng thì không biết đã mất gì, và phép kiểm "sạch + cách ly = nhận về" cũng không làm được.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- `max_gap_ms` là **`warn`** vì khoảng trống trong telemetry (cảm biến mất tín hiệu, ngưỡng 1000 ms) là hiện tượng thật của dữ liệu xe, không phải lỗi pipeline và không làm sai số liệu. Cần báo để người vận hành biết, nhưng dừng cả pipeline là vô lý.
- `in_range` (tốc độ 0..70 m/s) là **`error`** vì tốc độ vô lý làm hỏng mọi bước sau (huấn luyện, thống kê, dashboard), nên phải chặn trước khi dữ liệu đi tiếp.

**Kết quả thí nghiệm `break-gate`** (`report/evidence/break_gate.json`):

| | Kết quả |
|---|---|
| Luật bị nới | `max_speed_mps` từ 70 lên 1000 |
| `gate_failed` | `true` |
| Test bắt được | `in_range_silver_telemetry_speed_mps__70__0`, **6 dòng** (= 3 dòng vô lý × 2 drive) |
| Tổng kết khi nới | `PASS=5 WARN=1 ERROR=1` |
| Tổng kết sau khôi phục | `PASS=6 WARN=1 ERROR=0` |

Điều này cho thấy cổng chất lượng là **lớp bảo vệ thứ hai**: dù luật làm sạch ở lớp trước bị nới nhầm, test `error` vẫn phát hiện và dừng DAG, nên dữ liệu xấu không lọt xuống gold. Test `warn` vẫn báo (WARN=1 ở cả hai lần) nhưng không chặn.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Kết quả từ `idempotency.json` (partition `trip-00003`, trip này có 60 khung):

| Cách ghi | Số dòng bảng `silver.frames` | Dấu vân tay |
|---|---|---|
| Trước khi chạy lại | 1246 | `6c7480dfde7e` |
| Delta `overwrite` theo `trip_id`, chạy lại 2 lần | 1246 | `6c7480dfde7e` (không đổi) |
| `append`, chạy 2 lần | trip-00003 có 120 dòng (đáng lẽ 60) | nhân đôi (2x) |

Ghi đè theo partition là **idempotent**: chạy bao nhiêu lần cũng ra cùng một kết quả. `append` thì mỗi lần chạy lại (do retry của Airflow hoặc do mình bấm chạy lại) lại cộng thêm dữ liệu, làm trùng khung và sai mọi thống kê phía sau.

**Vì sao Delta `overwrite` theo partition an toàn hơn xoá rồi ghi lại:**
- Delta ghi theo **giao dịch nguyên tử**: lệnh `overwrite` chỉ thay các file của partition đó bằng một commit duy nhất trong transaction log. Nếu giữa chừng có lỗi thì commit không xảy ra, bảng vẫn ở phiên bản cũ nguyên vẹn.
- Xoá rồi ghi lại là **hai bước riêng**. Nếu sập sau bước xoá và trước bước ghi, trip đó biến mất khỏi bảng (mất dữ liệu), và người đọc trong lúc đó thấy một partition rỗng.
- Chỉ đụng đúng một partition (một trip), 25 trip còn lại không bị ảnh hưởng, nên chạy lại rẻ và rủi ro nhỏ. Lịch sử phiên bản của Delta còn cho phép quay lại bản cũ nếu cần.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Khung đen và khung tối khác nhau:**

| | Khung đen | Khung tối |
|---|---|---|
| Độ sáng trung bình (luma) | dưới `dark_luma` = 12 | từ 12 đến dưới `dark_soft` = 40 |
| Bản chất | gần như không có thông tin hình ảnh | vẫn còn nội dung, chỉ thiếu sáng (đường hầm, bóng râm, ban đêm) |
| Cách xử lý | gắn cờ `is_black` và loại khỏi dữ liệu huấn luyện | gắn cờ `is_dark` nhưng **giữ lại** |
| Số khung của mình | drive A: 6, drive B: 4 | drive A: 4, drive B: 61 |

Mình **đồng ý** với cách làm này: khung đen không dạy được mô hình điều gì, còn khung tối là điều kiện thật khi lái xe (B có tới 61 khung tối vì đường hầm và bóng râm). Nếu loại hết khung tối, mô hình sẽ không bao giờ thấy điều kiện thiếu sáng và hoạt động kém đúng lúc cần nhất. Giữ lại kèm cờ thì người dùng dữ liệu vẫn lọc được nếu muốn.

**Số khung bị coi là trùng theo ngưỡng dHash** (từ `threshold.json`, tổng A = 602 khung, B = 644 khung):

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tỉ lệ tương ứng: A là 10%, 25%, 60%; B là 1%, 8%, 38%.

**Mình chọn ngưỡng 2** (đang dùng trong pipeline) vì:
- Ngưỡng 0 chỉ bắt khung giống hệt từng bit, bỏ sót khung gần như trùng chỉ khác nhiễu nén video. Với drive A, kiểm tra tự động cho thấy ngưỡng 2 bắt được 11/12 khung của đoạn camera đứng hình 6 giây.
- Ngưỡng 6 loại tới 60% khung của A, gần như chắc chắn xoá cả những khung thật sự khác nhau (số khung trùng tăng mạnh theo ngưỡng, từ 149 lên 363).
- Ngưỡng 2 nằm ở giữa: bắt được khung trùng thật mà không phá quá nhiều dữ liệu.

**Phụ thuộc vào tốc độ xe:** lấy mẫu 2 khung/giây, nên khi xe chạy chậm hoặc đứng yên, hai khung liên tiếp rất giống nhau và trùng rất nhiều; xe chạy nhanh thì cảnh đổi nhanh, khung khác nhau nhiều và cùng một ngưỡng chỉ bắt được ít khung trùng. Dữ liệu của mình phản ánh điều này: A có nhiều khung trùng hơn B ở mọi ngưỡng (149 so với 53 tại ngưỡng 2), một phần vì A có đoạn camera đứng hình và xe chạy chậm. Vì vậy một ngưỡng cố định là sự thoả hiệp; nếu muốn chính xác hơn có thể điều chỉnh ngưỡng theo tốc độ (xe chậm thì chặt hơn, xe nhanh thì lỏng hơn).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Kết quả từ `leakage.json` (1019 khung hình đáng dùng, thuộc 26 trip):

| Cách chia | Trip bị chia cắt qua nhiều split | Trip có khung ở cả train lẫn test |
|---|---|---|
| Chia **theo khung hình** (ngẫu nhiên 70/15/15) | **26 / 26** | **26 / 26** |
| Chia **theo trip** (gold của pipeline) | **0 / 26** | 0 |

Khi chia ngẫu nhiên theo khung hình, **toàn bộ 26 trip đều bị chia cắt**, và cả 26 trip đều có khung nằm ở train lẫn test.

**Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo:** pipeline lấy mẫu 2 khung/giây, nên các khung liền nhau của cùng một trip gần như giống hệt nhau (cùng cảnh, cùng ánh sáng, cùng góc nhìn). Nếu một khung nằm ở train và khung ngay cạnh nó nằm ở test, mô hình thực chất "đã thấy đề thi": nó chỉ cần nhớ cảnh đã học chứ không cần tổng quát hoá sang cảnh mới. Điểm trên tập test vì thế cao ảo, nhưng khi gặp đoạn đường chưa từng thấy ngoài thực tế thì hiệu năng thấp hơn nhiều. Chia theo trip (cảnh) thì test gồm những cảnh mô hình chưa từng gặp, nên điểm phản ánh đúng khả năng tổng quát hoá.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

**Vì sao `dataset_version` là băm nội dung:** theo `publish.py`, version là 12 ký tự đầu của SHA-256 trên danh sách `frame_id|split` của toàn bộ khung (đã sắp xếp theo `frame_id`). Ngày giờ hoặc số đếm thì **mỗi lần chạy lại lại cho giá trị khác** dù dữ liệu không đổi, nên không biết hai bản có giống nhau không, và không tái tạo được đúng bản đã huấn luyện. Băm nội dung thì cùng nội dung luôn ra cùng version (nhận diện được bản trùng, chạy lại an toàn), còn nội dung khác đi thì version tự đổi nên không thể nhầm lẫn giữa hai dataset khác nhau. Thời điểm tạo (`created_at`) chỉ nằm trong `manifest.json`, không tham gia vào phép băm.

**Hai lần chạy liên tiếp:** `step 4` và `cli export-dataset` chạy lại đều cho `dataset_version = d4cc4614711a` (1019 khung, train 763 / val 101 / test 155, 18/4/4 trip), nên version không đổi dù giờ chạy khác nhau.

**Vì sao khác bạn bè dù cùng dữ liệu:** theo SQL của gold, thứ tự xếp trip trong mỗi drive là `md5(trip_id || LAB_STUDENT_ID)`, tức MSSV (`2A202602412`) được trộn vào làm "muối". MSSV khác thì thứ tự khác, nên các trip rơi vào train/val/test khác nhau, danh sách `frame_id|split` khác nhau, và băm ra version khác. Cùng một dữ liệu gốc nhưng mỗi người có một cách chia hợp lệ riêng, nên không chép được kết quả của nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Dataset `d4cc4614711a` (1019 khung: drive_a 436, drive_b 583) được tạo từ dữ liệu sau, đọc trực tiếp từ `lineage` trong `manifest.json`:

| Nguồn | Phiên bản Delta |
|---|---|
| `bronze.raw_telemetry_a` | 0 |
| `bronze.raw_telemetry_b` | 0 |
| `bronze.trips_raw` | 0 |
| `bronze.video_files` | 1 |
| `silver.frames` | 27 |
| `silver.telemetry` | 0 |
| `silver.frames_flagged` | 0 |
| `silver.frame_telemetry` | 0 |

Video đầu vào được xác định bằng sha256:
- `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
- `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`

Hai giá trị này khớp với cột `sha256` trong `bronze.video_files` mà mình đã truy vấn ở bước 1, nên dataset đúng là sinh từ hai file video đó chứ không phải bản khác. Một số phiên bản đọc được từ bảng: `bronze.video_files` ở version 1 vì có hai lần ghi (mỗi drive một lần nạp), còn `silver.frames` ở version 27 vì mỗi trip ghi một partition riêng (26 trip) cộng thêm hai lần chạy lại `trip-00003` của thí nghiệm idempotent.

**Nếu 3 tháng sau có người hỏi, thông tin giúp tái tạo đúng dataset này:**
1. `dataset_version` `d4cc4614711a` và `manifest_sha256` để đối chiếu và xác nhận kết quả khớp.
2. Phiên bản của từng bảng trong `lineage`: Delta cho phép đọc lại bảng ở đúng phiên bản đó (time travel), nên lấy lại được dữ liệu y hệt lúc xuất.
3. sha256 của hai video gốc, chứng minh đầu vào không bị thay.
4. Luật trong manifest: loại các khung `is_black`, `is_blurry`, `is_near_dup` và khung không đồng bộ (`not sync_ok`); chia split theo trip, phân tầng theo drive, thứ tự md5.
5. Các tham số trong `dbt_project.yml` (`dark_luma`, `blur_var`, `dup_hamming` = 2, `sync_tol_ms` = 150, `split_train` / `split_val`) và **MSSV** dùng làm muối chia split, cùng commit Git của mã pipeline.

Thiếu bất kỳ mục nào ở trên (nhất là MSSV hoặc phiên bản bảng) thì không dựng lại được cùng một dataset.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

**Khi sập:** đúng **1 task** `silver_frames` (bản của trip-04237, trong 26 bản) chuyển **đỏ**. Log ghi `RuntimeError: simulated crash while extracting trip-04237; run it again`. **7 task** phía sau (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) chuyển `upstream_failed`: Airflow không chạy chúng vì task cha hỏng, nên dữ liệu dở dang không đi tiếp xuống gold.

**Cách chạy lại:** mình chỉ **clear đúng task đỏ** (bản trip-04237, kèm các task phía sau theo mặc định), không clear cả DAG hay cả run. 25 trip đã thành công được giữ nguyên, chỉ trip hỏng chạy lại (biến sập chỉ sập đúng một lần) rồi 7 task sau chạy tiếp. DAG chuyển xanh.

**Kết quả kiểm tra:** `scripts/lab.sh verify` sau khi chạy lại đạt **27/27**; `silver.frames` vẫn có 1246 khung với `frame_id` duy nhất, nên **không nhân đôi** dữ liệu; `dataset_version` vẫn là **`d4cc4614711a`** (không đổi so với trước khi sập). Lý do: bước `silver_frames` ghi bằng Delta `overwrite` theo partition từng trip (idempotent, xem Câu 4), còn `dataset_version` là băm nội dung (xem Câu 7), nên chạy lại cùng dữ liệu cho đúng cùng kết quả.

**Bằng chứng:** `report/screenshots/airflow_fail_log.png` (log task đỏ, thấy trip-04237), `airflow_graph_failed.png` (Graph lúc đỏ), `airflow_graph_after_retry.png` (Graph xanh sau khi clear), `report/verify.txt`.
### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Với 500 xe mỗi ngày (mỗi xe nhiều trip, video và telemetry liên tục), ba thành phần mình thấy sẽ vỡ đầu tiên:

| Thành phần hiện tại | Vì sao vỡ | Thay bằng |
|---|---|---|
| **Một container Airflow chạy `silver_frames` bằng lệnh bash và FFmpeg decode ngay trong container** | Mỗi trip một task, hiện 26 task đã đủ làm máy chậm. Hàng nghìn trip mỗi ngày sẽ hết CPU/RAM trên một máy, task bị kill | Chạy task trên cụm phân tán (Kubernetes Executor hoặc Celery), decode video bằng worker/GPU tách riêng, xử lý song song theo partition |
| **dbt chạy trên DuckDB một máy** | DuckDB và file `.duckdb` nằm trên một node, không scale ngang; bảng khung hình lên hàng chục triệu dòng | Engine phân tán hoặc kho dữ liệu đám mây (Spark/Trino trên Delta, hoặc BigQuery/Snowflake), dbt vẫn giữ làm lớp biến đổi |
| **MinIO đơn nút + Postgres làm warehouse và Metabase đọc trực tiếp** | Hàng triệu file ảnh nhỏ làm object store và phần ghi/đọc thành nút cổ chai; Postgres không phù hợp cho mart rất lớn | Object storage đám mây (S3) có phân vùng và nén file nhỏ, warehouse dạng cột (ClickHouse, BigQuery) cho dashboard |

Ngoài ba điểm trên, telemetry hiện nạp theo lô từ CSV, với 500 xe mỗi ngày nên chuyển sang luồng liên tục (Kafka) và chỉ nạp gia tăng thay vì quét lại. 

## Câu hỏi riêng của bạn trên Metabase

**Câu hỏi:** Tỷ lệ khung bị loại (reject rate) của từng trip là bao nhiêu, và trip nào bị loại nhiều nhất?

**SQL** (chạy trên `marts.mart_trip_summary`):

​```sql
SELECT
  trip_id,
  frames,
  frames_kept,
  frames - frames_kept AS frames_rejected,
  ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS reject_pct
FROM marts.mart_trip_summary
ORDER BY reject_pct DESC;
​```

**Kết quả:** Truy vấn trả về 26 trip. Năm trip có tỷ lệ loại cao nhất là
trip-04622 (56,67%, loại 34/60 khung), trip-03904 (50%, 11/22),
trip-02086 (46,67%), trip-08888 (40%) và trip-07774 (38,24%).
Trip thấp nhất là trip-04324 với 0%; trip của mình, trip-04237, có tỷ lệ 1,67%.

**Nhận xét:**
- Tỷ lệ loại giữa các trip chênh nhau lớn, và trip tệ nhất bị loại hơn một nửa số khung,
  nên chất lượng dữ liệu đầu vào không đồng đều giữa các trip.
- trip-03904 chỉ có 22 khung nên con số 50% dựa trên mẫu nhỏ, cần thận trọng khi so với
  các trip có 60 khung.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

**Điều mình học được:**
Ở bước Metabase, mình mất khá nhiều thời gian vì chạy SQL nhầm trên "Sample Database" thay vì database `warehouse`. Lỗi `no such table` và `relation does not exist` khác nhau tuỳ database đang chọn, nên mình rút ra là đọc kỹ thông báo lỗi và kiểm tra nguồn dữ liệu trước khi nghi ngờ câu SQL. Mình cũng nhận ra cần dùng tên đầy đủ `marts.mart_trip_summary`.

**Điều làm mình bất ngờ:**
Tỷ lệ khung bị loại giữa các trip chênh nhau rất lớn: trip cao nhất (trip-04622) bị loại 56,67% số khung, trong khi trip-04324 thấp nhất chỉ 0% (trip của mình là trip-04237 bị loại 1,67%). Điều này cho thấy chất lượng thu thập thực tế phụ thuộc rất mạnh vào điều kiện ngoại cảnh từng chuyến (rung lắc, hầm tối, xe đứng yên gây trùng lặp) chứ không phân bổ đồng đều giữa các xe.

**Điều còn chưa rõ & Khâu khó nhất:**
Khâu khó nhất với mình là debug sự phụ thuộc giữa các task trong Airflow DAG khi có lỗi phát sinh (clear task con nhưng cần nắm rõ lineage để tránh re-run thừa). 

**Lỗi dữ liệu pipeline chưa bắt được:**
Pipeline lọc tốt khung đen, mờ, trùng lặp và lệch mốc đồng bộ (sync tolerance), nhưng chưa bắt được lỗi "đóng băng cảm biến" (freeze sensor) khi telemetry vẫn gửi đều đúng timestamp nhưng các giá trị vận tốc/gia tốc bị kẹt cứng một số không đổi dù xe đang di chuyển.

**Nếu làm lại, mình sẽ:**
Chủ động kiểm tra kết nối DB và cấu trúc schema ngay từ đầu

**Hạn chế của bài làm:**
Chưa đào sâu kiểm tra nguyên nhân cụ thể từng khung bị loại ở các trip có reject rate cao bất thường (như trip-04622); cỡ mẫu một số trip quá nhỏ (như trip-03904 chỉ có 22 khung) khiến tỷ lệ phần trăm dễ bị lệch và chưa hoàn toàn phản ánh độ ổn định lâu dài.