# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Đồng Mạnh Hùng` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602412` |
| GitHub | `https://github.com/Hung23020370/K4-L3L4-Track4-DataPipeline4ComputerVision` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
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

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

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

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
