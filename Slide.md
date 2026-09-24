# Slide.md — Cấu trúc slide cho phần Practice: Dagster Orchestration trên nền dbt + Pydantic

Tài liệu này là **outline/script cho slide**, không phải slide đã dựng — mỗi mục dưới đây tương ứng
1 slide, theo đúng "giọng" của 2 tài liệu tham khảo (dbt course + Basic Data Pipeline with Pydantic
and dbt): tiêu đề khẳng định 1 ý, diagram/bảng, code box thật, và "Điểm cần nhớ" ở cuối khi cần.

Khác với 2 tài liệu tham khảo (dbt thuần, Pydantic+dbt+Postgres+BigQuery không có orchestration),
deck này **ưu tiên trình bày từ Dagster trước** — vì đó là phần khác biệt chính của project so với
2 tài liệu đã có, không phải lặp lại nội dung staging/intermediate/marts đã có sẵn trong tài liệu 2.

---

## Phần A — Vì sao cần Dagster (bối cảnh, đối chiếu với pipeline nền)

### Slide 1 — Title
- **Dagster Orchestration cho Data Pipeline with dbt + Pydantic**
- Subtitle: Từ `make pipeline` thủ công đến asset graph có retry, alert, run history
- Tác giả / ngày / logo (theo template hiện có)

### Slide 2 — Outline
➢ Vì sao cần Dagster (đối chiếu pipeline nền)
➢ Kiến trúc tổng thể có Dagster
➢ Ingestion layer: partitioned vs dimension assets
➢ dbt trong Dagster: 2 nhóm asset, rolling-window incremental
➢ Quality gates: Asset Checks + Retry Policy
➢ Alerting: run_failure_sensor + HTML email
➢ Run metadata: thay thế `ingest_errors.csv`
➢ 5 bug thật đã gặp và sửa
➢ BI: Metabase trên marts
➢ Demo & Q&A

### Slide 3 — Pipeline nền (đã có ở tài liệu Pydantic+dbt)
> Nhắc lại nhanh 1 slide — không đi sâu vì đã có tài liệu riêng.

```
CSV (13 files) → ingest.py (Pydantic validate) → PostgreSQL (raw.*) → dbt → BigQuery (marts)
```
- Chạy bằng `make pipeline` = `ingest && dbt run && dbt test` — 2 lệnh nối `&&`, không hơn.
- Mỗi lần chạy: `DROP SCHEMA raw CASCADE`, nạp lại **toàn bộ** 13 CSV, rebuild **toàn bộ** dbt models.
- Đã fix sạch data-quality ở tầng Pydantic (0 dòng bị reject / 2.960.188 dòng, xem tài liệu 2).

### Slide 4 — 5 vấn đề pipeline nền vẫn còn (đo được thật, không phải giả định)

| # | Vấn đề | Bằng chứng cụ thể |
|---|---|---|
| 1 | Không có dependency graph / phục hồi từng phần | Lỗi giữa chừng → `DROP SCHEMA CASCADE` → chạy lại từ đầu |
| 2 | Không có retry | Mất kết nối Postgres → script chết ngay, không backoff |
| 3 | Lỗi dữ liệu âm thầm | `raw_sales` từng **0/3.833 dòng**, `dbt test` vẫn **pass** (bảng rỗng → test trivially đúng) |
| 4 | Không có run history | `ingest_errors.csv` bị **ghi đè** mỗi lần chạy — không so được hôm nay vs hôm qua |
| 5 | Bug logic ẩn trong `mart_daily_sales_features` | `daily_returns` group theo `order_date`, nhưng return trễ 5–31 ngày (median 18d, p90 26d) so với đơn — full-refresh "vô tình đúng", sẽ sai nếu chuyển incremental |

*(Đây chính là 5 vấn đề đã liệt kê trong `doc.txt` — dùng lại ở đây làm cầu nối sang phần Dagster.)*

### Slide 5 — Dagster giải quyết từng vấn đề như thế nào

| Vấn đề | Dagster fix | Ở đâu trong code |
|---|---|---|
| Không dependency graph | Asset graph 36 asset, re-run đúng asset lỗi | `defs/assets_ingest_*.py`, `defs/dbt/` |
| Không retry | `RetryPolicy` khai báo trên từng asset | `retry_policy=` param |
| Lỗi âm thầm | Asset Checks: `row_count`, `reject_rate` tính từ **run thật** | `defs/checks_ingest.py` |
| Không run history | Metadata `rejected_row_samples` theo từng partition, giữ mãi trong instance | `MaterializeResult.metadata` |
| Bug rolling-window | Incremental thật + cửa sổ trượt 45 ngày | `defs/dbt_incremental.py` |

---

## Phần B — Kiến trúc tổng thể (bắt đầu từ Dagster)

### Slide 6 — Kiến trúc pipeline có Dagster

```
[Baseline — ngoài Dagster, chạy 1 lần]
  python -m data_pipeline.ingest --start-date 2013-01-01 --end-date 2013-02-28
  → raw.* (13 bảng, DROP SCHEMA CASCADE + reload — giống hệt pipeline nền)

[Dagster — tiếp quản từ 2013-03-01, daily partition]

  Ingestion (13 asset, nhóm "raw"):
    9 asset partitioned theo ngày (nhóm "ingestion")
    4 asset dimension, full-reload (nhóm "ingestion_dimensions")

  dbt (23 asset, sinh từ manifest.json):
    20 unpartitioned  → DbtProjectComponent (khai báo YAML thuần)
     3 partitioned    → @dbt_assets viết tay (rolling-window incremental)

  Asset Checks: row_count>0 (dimension) · reject_rate<20% (13 asset ingestion)
  Retries: 3 lần (ingestion) · 2 lần (dbt incremental) — exponential backoff
  Alerts: run_failure_sensor → email HTML qua Gmail SMTP
```

- **Vì sao có baseline ngoài Dagster?** Để demo rõ ràng "trước/sau khi có Dagster" — đúng tinh
  thần Slide 3, không phải backfill toàn bộ 10 năm qua Dagster ngay từ đầu.
- **Vì sao range bắt đầu 2013-01-01** (không phải 2012-07-04 như data thật)?
  `web_traffic.csv` chỉ có dữ liệu từ 2013-01-01 — bắt đầu từ đó để không bảng nào cần đặc cách
  "0 dòng hôm nay là đúng, không phải lỗi".

### Slide 7 — Mono-repo, không phải 2 repo tách rời

```
data-pipeline-dagster/
  dbt_project/                 dbt project (copy từ repo gốc)
  packages/data_pipeline/      Pydantic models + ingest.py (copy, dùng cho baseline)
  src/data_pipeline_dagster/
    definitions.py              @definitions + load_from_defs_folder
    defs/                       toàn bộ asset/check/schedule/sensor (auto-discover)
  scripts/split_dataset_by_date.py
```
- Lần đầu: project Dagster **tham chiếu** repo dbt+pydantic qua path dependency.
- Sau đó **chuyển hẳn thành mono-repo** — copy `dbt_project/` + package `data_pipeline` vào
  trong, `[tool.uv.sources]` trỏ nội bộ. Repo gốc giữ nguyên, vẫn chạy độc lập được.
- Lý do: path dependency xuyên 2 repo giả định cấu trúc thư mục cố định, không có ranh giới
  version rõ ràng — không phù hợp để deploy.

---

## Phần C — Ingestion layer: partitioned vs dimension

### Slide 8 — 13 bảng, 2 kiểu asset (fact vs dimension)

```
raw_customers        raw_orders           raw_shipments
raw_returns           raw_reviews          raw_web_traffic
raw_sales             raw_order_items*     raw_payments*
                                            (* suy ra ngày qua order_id → orders.order_date)
→ Partitioned theo ngày (DailyPartitionsDefinition), group "ingestion"

raw_products   raw_geography   raw_promotions   raw_inventory
→ Full-reload mỗi lần, group "ingestion_dimensions" (không có "grain theo ngày" hợp lý)
```
- **Điểm cần nhớ**: bug thật đã gặp — `AssetSelection.groups("ingestion")` quên nhóm
  `"ingestion_dimensions"` → 8 asset (`raw_products`, `stg_products`...) không bao giờ được
  build bởi job hàng ngày, không hề báo lỗi (xem Slide 16, mục #1).

### Slide 9 — Code: 1 asset ingestion thật (rút gọn)

```python
@dg.asset(
    name=f"raw_{table_name}",
    key_prefix="raw",
    partitions_def=daily_partitions,
    group_name="ingestion",
    retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
)
def _asset(context, postgres: PostgresResource) -> dg.MaterializeResult:
    partition_date = context.partition_key
    stats = ingest_daily_partition(table_name, postgres.get_engine(), partition_date)
    return dg.MaterializeResult(metadata={
        "rows_read": stats["rows_read"],
        "reject_rate": dg.MetadataValue.float(round(stats["reject_rate"], 4)),
        "rejected_row_samples": dg.MetadataValue.md(error_samples_markdown(stats["sample_errors"])),
    })
```
- `ingest_daily_partition()` **tái dùng** `MODEL_MAP`/`TABLE_NAME_MAP` từ `data_pipeline.ingest`
  gốc — không viết lại rule validate, không lệch giữa 2 pipeline.
- `DELETE ... WHERE <date_col> = partition_date` rồi `INSERT` — **idempotent theo partition**,
  khác hẳn `DROP SCHEMA CASCADE` toàn bộ của pipeline nền.

### Slide 10 — Vì sao cần pre-split CSV theo ngày?

- Naive: filter CSV gốc theo ngày trên **mỗi lần chạy partition**.
- `order_items.csv` có 714k dòng × ~3.700 partition ngày (nếu backfill toàn bộ) = quét lại toàn
  file hàng nghìn lần → không khả thi.
- `scripts/split_dataset_by_date.py` chạy **1 lần**, tách mỗi bảng thành
  `dataset/daily/<table>/<date>.csv` — mỗi partition chỉ đọc 1 file nhỏ.

---

## Phần D — dbt bên trong Dagster

### Slide 11: Why the dbt layer is split into two asset groups

Standard setup: one YAML component turns every dbt model into an asset and runs `dbt build`.
It always runs the same command, so it cannot tell dbt which day to process.

| Group | Declared with | Why |
|---|---|---|
| 20 unpartitioned models | `DbtProjectComponent` (YAML only) | Rebuild everything, no date needed |
| 3 incremental models (`int_daily_metrics`, `int_return_analysis`, `mart_daily_sales_features`) | Hand-written `@dbt_assets` (Python) | SQL filters on `var("run_date")`, which must come from the Dagster partition |

(code block unchanged)

- `context.partition_key` → `dbt build --vars '{"run_date": ...}'`: the partition date becomes the dbt variable.
- Asset keys from `source('raw', 'raw_orders')` automatically match the ingestion assets' `raw/raw_orders`,
  so Dagster links ingestion → dbt without a custom `DagsterDbtTranslator`.

### Slide 12 — Sửa đúng bug rolling-window đã nêu ở Slide 4 (#5)

```sql
-- mart_daily_sales_features.sql
{% if is_incremental() %}
where date >= '{{ var("run_date") }}'::date - interval '45 days'
  and date <= '{{ var("run_date") }}'::date
{% endif %}
```
- 45 ngày = biên an toàn trên p90 lag đo thật (26 ngày).
- **Verify thật** qua Dagster run: `int_daily_metrics` → `INSERT 0 1` (đúng 1 ngày),
  `mart_daily_sales_features` → `INSERT 0 46` (45 ngày trước + hôm nay) — khớp thiết kế.
- `int_daily_metrics`/`int_return_analysis` cũng incremental nhưng vì **hiệu năng** (tránh quét
  lại 10 năm dữ liệu mỗi partition khi backfill), không phải vì cần rolling-window.

---

## Phần E — Quality gates: từ 3 tầng (tài liệu 2) lên 4 tầng có Dagster

### Slide 13 — Tầng thứ 4: Asset Checks (Dagster nhìn thấy cả run thật, không chỉ bảng)

| Tầng | Kiểm tra gì | Biết gì |
|---|---|---|
| 1. Pydantic | Kiểu, miền giá trị, quy tắc liên cột | Mức bản ghi |
| 2. PostgreSQL | `NOT NULL`, `NUMERIC(15,2)` | Mức lưu trữ |
| 3. dbt tests | `unique`, `not_null`, `relationships` | Mức tập dữ liệu sau transform |
| **4. Dagster Asset Checks** | `row_count > 0` (dimension), `reject_rate < 20%` (13 asset) | **Mức run** — biết chính xác partition/asset nào, không chỉ "bảng nào" |

- Khác biệt then chốt: `reject_rate` tính từ `rows_read`/`rows_rejected` **của đúng partition vừa
  chạy** (đọc từ `context.instance.get_latest_materialization_event`), không phải scan cả bảng.
- Bug thật đã gặp: `row_count > 0` áp cho asset partitioned → false positive ngày `web_traffic`
  hợp lệ có 0 dòng (trước 2013-01-01) — xem Slide 16, mục #3.

### Slide 14 — Retry Policy: demo bằng deliberate failure thật

```python
retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL)
```
- Test thật: cố tình đổi `connection_url` sai qua `--config-json` (không sửa file nào), quan sát
  event log:
```
STEP_UP_FOR_RETRY raw__raw_orders
STEP_RESTARTED    raw__raw_orders
STEP_UP_FOR_RETRY raw__raw_orders
STEP_RESTARTED    raw__raw_orders
STEP_UP_FOR_RETRY raw__raw_orders
STEP_RESTARTED    raw__raw_orders
STEP_FAILURE      raw__raw_orders
```
- Đúng 3 lần retry rồi mới fail hẳn — khớp `max_retries=3`.
- **Giới hạn cần nói rõ**: 20 model dbt qua `DbtProjectComponent` (YAML) **không có** retry —
  `OpSpec` của component này không có field `retry_policy`.

---

## Phần F — Alerting & Run metadata

### Slide 15 — `run_failure_sensor` + email HTML thật (không phải template mặc định)

```python
return dg.make_email_on_run_failure_sensor(
    email_from=GMAIL_USER, email_password=GMAIL_APP_PASSWORD, email_to=[ALERT_EMAIL_TO],
    email_body_fn=_failure_email_body, email_subject_fn=_failure_email_subject,
    name="failure_email_alert", monitor_all_code_locations=True,
)
```
- Nội dung email: job/run/partition, **bảng lỗi từng step** (`context.get_step_failure_events()`),
  nút "View run in Dagster UI" link thẳng về `http://localhost:3000/runs/<run_id>`.
- Sensor cần **daemon** (`dagster-daemon run`) đang chạy để tự poll — CLI launch không tự kích
  hoạt sensor.
- Verify thật: daemon log ghi `Sensor "failure_email_alert" acted on run status FAILURE` — email
  đã nhận thật trong hộp thư.

### Slide 16 — `rejected_row_samples`: thay thế hẳn `ingest_errors.csv`

| | `ingest_errors.csv` (pipeline nền) | `rejected_row_samples` (Dagster) |
|---|---|---|
| Phạm vi | Toàn pipeline, 1 file | Từng asset + từng partition riêng |
| Lịch sử | Ghi đè mỗi lần chạy | Giữ mãi trong run history |
| Nội dung | `str(error)` thô | row/field/type/message có cấu trúc, hiện thành bảng markdown trên UI |
| Giới hạn | Không giới hạn (nhưng mất sau lần chạy sau) | Tối đa 25 mẫu/partition (đủ chẩn đoán, tránh phình metadata) |

```python
sample_errors.append({"row": rownum, "field": field, "type": first["type"], "msg": first["msg"]})
...
"rejected_row_samples": dg.MetadataValue.md(error_samples_markdown(stats["sample_errors"])),
```

---

## Phần G — 5 bug thật đã gặp khi build (institutional memory)

### Slide 17 — Bug list (mỗi bug: nguyên nhân → cách phát hiện → cách sửa)

1. **`schedules.py` thiếu group `"ingestion_dimensions"`** — 8 asset dimension không bao giờ
   được build bởi job hàng ngày, không báo lỗi (chỉ phát hiện khi soi asset graph resolve ra thiếu
   gì). Fix: `AssetSelection.groups("ingestion", "ingestion_dimensions")`.
2. **`dbt_utils` không dùng vẫn còn khai báo** → nhiều subprocess Dagster tranh nhau
   `dbt deps` trên cùng 1 lock file → timeout. Fix: xoá hẳn `packages.yml`/`dbt_packages/`.
3. **`row_count > 0` false positive** trên asset partitioned — `web_traffic` trước 2013-01-01
   hợp lệ có 0 dòng. Fix: check này chỉ áp cho 4 asset dimension, dùng `reject_rate` cho asset
   partitioned.
4. **`.env` không được load trong `sensors.py`** — thiếu `load_dotenv()`, sensor âm thầm chạy ở
   nhánh "disabled" dù `.env` đã điền thật. Fix: `load_dotenv(Path(__file__).parents[3] / ".env")`.
5. **`DAGSTER_HOME` chưa set + 2 daemon xung đột** — mỗi `dg dev` tạo `.tmp_dagster_home_*` mới,
   run history không thấy nhau giữa các terminal; sau khi set `DAGSTER_HOME`, 1 daemon test cũ
   (`kill <bash-job-pid>` không dừng đúng process Windows) vẫn chạy song song gây
   `"Another daemon is still sending heartbeats"`. Fix: `Stop-Process -Name dagster-daemon -Force`.

*(Chi tiết đầy đủ + lệnh tái hiện từng bug: xem `IMPROVEMENTS.md` §7 trong repo.)*

---

## Phần H — BI & Demo

### Slide 18 — Visualizing the marts (Metabase, không phải BigQuery)

- Khác tài liệu 2 (publish thủ công lên BigQuery), project này **thêm BI trực tiếp trên Postgres**
  bằng Metabase (Docker, port 3001 — 3000 đã là Dagster UI).
- Kết nối: host `localhost`, port `5432`, db `data_pipeline`, schema `marts` →
  `mart_customer_features`, `mart_order_features`, `mart_product_features`,
  `mart_daily_sales_features` — dựng dashboard trực tiếp, không cần bước export CSV.

### Slide 19 — Lệnh chạy demo chính

```bash
uv sync
uv run python scripts/split_dataset_by_date.py
uv run python -m data_pipeline.ingest --start-date 2013-01-01 --end-date 2013-02-28   # baseline
uv run dbt build --project-dir dbt_project --profiles-dir dbt_project                 # build lần đầu
uv run dg dev                                                                          # UI :3000
uv run dg launch --assets "raw/raw_orders,raw/raw_sales" --partition "2013-03-05"      # materialize 1 ngày
docker start data-pipeline-pg data-pipeline-metabase                                   # BI :3001
```

### Slide 20 — Summary

| Vấn đề pipeline nền | Giải pháp Dagster | Verify thật |
|---|---|---|
| Không dependency graph | Asset graph 36 asset | Re-run đúng asset lỗi, không cần `DROP CASCADE` |
| Không retry | `RetryPolicy` | `STEP_UP_FOR_RETRY` ×3 quan sát được trong log |
| Lỗi âm thầm | Asset Checks (`reject_rate`, `row_count`) | Check fail đúng lúc, đúng asset |
| Không run history | `rejected_row_samples` metadata | Giữ theo từng partition, không bị ghi đè |
| Bug rolling-window | Incremental 45-day window | `INSERT 0 46` khớp thiết kế |
| Không alert | `run_failure_sensor` + HTML email | Email thật đã nhận, có link về run |

### Slide 21 — Q&A

---

## Ghi chú khi dựng slide thật (không phải nội dung trình bày)

- Slide 3–5, 11–12 nên **dẫn lại** hình/bảng đã có sẵn trong 2 tài liệu tham khảo (Hình 2, Bảng 2
  của `data_pipeline.pdf`) thay vì vẽ lại, để không lặp công.
- Slide 8, 13 hợp để vẽ dạng ERD/box giống style Hình 7 (`data_pipeline.pdf`) hoặc "Lược đồ dữ
  liệu cốt lõi" (trang 40, dbt slide) — liệt kê asset + group + join key.
- Code box nên giữ **đúng nguyên văn** từ repo (`defs/assets_ingest_partitioned.py`,
  `defs/dbt_incremental.py`, `defs/sensors.py`, `defs/checks_ingest.py`) — không viết lại/tối giản
  thêm, để khớp với những gì demo trực tiếp trên máy lúc trình bày.
- Toàn bộ số liệu (`INSERT 0 46`, `0/3.833 dòng`, `STEP_UP_FOR_RETRY ×3`...) đều là kết quả chạy
  thật trong quá trình build project này — nên giữ nguyên khi trình bày, không làm tròn/đổi số.
