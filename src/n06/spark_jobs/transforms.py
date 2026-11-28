"""Biến đổi DataFrame của `process-day`: Bronze -> Silver -> đối soát MATCHED -> Gold.

Chỉ dùng Spark SQL functions (không Python UDF, đặc tả I.3). Mọi hàm thuần theo DataFrame đầu vào;
không đọc đồng hồ (`as_of_time` truyền vào) và không đọc ground truth.

Phạm vi T25 (docs/TASKS.md): dedup cơ bản, không quarantine (T30), không `key_conflicts` (T31),
chỉ trạng thái MATCHED (T33). Bản ghi mà T30/T31 sẽ xử lý được ĐẾM và làm job thất bại thay vì bị
bỏ im lặng: xem `n06.spark_jobs.process_day_job`.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from n06.spark_jobs.plan import RECON_RULE_VERSION
from n06.spark_jobs.receipts import Receipt

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"
RECORD_SEPARATOR = "\x1f"
CHANNELS = ("SIM_CHANNEL_A", "SIM_CHANNEL_B")
CURRENCIES = ("VND", "USD")
STATUSES = ("SUCCESS", "FAILED")
SCHEMA_VERSION_VALUE = "1.0.0"
MAX_AMOUNT_MINOR = 1_000_000_000_000
MAX_OPENING_BALANCE_MINOR = 9_000_000_000_000_000

TRANSACTION_PAYLOAD = (
    "external_reference",
    "account_from",
    "account_to",
    "amount_minor",
    "currency",
    "status",
    "transaction_type",
    "channel",
    "event_time",
    "business_date",
)
LEDGER_PAYLOAD = (
    "transaction_id",
    "ledger_account_id",
    "side",
    "amount_minor",
    "currency",
    "event_time",
    "business_date",
)
STATEMENT_PAYLOAD = (
    "external_reference",
    "channel",
    "amount_minor",
    "currency",
    "status",
    "event_time",
    "business_date",
    "available_at",
)
STATEMENT_SCHEMA = StructType(
    [
        StructField(name, LongType() if name == "amount_minor" else StringType(), True)
        for name in (
            "schema_version",
            "dataset_id",
            "statement_record_id",
            "external_reference",
            "channel",
            "amount_minor",
            "currency",
            "status",
            "event_time",
            "business_date",
            "available_at",
        )
    ]
)
ACCOUNT_SCHEMA = StructType(
    [
        StructField(name, LongType() if name == "opening_balance_minor" else StringType(), True)
        for name in (
            "schema_version",
            "dataset_id",
            "account_id",
            "currency",
            "opening_balance_minor",
            "opening_business_date",
            "account_type",
        )
    ]
)


def _text(columns: Sequence[str]) -> Column:
    return F.concat_ws(RECORD_SEPARATOR, *[F.col(name).cast("string") for name in columns])


def _not_in(name: str, values: Sequence[str]) -> Column:
    """Đúng khi cột NULL hoặc ngoài tập giá trị (so sánh với NULL không được làm mất dòng lỗi)."""
    return F.col(name).isNull() | ~F.col(name).isin(*values)


def _differs(name: str, value: str) -> Column:
    return F.col(name).isNull() | (F.col(name) != value)


def pointer_column() -> Column:
    """Source pointer Kafka `<topic>:<partition>:<offset>` (ADR-0004b)."""
    return F.concat_ws(
        ":", F.col("topic"), F.col("partition").cast("string"), F.col("offset").cast("string")
    )


def business_date_mismatch(event_time: str, business_date: str, timezone: str) -> Column:
    """Đúng khi `business_date` không phải ngày theo múi giờ nghiệp vụ của `event_time`."""
    local = F.to_date(F.from_utc_timestamp(F.col(event_time), timezone))
    return local.isNull() | F.col(business_date).isNull() | (local != F.col(business_date))


def _typed(df: DataFrame) -> DataFrame:
    """Chuỗi hợp đồng -> `timestamp` UTC và `date`; chuỗi sai định dạng trở thành NULL."""
    return df.withColumn("event_time", F.to_timestamp(F.col("event_time"), TS_FORMAT)).withColumn(
        "business_date", F.to_date(F.col("business_date"), "yyyy-MM-dd")
    )


def dedup_event_bronze(
    bronze: DataFrame, *, dataset_id: str, key: str, payload: Sequence[str]
) -> tuple[DataFrame, DataFrame]:
    """Gộp bản trùng theo khóa nghiệp vụ (ADR-0004b 3.1/3.2). Trả `(đã gộp, bản chưa gộp)`.

    Cột `variants` > 1 nghĩa là cùng khóa khác nội dung: xung đột, thuộc T31; T25 không chọn.
    """
    # Lấy cả bản ghi mà envelope hoặc payload nêu dataset này để bản lệch nhau lộ ra ở
    # `envelope_mismatch` thay vì bị lọc mất im lặng.
    source = (
        bronze.where(
            (F.col("dataset_id") == dataset_id) | (F.col("envelope_dataset_id") == dataset_id)
        )
        .withColumn("payload_hash", F.sha2(_text(payload), 256))
        .withColumn("event_hash", F.sha2(_text([key, *payload]), 256))
        .withColumn("pointer", pointer_column())
    )
    grouped = source.groupBy("dataset_id", key).agg(
        F.countDistinct("payload_hash").alias("variants"),
        *[F.first(name).alias(name) for name in payload if name != key],
        F.min("event_id").alias("first_event_id"),
        F.sort_array(F.collect_set("event_id")).alias("event_ids"),
        F.count(F.lit(1)).cast("int").alias("replay_count"),
        F.sort_array(F.collect_list("pointer")).alias("source_pointers"),
        F.min("kafka_timestamp").alias("first_kafka_timestamp"),
        F.min("ingested_at").alias("first_ingested_at"),
    )
    return grouped, source


def event_id_conflicts(source: DataFrame) -> DataFrame:
    """`event_id` gắn với nhiều nội dung hoặc nhiều khóa nghiệp vụ (EVENT_ID_CONFLICT, T31)."""
    return (
        source.groupBy("event_id")
        .agg(F.countDistinct("event_hash").alias("variants"))
        .where(F.col("variants") > 1)
    )


def envelope_mismatch(source: DataFrame, dataset_id: str) -> DataFrame:
    """Bản ghi mà envelope và payload không cùng dataset hoặc cùng `event_time` (spec S891)."""
    return source.where(
        _differs("envelope_dataset_id", dataset_id)
        | _differs("dataset_id", dataset_id)
        | (F.col("envelope_event_time") != F.col("event_time"))
        | F.col("envelope_event_time").isNull()
    )


def silver_transactions(grouped: DataFrame, *, run_id: str) -> DataFrame:
    typed = _typed(grouped.drop("variants"))
    return typed.select(
        "dataset_id",
        "transaction_id",
        "external_reference",
        "channel",
        "account_from",
        "account_to",
        "amount_minor",
        "currency",
        "status",
        "transaction_type",
        "event_time",
        "business_date",
        "first_event_id",
        "event_ids",
        "replay_count",
        "source_pointers",
        "first_kafka_timestamp",
        "first_ingested_at",
        F.lit(run_id).alias("run_id"),
    )


def silver_ledger_entries(grouped: DataFrame, *, run_id: str) -> DataFrame:
    typed = _typed(grouped.drop("variants"))
    return typed.select(
        "dataset_id",
        "entry_id",
        "transaction_id",
        "ledger_account_id",
        "side",
        "amount_minor",
        "currency",
        "event_time",
        "business_date",
        "first_event_id",
        "event_ids",
        "replay_count",
        "source_pointers",
        F.lit(run_id).alias("run_id"),
    )


def invalid_event_rows(silver: DataFrame, timezone: str) -> DataFrame:
    """Dòng Silver vi phạm kiểm tra nghiệp vụ cơ bản mà T30 sẽ đưa vào quarantine."""
    return silver.where(
        F.col("event_time").isNull()
        | F.col("business_date").isNull()
        | business_date_mismatch("event_time", "business_date", timezone)
    )


def file_stats(files: DataFrame) -> DataFrame:
    """`binaryFile` -> `(path, sha256, rows)`; `rows` là số dòng không rỗng (file rỗng có 0)."""
    return files.select(
        "path",
        F.sha2(F.col("content"), 256).alias("sha256"),
        F.size(
            F.filter(F.split(F.decode(F.col("content"), "UTF-8"), "\n"), lambda x: x != "")
        ).alias("rows"),
    )


def _read_lines(files: DataFrame) -> DataFrame:
    """`binaryFile` -> `(path, sha256, idx, line)`; mỗi dòng không rỗng của file một hàng."""
    return (
        files.select(
            "path",
            F.sha2(F.col("content"), 256).alias("sha256"),
            F.split(F.decode(F.col("content"), "UTF-8"), "\n").alias("lines"),
        )
        .select("path", "sha256", F.posexplode("lines").alias("idx", "line"))
        .where(F.col("line") != "")
    )


def read_binary_files(spark: SparkSession, uris: Sequence[str]) -> DataFrame:
    return spark.read.format("binaryFile").load(list(uris))


def statement_files(
    spark: SparkSession, receipts: Sequence[Receipt], uris: dict[str, str]
) -> DataFrame:
    """`binaryFile` của mọi file sao kê có biên nhận; `uris` ánh xạ `file_path` sang URI S3A."""
    return read_binary_files(spark, [uris[r.file_path] for r in receipts])


def silver_statements(
    spark: SparkSession,
    files: DataFrame,
    receipts: Sequence[Receipt],
    uris: dict[str, str],
    *,
    dataset_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Trả `(Silver sao kê đã loại trùng theo statement_record_id, dòng không hợp lệ)`.

    `file_available_at` và `delivery_order` lấy từ biên nhận `_ingest/` (ADR-0006 D3). Nhiều
    `statement_record_id` cho cùng match key được GIỮ (ADR-0004b 3.3) để T33 phát hiện.
    """
    parsed = parse_statement_lines(spark, files, receipts, uris)
    is_invalid = (
        F.col("unparsable")
        | F.col("statement_record_id").isNull()
        | F.col("external_reference").isNull()
        | _differs("schema_version", SCHEMA_VERSION_VALUE)
        | _differs("dataset_id", dataset_id)
        | _not_in("channel", CHANNELS)
        | _not_in("currency", CURRENCIES)
        | _not_in("status", STATUSES)
        | F.col("amount_minor").isNull()
        | (F.col("amount_minor") < 1)
        | (F.col("amount_minor") > MAX_AMOUNT_MINOR)
        | F.to_timestamp(F.col("event_time"), TS_FORMAT).isNull()
        | F.to_timestamp(F.col("available_at"), TS_FORMAT).isNull()
        | F.to_date(F.col("business_date"), "yyyy-MM-dd").isNull()
    )
    flagged = parsed.withColumn("_invalid", F.coalesce(is_invalid, F.lit(True)))
    invalid = flagged.where(F.col("_invalid")).drop("_invalid")
    # Dòng không hợp lệ chỉ đi quarantine, không được vào Silver hay đối soát.
    valid = (
        flagged.where(~F.col("_invalid"))
        .drop("_invalid")
        .withColumn("payload_hash", F.sha2(_text(STATEMENT_PAYLOAD), 256))
    )
    grouped = valid.groupBy("dataset_id", "statement_record_id").agg(
        F.countDistinct("payload_hash").alias("variants"),
        F.min(
            F.struct(
                "delivery_order",
                "source_file",
                "source_line",
                "source_sha256",
                "file_available_at",
                *STATEMENT_PAYLOAD,
            )
        ).alias("m"),
    )
    flat = grouped.select("dataset_id", "statement_record_id", "variants", "m.*")
    return flat, invalid


def parse_statement_lines(
    spark: SparkSession,
    files: DataFrame,
    receipts: Sequence[Receipt],
    uris: dict[str, str],
) -> DataFrame:
    """Mỗi dòng vật lý của các file sao kê có biên nhận, đã tách trường (chưa lọc, chưa gộp)."""
    mapping = spark.createDataFrame(
        [
            (
                uris[r.file_path],
                r.file_path,
                r.delivery_order,
                r.file_available_at.strftime("%Y-%m-%d %H:%M:%S"),
                r.sha256,
            )
            for r in receipts
        ],
        "path string, source_file string, delivery_order int, file_available_at string, "
        "source_sha256 string",
    ).withColumn("file_available_at", F.col("file_available_at").cast("timestamp"))
    return (
        _read_lines(files)
        .join(F.broadcast(mapping), "path")
        .withColumn("j", F.from_json(F.col("line"), STATEMENT_SCHEMA))
        .select(
            "source_file",
            "source_sha256",
            "delivery_order",
            "file_available_at",
            (F.col("idx") + 1).cast("long").alias("source_line"),
            "j.*",
            F.col("j").isNull().alias("unparsable"),
        )
    )


def finish_silver_statements(flat: DataFrame, *, run_id: str) -> DataFrame:
    typed = _typed(flat.drop("variants")).withColumn(
        "available_at", F.to_timestamp(F.col("available_at"), TS_FORMAT)
    )
    return typed.select(
        "dataset_id",
        "statement_record_id",
        "external_reference",
        "channel",
        "amount_minor",
        "currency",
        "status",
        "event_time",
        "business_date",
        "available_at",
        "file_available_at",
        "delivery_order",
        "source_file",
        "source_sha256",
        "source_line",
        F.lit(run_id).alias("run_id"),
    )


def silver_accounts(
    spark: SparkSession, content: DataFrame, *, dataset_id: str, source_file: str, run_id: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Trả `(Silver tài khoản đã loại trùng, dòng không hợp lệ, nhóm có nhiều nội dung)`.

    Cột theo `account.schema.json` của hợp đồng (`opening_business_date`, không có `account_code`
    hay `status`; xem báo cáo T25: lệch với danh sách cột trong ADR-0004b 3.4).
    """
    lines = _read_lines(content).select("line", "idx")
    parsed = lines.withColumn("j", F.from_json(F.col("line"), ACCOUNT_SCHEMA)).select(
        (F.col("idx") + 1).cast("long").alias("source_line"),
        "j.*",
        F.col("j").isNull().alias("unparsable"),
    )
    invalid = parsed.where(
        F.col("unparsable")
        | F.col("account_id").isNull()
        | _differs("schema_version", SCHEMA_VERSION_VALUE)
        | _differs("dataset_id", dataset_id)
        | _differs("account_type", "CUSTOMER_DEPOSIT")
        | _not_in("currency", CURRENCIES)
        | F.col("opening_balance_minor").isNull()
        | (F.col("opening_balance_minor") < 0)
        | (F.col("opening_balance_minor") > MAX_OPENING_BALANCE_MINOR)
        | F.to_date(F.col("opening_business_date"), "yyyy-MM-dd").isNull()
    )
    payload = ("account_type", "currency", "opening_balance_minor", "opening_business_date")
    valid = parsed.where(~F.col("unparsable")).withColumn(
        "payload_hash", F.sha2(_text(payload), 256)
    )
    grouped = valid.groupBy("dataset_id", "account_id").agg(
        F.countDistinct("payload_hash").alias("variants"),
        *[F.first(name).alias(name) for name in payload],
    )
    conflicts = grouped.where(F.col("variants") > 1)
    accounts = grouped.select(
        "dataset_id",
        "account_id",
        "account_type",
        "currency",
        "opening_balance_minor",
        F.to_date(F.col("opening_business_date"), "yyyy-MM-dd").alias("opening_business_date"),
        F.lit(source_file).alias("source_file"),
        F.lit(run_id).alias("run_id"),
    )
    return accounts, invalid, conflicts


def gold_fact_transactions(silver: DataFrame, *, run_id: str) -> DataFrame:
    """`fact_transactions` theo ADR-0005 mục 2.1 (một dòng cho mỗi giao dịch Silver)."""
    return silver.select(
        "dataset_id",
        "transaction_id",
        "business_date",
        F.date_format("business_date", "yyyyMMdd").cast("int").alias("date_key"),
        "external_reference",
        "account_from",
        "account_to",
        "channel",
        "transaction_type",
        "currency",
        "status",
        "amount_minor",
        "event_time",
        "replay_count",
        F.element_at("source_pointers", 1).alias("source_pointer"),
        F.lit(run_id).alias("run_id"),
    )


def _null(type_name: str) -> Column:
    return F.lit(None).cast(type_name)


def reconcile_matched(
    silver_tx: DataFrame,
    silver_statements: DataFrame,
    *,
    as_of_time: datetime,
    run_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Đối soát tối thiểu: chỉ khóa MATCHED (ADR-0006 D2, D5 hàng 5). Trả `(Gold, tổng quan)`.

    Sao kê nhìn thấy khi `file_available_at <= as_of_time` (D3). Khóa là
    `(dataset_id, channel, external_reference)`; mỗi phía phải có đúng một bản ghi độc lập trước
    khi so thuộc tính (D2), nên không có many-to-many. Mọi khóa còn lại (khác thuộc tính, thiếu
    một phía, trùng) KHÔNG được ghi và chỉ được đếm trong `tổng quan`: phân loại thuộc T33.
    `business_date` của khóa là ngày của giao dịch nội bộ (ADR-0006 Q3).
    """
    stamp = as_of_time.strftime("%Y-%m-%d %H:%M:%S")
    keys = ["dataset_id", "channel", "external_reference"]
    internal = silver_tx.groupBy(*keys).agg(
        F.countDistinct("transaction_id").cast("int").alias("internal_record_count"),
        F.min("business_date").alias("business_date"),
        F.min(
            F.struct(
                "transaction_id",
                F.col("currency").alias("currency"),
                F.col("status").alias("status"),
                F.col("amount_minor").alias("amount_minor"),
            )
        ).alias("i"),
        F.sort_array(F.flatten(F.collect_list("source_pointers"))).alias("internal_pointers"),
    )
    visible = silver_statements.where(F.col("file_available_at") <= F.lit(stamp).cast("timestamp"))
    external = visible.groupBy(*keys).agg(
        F.countDistinct("statement_record_id").cast("int").alias("statement_record_count"),
        F.min(
            F.struct(
                F.col("currency").alias("currency"),
                F.col("status").alias("status"),
                F.col("amount_minor").alias("amount_minor"),
            )
        ).alias("s"),
        F.sort_array(
            F.collect_list(F.concat_ws(":", "source_file", "source_sha256", "source_line"))
        ).alias("statement_pointers"),
    )
    joined = internal.join(external, keys, "full_outer")
    matched_condition = (
        (F.col("internal_record_count") == 1)
        & (F.col("statement_record_count") == 1)
        & (F.col("i.currency") == F.col("s.currency"))
        & (F.col("i.amount_minor") == F.col("s.amount_minor"))
        & (F.col("i.status") == F.col("s.status"))
    )
    matched = joined.where(matched_condition)
    gold = matched.select(
        "dataset_id",
        "channel",
        "external_reference",
        "business_date",
        F.date_format("business_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.lit("MATCHED").alias("recon_status"),
        F.array().cast("array<string>").alias("issue_codes"),
        F.col("i.transaction_id").alias("transaction_id"),
        F.col("i.amount_minor").alias("internal_amount_minor"),
        F.col("i.currency").alias("internal_currency"),
        F.col("i.status").alias("internal_status"),
        F.col("s.amount_minor").alias("statement_amount_minor"),
        F.col("s.currency").alias("statement_currency"),
        F.col("s.status").alias("statement_status"),
        "internal_record_count",
        "statement_record_count",
        _null("timestamp").alias("anchor_event_time"),
        _null("long").alias("discrepancy_age_seconds"),
        F.lit(stamp).cast("timestamp").alias("as_of_time"),
        F.lit(stamp).cast("timestamp").alias("last_evaluated_at"),
        _null("timestamp").alias("first_seen_at"),
        _null("timestamp").alias("resolved_at"),
        F.lit(RECON_RULE_VERSION).alias("recon_rule_version"),
        F.sort_array(F.concat("internal_pointers", "statement_pointers")).alias("source_pointers"),
        F.lit(run_id).alias("run_id"),
    )
    summary = joined.agg(
        F.count(F.lit(1)).alias("total_keys"),
        F.sum(F.when(matched_condition, 1).otherwise(0)).alias("matched_keys"),
    )
    return gold, summary


def entity_digest(df: DataFrame) -> str:
    """Băm không phụ thuộc thứ tự dòng và `run_id`: `<số dòng>:<Σhash1>:<Σhash2>`.

    Mỗi dòng là JSON của mọi cột trừ `run_id`; hai hàm băm 64 bit độc lập được cộng dưới dạng
    `decimal(38,0)` nên không tràn và không phụ thuộc cách chia partition.
    """
    columns = [name for name in df.columns if name != "run_id"]
    text = F.to_json(F.struct(*columns))
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(text).cast("decimal(38,0)")).alias("h1"),
        F.sum(F.xxhash64(text, F.lit("n06-2")).cast("decimal(38,0)")).alias("h2"),
    ).collect()[0]
    return f"{row['rows']}:{row['h1'] or 0}:{row['h2'] or 0}"


def split_by_date(df: DataFrame) -> Iterator[tuple[date, DataFrame, int]]:
    """Từng `business_date` có dòng, theo thứ tự tăng: `(ngày, DataFrame của ngày, số dòng)`."""
    counts = {
        row["business_date"]: int(row["count"])
        for row in df.groupBy("business_date").count().collect()
    }
    for business_date in sorted(counts):
        yield (
            business_date,
            df.where(F.col("business_date") == F.lit(business_date)),
            counts[business_date],
        )
