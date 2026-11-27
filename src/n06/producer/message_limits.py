"""Hằng giới hạn kích thước message, tách khỏi `kafka_client` để đọc nguồn không cần thư viện Kafka.

`n06.producer.source` được job Spark (image không có `confluent-kafka`) dùng lại.
"""

# Giới hạn kích thước message (key + value) của producer; librdkafka từ chối message lớn hơn
# `message.max.bytes` ngay khi gọi produce(). Chừa biên cho phần overhead của bản ghi.
DEFAULT_MAX_MESSAGE_BYTES = 1_000_000
MESSAGE_OVERHEAD_MARGIN_BYTES = 1024
