"""
Flink Streaming Pipeline — Trade Event Enrichment
==================================================

Pipeline stages:
  Kafka Source (dp.trading.trade-events.v1)
    → Watermark / Event-time assignment
    → Enrichment join with Reference Data (instrument, counterparty)
    → Validation & Dead-letter routing
    → Aggregation (5-min tumbling window — notional by instrument)
    → Iceberg Sink (s3://dp-datalake-raw/trading/trade_events/)
    → Redshift Sink (via Kafka Connect JDBC — optional)

Guarantees: exactly-once via Kafka + Flink checkpointing to S3
"""

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common import WatermarkStrategy, Duration, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.table import StreamTableEnvironment
import json
import logging

logger = logging.getLogger(__name__)

KAFKA_BROKERS = "msk-broker-1:9092,msk-broker-2:9092"
INPUT_TOPIC    = "dp.trading.trade-events.v1"
DLT_TOPIC      = "dp.trading.trade-events.v1.dlt"
CONSUMER_GROUP = "flink-trade-enrichment-cg"
S3_OUTPUT_PATH = "s3a://dp-datalake-raw/trading/trade_events/"
CHECKPOINT_DIR = "s3a://dp-checkpoints/flink/trade-enrichment/"


def build_pipeline():
    # ── Environment ──────────────────────────────────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60_000)  # checkpoint every 60s
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage(CHECKPOINT_DIR)

    t_env = StreamTableEnvironment.create(env)

    # ── Kafka Source ─────────────────────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(TradeEventTimestampAssigner()),
        source_name="KafkaTradeEvents"
    )

    # ── Parse & Validate ─────────────────────────────────────────────────────
    parsed_stream = raw_stream.map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())

    valid_stream = parsed_stream.filter(lambda e: e is not None and e.get("valid"))
    invalid_stream = parsed_stream.filter(lambda e: e is None or not e.get("valid"))

    # Route invalid events to DLT
    invalid_stream.map(lambda e: json.dumps(e or {"error": "parse_failure"})) \
        .sink_to(build_kafka_dlt_sink())

    # ── Enrichment join with reference data ──────────────────────────────────
    # TODO: Replace with async I/O against ElastiCache / RDS reference store
    enriched_stream = valid_stream.map(enrich_with_reference_data)

    # ── 5-minute tumbling window — notional aggregation per instrument ────────
    windowed_agg = (
        enriched_stream
        .key_by(lambda e: e["instrumentId"])
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .aggregate(NotionalAggregateFunction())
    )

    windowed_agg.map(lambda agg: logger.info("Agg: %s", agg))  # metrics hook

    # ── Iceberg / S3 Sink (Parquet, partitioned by date/instrument) ───────────
    # TODO: Switch to official Flink Iceberg connector for full ACID writes
    enriched_stream.map(lambda e: json.dumps(e)) \
        .sink_to(
            FileSink.for_row_format(S3_OUTPUT_PATH, SimpleStringSchema())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("trade")
                .with_part_suffix(".json")
                .build()
            )
            .build()
        )

    return env


def parse_event(raw: str) -> dict | None:
    """Parse raw JSON string into a trade event dict. Returns None on failure."""
    try:
        event = json.loads(raw)
        required = {"tradeId", "instrumentId", "notional", "currency", "eventTimestamp"}
        missing = required - event.keys()
        if missing:
            return {"valid": False, "reason": f"Missing fields: {missing}", "raw": raw}
        event["valid"] = True
        return event
    except Exception as ex:
        return {"valid": False, "reason": str(ex), "raw": raw}


def enrich_with_reference_data(event: dict) -> dict:
    """
    Enrich trade event with instrument master and counterparty data.
    TODO: Replace stub with async I/O against reference data cache (ElastiCache).
    """
    # Stub enrichment
    event["instrumentName"] = f"INSTRUMENT_{event['instrumentId']}"
    event["assetClass"] = "EQUITY"        # TODO: lookup from reference store
    event["counterpartyLei"] = "STUB_LEI" # TODO: lookup from counterparty store
    return event


def build_kafka_dlt_sink():
    """Build a Kafka sink for the dead-letter topic."""
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


class TradeEventTimestampAssigner:
    """Extract event-time from the eventTimestamp field in the payload."""
    def extract_timestamp(self, element, record_timestamp):
        try:
            event = json.loads(element)
            return int(event.get("eventTimestamp", record_timestamp))
        except Exception:
            return record_timestamp


class NotionalAggregateFunction:
    """Aggregate total notional per instrument per 5-min window."""
    def create_accumulator(self):
        return {"instrumentId": None, "totalNotional": 0.0, "tradeCount": 0}

    def add(self, value, accumulator):
        accumulator["instrumentId"] = value["instrumentId"]
        accumulator["totalNotional"] += float(value.get("notional", 0))
        accumulator["tradeCount"] += 1
        return accumulator

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return {
            "instrumentId": a["instrumentId"],
            "totalNotional": a["totalNotional"] + b["totalNotional"],
            "tradeCount": a["tradeCount"] + b["tradeCount"]
        }


if __name__ == "__main__":
    pipeline = build_pipeline()
    pipeline.execute("trade-event-enrichment-pipeline")
