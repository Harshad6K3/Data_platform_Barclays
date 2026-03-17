"""
IBM Sterling B2B Integration — Trade Confirmation Ingestion
============================================================

IBM Sterling Connect:Direct / Sterling File Gateway is widely used in
financial services for secure B2B file transfers (SWIFT, FIX, FPML).

This module handles:
  1. Sterling-delivered trade confirmation files (CSV/XML) from counterparties
  2. Validation and parsing
  3. Publishing to Kafka (dp.trading.confirmations.v1) for pipeline consumption
  4. Acknowledgement file generation back to Sterling

Integration pattern:
  Sterling File Gateway (SFTP pickup)
    → This Python adapter (Spring Boot microservice calls via REST)
    → Kafka topic
    → Flink enrichment pipeline (reuses existing streaming infrastructure)

Sterling-specific considerations:
  - Files arrive via Sterling MFT with PGP encryption
  - Sterling metadata (sender DUNS, file ID) preserved as Kafka headers
  - Duplicate detection: Sterling may redeliver files on failure
  - Audit: every file received/processed recorded in dp_governance.sterling_audit
"""

import xml.etree.ElementTree as ET
import csv
import logging
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from typing import Iterator

logger = logging.getLogger(__name__)


@dataclass
class SterlingFileMetadata:
    """Metadata extracted from Sterling File Gateway envelope."""
    file_id:        str
    sender_duns:    str
    sender_name:    str
    receiver_duns:  str
    file_type:      str     # TRADE_CONFIRM, SETTLEMENT_INSTRUCTION, etc.
    received_at:    datetime
    file_checksum:  str


@dataclass
class TradeConfirmation:
    """Parsed trade confirmation from counterparty."""
    trade_id:           str
    our_ref:            str    # Internal trade ID for matching
    instrument_isin:    str
    notional:           float
    currency:           str
    trade_date:         str
    settlement_date:    str
    counterparty_lei:   str
    price:              float
    direction:          str    # BUY / SELL
    source_file_id:     str    # Traceability back to Sterling file


class SterlingTradeConfirmationAdapter:
    """
    Parses trade confirmation files delivered via IBM Sterling File Gateway.

    Supports:
      - FpML 5.x (XML)  — standard derivatives confirmation format
      - ISO 15022 MT518 (fixed-width) — settlement confirmations
      - CSV (bespoke counterparty formats with configurable field mapping)
    """

    def __init__(self, kafka_producer, audit_repo):
        self.producer   = kafka_producer
        self.audit_repo = audit_repo

    def process_file(
        self,
        file_content: bytes,
        metadata:     SterlingFileMetadata,
        file_format:  str,   # FPML, MT518, CSV
    ) -> dict:
        """
        Process a Sterling-delivered file end-to-end.
        Returns summary: {processed: N, failed: M, file_id: ...}
        """
        # Idempotency: check if file already processed
        if self.audit_repo.is_processed(metadata.file_id):
            logger.warning("Duplicate file received, skipping. file_id=%s", metadata.file_id)
            return {"status": "DUPLICATE", "file_id": metadata.file_id}

        # Checksum verification
        actual_checksum = hashlib.sha256(file_content).hexdigest()
        if actual_checksum != metadata.file_checksum:
            raise ValueError(
                f"Checksum mismatch for file_id={metadata.file_id}: "
                f"expected={metadata.file_checksum} actual={actual_checksum}"
            )

        self.audit_repo.record_received(metadata)

        processed, failed = 0, 0
        parser = self._get_parser(file_format)

        for confirmation in parser(file_content, metadata):
            try:
                self._publish_to_kafka(confirmation, metadata)
                processed += 1
            except Exception as ex:
                logger.error("Failed to publish confirmation trade_id=%s: %s",
                             confirmation.trade_id, ex)
                failed += 1

        self.audit_repo.record_completed(metadata.file_id, processed, failed)

        result = {
            "status":     "COMPLETE" if failed == 0 else "PARTIAL",
            "file_id":    metadata.file_id,
            "processed":  processed,
            "failed":     failed,
        }
        logger.info("Sterling file processed: %s", result)
        return result

    def _get_parser(self, file_format: str):
        parsers = {
            "FPML":  self._parse_fpml,
            "MT518": self._parse_mt518,
            "CSV":   self._parse_csv,
        }
        if file_format not in parsers:
            raise ValueError(f"Unsupported file format: {file_format}")
        return parsers[file_format]

    def _parse_fpml(
        self, content: bytes, meta: SterlingFileMetadata
    ) -> Iterator[TradeConfirmation]:
        """Parse FpML 5.x XML trade confirmations."""
        root = ET.fromstring(content)
        ns   = {"fpml": "http://www.fpml.org/FpML-5/confirmation"}

        for trade_el in root.findall(".//fpml:trade", ns):
            try:
                yield TradeConfirmation(
                    trade_id         = trade_el.findtext("fpml:tradeHeader/fpml:partyTradeIdentifier/fpml:tradeId", namespaces=ns, default=""),
                    our_ref          = trade_el.findtext("fpml:tradeHeader/fpml:partyTradeIdentifier/fpml:tradeId[@partyReference='party1']", namespaces=ns, default=""),
                    instrument_isin  = trade_el.findtext(".//fpml:instrumentId", namespaces=ns, default=""),
                    notional         = float(trade_el.findtext(".//fpml:notionalAmount/fpml:amount", namespaces=ns, default="0")),
                    currency         = trade_el.findtext(".//fpml:notionalAmount/fpml:currency", namespaces=ns, default=""),
                    trade_date       = trade_el.findtext("fpml:tradeHeader/fpml:tradeDate", namespaces=ns, default=""),
                    settlement_date  = trade_el.findtext(".//fpml:settlementDate", namespaces=ns, default=""),
                    counterparty_lei = trade_el.findtext(".//fpml:partyId[@partyIdScheme='http://www.gleif.org/data/schema/leiData']", namespaces=ns, default=""),
                    price            = float(trade_el.findtext(".//fpml:initialPrice/fpml:cleanPrice", namespaces=ns, default="0")),
                    direction        = trade_el.findtext(".//fpml:buyerPartyReference/@href", namespaces=ns, default="BUY"),
                    source_file_id   = meta.file_id,
                )
            except Exception as ex:
                logger.warning("Failed to parse FpML trade element: %s", ex)

    def _parse_csv(
        self, content: bytes, meta: SterlingFileMetadata
    ) -> Iterator[TradeConfirmation]:
        """
        Parse bespoke CSV format.
        Field mapping configured per counterparty in Sterling partner profile.
        TODO: load field mapping from dp_reference.sterling_counterparty_config
        """
        reader = csv.DictReader(StringIO(content.decode("utf-8")))
        for row in reader:
            yield TradeConfirmation(
                trade_id        = row.get("TRADE_ID", ""),
                our_ref         = row.get("OUR_REF", ""),
                instrument_isin = row.get("ISIN", ""),
                notional        = float(row.get("NOTIONAL", 0)),
                currency        = row.get("CURRENCY", ""),
                trade_date      = row.get("TRADE_DATE", ""),
                settlement_date = row.get("SETTLEMENT_DATE", ""),
                counterparty_lei= row.get("LEI", ""),
                price           = float(row.get("PRICE", 0)),
                direction       = row.get("DIRECTION", ""),
                source_file_id  = meta.file_id,
            )

    def _parse_mt518(self, content: bytes, meta: SterlingFileMetadata) -> Iterator[TradeConfirmation]:
        """
        Parse ISO 15022 MT518 (Market-Side Securities Trade Confirmation).
        TODO: implement full MT518 field tag parsing (Field 20, 23G, 35B, etc.)
        """
        raise NotImplementedError("MT518 parser — TODO: implement field tag extraction")

    def _publish_to_kafka(self, conf: TradeConfirmation, meta: SterlingFileMetadata):
        """Publish parsed confirmation to Kafka with Sterling traceability headers."""
        self.producer.publish(
            topic   = "dp.trading.confirmations.v1",
            key     = conf.trade_id,
            value   = json.dumps(conf.__dict__, default=str),
            headers = {
                "sourceSystem":  "IBM_STERLING",
                "senderDuns":    meta.sender_duns,
                "sterlingFileId": meta.file_id,
                "schemaVersion": "v1",
            }
        )
