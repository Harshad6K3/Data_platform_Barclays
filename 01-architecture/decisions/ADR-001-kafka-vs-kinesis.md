# ADR-001: Event Streaming — Apache Kafka over AWS Kinesis

**Date:** 2024-01  
**Status:** Accepted  
**Deciders:** Data Platform Lead, Principal Architect, Security

---

## Context

We need a high-throughput, fault-tolerant event streaming backbone for real-time trade
events, customer activity, and audit logs. Two primary candidates were evaluated:
Apache Kafka (MSK) and AWS Kinesis Data Streams.

## Decision

**Use Amazon MSK (Managed Kafka).**

## Rationale

| Factor | Kafka (MSK) | Kinesis |
|--------|-------------|---------|
| Throughput | Millions msg/sec per partition | ~1 MB/s per shard |
| Replay window | Configurable (days–forever) | 7 days max |
| Consumer groups | Unlimited | Limited fan-out |
| Ecosystem | Kafka Connect, Flink, Spark | AWS-native only |
| Vendor lock-in | Low (portable) | High |
| Operational overhead | Medium (MSK manages brokers) | Low |

Kafka's consumer group model and deep Flink/Spark integration are critical for our
multi-consumer streaming architecture. The replay flexibility is non-negotiable for
audit and reprocessing requirements in a regulated environment.

## Consequences

- **Positive:** Portability, rich ecosystem, fine-grained offset control
- **Negative:** Higher operational knowledge required; mitigated by MSK managed service
- **Risk:** MSK cost at scale — mitigated by partition-level sizing review at 6 months
