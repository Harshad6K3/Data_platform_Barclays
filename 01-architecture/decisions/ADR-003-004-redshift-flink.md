# ADR-003: Redshift Serverless vs Provisioned Cluster

**Date:** 2024-01  
**Status:** Accepted

---

## Context

The data warehouse needs to serve two distinct workloads:
- **Heavy batch**: nightly dbt runs (high concurrency, 02:00–06:00 UTC)
- **Light interactive**: risk dashboards and ad-hoc analyst queries (09:00–18:00 UTC)

A provisioned Redshift cluster sized for peak batch load wastes cost during off-peak hours.

## Decision

**Amazon Redshift Serverless** with base capacity 32 RPU, auto-scaling to 256 RPU.

## Rationale

| Factor | Serverless | Provisioned |
|--------|-----------|-------------|
| Cost model | Per-second RPU billing | Fixed hourly regardless of load |
| Scaling | Auto 32→256 RPU in ~60s | Manual resize takes minutes |
| Ops burden | Zero node management | Patch, resize, snapshot management |
| Concurrency scaling | Built-in | Extra cost add-on |
| Performance | Equivalent for our volumes (<10 TB) | Marginally better at >100 TB |

At our projected data volume (2–5 TB/year), Serverless is 35–40% cheaper than
an equivalent provisioned cluster running 24/7.

## Consequences

- Serverless has a ~3s cold start if idle >30min — acceptable for batch; 
  mitigated for dashboards by a scheduled warm-up query via Lambda.
- Cross-AZ Serverless not supported in all regions — confirm eu-west-1 support.
- Budget alert set at £5,000/month RPU cost.

---

# ADR-004: Apache Flink on EKS vs Amazon Managed Flink (Kinesis Data Analytics)

**Date:** 2024-01  
**Status:** Accepted

---

## Context

The streaming enrichment pipeline requires Apache Flink. Two deployment options:
- **Amazon Managed Flink** (formerly Kinesis Data Analytics for Flink)
- **Self-managed Flink on EKS** (Kubernetes)

## Decision

**Amazon Managed Flink** for production streaming jobs.

## Rationale

| Factor | Managed Flink | Flink on EKS |
|--------|--------------|--------------|
| Ops overhead | Zero (AWS managed) | High (K8s + Flink ops) |
| Auto-scaling | Built-in Flink parallelism scaling | Manual HPA config |
| Checkpointing | S3 managed | Self-configure |
| Flink version | AWS controls upgrades | Full control |
| Cost | Per-KPU per hour | EC2/EKS + engineering time |
| Debugging | CloudWatch + Flink UI | Custom setup needed |

The engineering team's bandwidth is better spent on data product features than
Kubernetes Flink operations. We accept the minor version lag on AWS-managed Flink.

## Migration path

If we outgrow Managed Flink (>200 KPUs sustained), revisit EKS with a dedicated
platform engineer. Architecture is portable — Flink jobs are standard Java/Python.

## Consequences

- Slightly less flexibility on Flink connector versions
- No dependency on Databricks/Confluent — fully AWS-native streaming stack
