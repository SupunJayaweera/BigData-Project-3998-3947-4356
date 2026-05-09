# EC8207 Mini Project Report

## Smart City Traffic & Congestion Pipeline

**Student Names:**
Jayaweera S. T. - EG/2020/3998 |
Gnathilake A. P. B. - EG/2020/3947 |
Madhushan K. K. S. - EG/2020/4356

**Date:** 2026-05-04

---

## 1. Introduction

The Smart City Traffic & Congestion Pipeline is a robust, scaleable data processing architecture built to ingest, analyze, and store real-time telemetry from intersection sensors. By implementing a Lambda Architecture approach, the system seamlessly serves both ultra-low-latency alerting for urgent traffic incidents and deep batch-analytics for overnight decision-making. The real-time aspect ensures that immediate traffic anomalies (e.g., sudden massive decelerations below $10.0$ km/h) trigger operational visibility, while the historical analysis feeds strategic decision-making such as overnight policing schedules and long-term infrastructural improvements.

## 2. Technology Stack Justification

### 2.1 Apache Kafka

Apache Kafka was selected as the ingestion backbone due to its exceptional throughput, horizontal scaleability, and persistence via topic partitioning. Sensor endpoints emit telemetry at high frequencies. Rather than hammering a transactional database or API endpoint linearly, Kafka buffers these writes optimally. We separated data structurally by allocating a 4-partition topic for `traffic-raw` normal telemetry and an isolated 1-partition topic for `traffic-critical` events, enabling decoupled downstream consumption and independent retention policies.

### 2.2 Apache Spark Structured Streaming

Spark Structured Streaming was chosen over alternative streaming engines (like Storm or Flink) primarily due to its unified batch and streaming API. It allows our pipeline to execute windowed calculations continuously against streaming data streams using the identical SQL/Dataframe syntaxes that we would employ on static datasets. Furthermore, its native handling of event-time semantics, watermarking, and fault-tolerance (via checkpointing) makes calculating accurate traffic congestion matrices exceptionally resilient to worker node failures constraints.

### 2.3 PostgreSQL

PostgreSQL provides robust relational data persistence that natively accommodates time-series nuances. Since our batch operations require intricate groupings (hourly aggregations, temporal sorting), having a true SQL engine allows us to drastically condense downstream API requirements. Crucially, the requirement to isolate event occurrences across global temporal shifts necessitated PostgreSQL's `TIMESTAMPTZ` data types. This natively differentiates between arbitrary server timestamps and actual sensor event origination times automatically.

### 2.4 Apache Airflow

To orchestrate the generation of nightly reports consistently without relying on host-level `cron` daemons, we integrated Apache Airflow into the stack. Airflow explicitly enables Directed Acyclic Graphs (DAGs) which map out our workflow: trigger, collect Postgres telemetry, serialize to CSV, and export locally. Its resilience, automatic retry paradigms on task failure, and the visibility of task executions via the Airflow Web UI make batch operational diagnostics profoundly easier for an operations team.

## 3. Event Time vs. Processing Time

Differentiating between _Event Time_ and _Processing Time_ is paramount in stream processing architectures. **Event time** references the exact moment the vehicle crossed the sensor (encoded within the simulated payload natively). **Processing time** references the moment the Spark execution or downstream service actually processes/inserts the data.

Our pipeline strictly honors event-time. It parses the JSON timestamp payload, applies a native Spark watermark of 10 minutes (`withWatermark("event_time", "10 minutes")`), and performs windowed calculations grouped inherently by this event time.

If we utilized pure processing time, any system latency (such as temporary Kafka ingestion outages, slow container reboots, or network congestion) would artificially distort the Congestion Index by blending delayed historical inputs with current intersection traffic. By watermarking and aggregating strictly against the emitted timestamp, we ensure congestion analytics perfectly reflect reality.

## 4. Pipeline Architecture Overview

The system operates across distinct phases:

1. **Producer**: Python script generating intersection counts every second.
2. **Kafka Message Broker**: Captures the raw sensor payload instantly.
3. **Spark Master/Worker**: Continuously consumes the `traffic-raw` topic. It executes real-time `.filter` constraints for critical alerts (speed $< 10.0$ km/h), calculates a Congestion Index over tumbling 5-minute event-time windows, and pushes batches directly to PostgreSQL using `foreachBatch` logic.
4. **PostgreSQL**: Sinks all data locally.
5. **Analytics Dashboard**: Parses the real-time data back upward dynamically into a Javascript GUI using a Node/Python micro-server API.
6. **Airflow Server**: Every day at 23:00, Airflow captures the entirety of `sensor_readings`, `critical_alerts`, and `congestion_windows` corresponding strictly to `CURRENT_DATE` logic, exporting them flawlessly to daily mounted CSV flat files.

## 5. Ethics & Data Governance

### 5.1 Privacy Implications

By design, our intersection data leverages macro-scale aggregations (`vehicle_count` and `avg_speed`) ensuring zero Personal Identifiable Information (PII) such as ANPR (license plates) or individual routing habits are retained. However, privacy risks still exist: heavily granular data mapping local neighborhood egress points at very specific times could hypothetically triangulate residential activity habits. Thus, data should remain inherently segmented and structurally blurred regarding highly localized intersections near residential areas.

### 5.2 Data Governance Recommendations

The system demands a robust governance implementation. First, a strict retention TTL should be instituted terminating raw telemetry after 30 days, leaving only aggregated analytical totals. In Sri Lanka, referencing the Personal Data Protection Act (2022), if these sensors were eventually upgraded with optical/camera capabilities, the entire Postgres pipeline must shift behind strict Role-Based Access Controls (RBAC), implementing data masking on arrival and preventing raw video egress beyond the edge node. Currently, simple read-only API database roles for the analytical dashboard mitigate most manipulation risks.

### 5.3 Ethical Use

The automated police deployment reports generate an inherently complex ethical dilemma. Because enforcement scores map strictly to congestion density, poorer neighborhoods with aging infrastructure resulting in slower traffic might trigger dramatically more "Police Deployments" purely systematically, despite lacking correlating crime statistics. Deploying policing based singularly on throughput volume effectively biases against infrastructural neglect. This risks contributing to systemic over-policing and discrimination. The generated deployment thresholds ($> 6.0\text{ score}$) should therefore solely represent traffic management/auxiliary operations—strictly not generalized enforcement scaling.

## 6. Analytical Report: Traffic Volume vs. Time of Day

![Dashboard Chart](assets/dash.png)
![Data Table](assets/tab.png)

**Data Analysis:**
The visualization and table above demonstrate the aggregated traffic volume plotted against the time of day across our monitored junctions. By extracting the hour from the semantic `event_time` timestamp, our Spark-PostgreSQL pipeline accurately segments temporal data regardless of processing latencies.

As observed, traffic scales significantly during peak transit hours. This visualization serves as a primary tool for urban planners to correlate road utilization with specific times of the day, allowing for data-driven infrastructure planning and targeted traffic police deployments.
