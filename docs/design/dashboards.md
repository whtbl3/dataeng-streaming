# Dashboard design
Status: designed, not built (spec 008) · Tool: Grafana OSS (ADR 0007), when built

Three dashboards: what a dispatcher, a manager and an operator would see, and where every
number comes from. The SQL of each panel is in [`dashboards.sql`](dashboards.sql), by panel
name, and runs against the stack today. `make health` checks the NFR-10 conditions now, with the
same rules as the alerts at the end of this page.

**Sources:**
- `postgres`: the serving tables, written by the control tower (spec 006);
- `trino`: the lakehouse, gold and the maintenance log (specs 005, 007);
- `prometheus`: Flink and Redpanda metrics, when the dashboards are built.

## 1. Dispatch (refresh 5 s)
For the dispatcher: where is each van, which orders will be late, what is stuck.

| Panel | Question it answers | Source | Query | Visual |
|---|---|---|---|---|
| Van map | Where is every van now, and is it moving? | postgres | `dispatch.van_map` | Geomap, one point per van; colour by `speed_kmh` (0 = grey), tooltip `age_s` |
| Open alerts | What needs a call right now? | postgres | `dispatch.open_alerts` | Table, newest first; `type` as a coloured badge (SLA_RISK amber, ABNORMAL_STOP red) |
| At-risk orders | Which open orders will miss their window, and by how much? | postgres | `dispatch.at_risk_orders` | Table sorted by `minutes_late`; a bar gauge per row |
| Alerts per hour | Is today worse than usual? | postgres | `dispatch.alerts_per_hour` | Stacked bars by `type`, last 24 h |

A van whose `age_s` is above 60 is drawn hollow: the map shows the last known position of a van
that stopped sending. That is not an abnormal stop (spec 006).

## 2. Delivery KPIs (refresh 5 min)
For the operations manager: per depot and local day (BR-4).

| Panel | Question it answers | Source | Query | Visual |
|---|---|---|---|---|
| Depot × day | How did each depot do, day by day? | trino | `kpi.depot_day` | Table: on-time and failure rate (BR-2), minutes late, ETA MAE and p90 at T-15, stops per hour, alert precision and recall (BR-5) |
| On-time trend | Is the fleet getting better? | trino | `kpi.on_time_trend` | Time series of the on-time rate over 14 days |
| Closed days | Which days are final, and what arrived after the close? | trino | `kpi.closed_days` | Table: closed at, late events absorbed, late after close, restatements |

**Provisional and final** (requirements, resolved question 3):
- A row with `status = provisional` carries an amber "provisional" badge and an italic value.
- A dashboard toggle "final only" adds `AND (is_final OR NOT $final_only)` to `kpi.depot_day`.
  Reports use the toggle; the daily view shows today as provisional.
- `kpi.on_time_trend` draws a day whose depots are not all final (`all_final = false`) as a dashed
  segment.

## 3. Pipeline health (refresh 30 s)
For the operator: is data flowing, and is anything falling behind?

| Panel | Question it answers | Source | Query or metric | Visual |
|---|---|---|---|---|
| Consumer lag | Is any reader falling behind? | prometheus | `sum by (group) (redpanda_kafka_max_offset - on (topic, partition) group_right redpanda_kafka_consumer_group_committed_offset)` | Time series per consumer group |
| Records per second | How much does each job process? | prometheus | `sum by (job_name) (flink_taskmanager_job_task_numRecordsInPerSecond)` | Time series per job |
| Checkpoints | Do checkpoints succeed, and how long do they take? | prometheus | `flink_jobmanager_job_lastCheckpointDuration`, `increase(flink_jobmanager_job_numberOfFailedCheckpoints[5m])` | Duration series plus a failure counter per job |
| Replication slot | Is Postgres keeping too much WAL for CDC? | postgres | `pipeline.slot_lag` | Stat in MB; red above 500 |
| Newest row per layer | Is each layer fresh? | trino | `pipeline.newest_row` | Table: layer, newest time, age; amber above 2 min, red above 10 min |
| Maintenance | When did compaction and cleanup last succeed? | trino | `pipeline.maintenance` | Table: task, table, last success |

Building it needs two settings beyond the dashboards themselves:
- Flink's Prometheus reporter (`metrics.reporter.prom.factory.class` in the Flink properties,
  port 9249).
- A Prometheus scrape of Redpanda's `/public_metrics` (port 9644). The Redpanda names above were
  read from the running broker; the Flink names follow its metric scope for 1.20.

## 4. Alert rules (NFR-10)
The same rules as `make health`, but with the windows a dashboard tool can keep (`make health`
samples once, or twice 30 s apart):

| Alert | Fires when | For | `make health` |
|---|---|---|---|
| Consumer lag grows | lag of a group is above 2,000 messages and higher than 5 min ago | 5 min | two samples 30 s apart, growth and above 2,000 |
| Checkpoint failed | a job's failed-checkpoint counter increases | 0 | the latest attempt failed after the latest success |
| Replication slot | slot lag > 500 MB | 5 min | now |
| Bronze stuck | GPS messages arrived in the last 2 min and bronze has no row in 2 min | 2 min | now |

"Bronze stuck" uses the topic's newest record time. Without it, an idle demo (nothing sent) would
alert all the time.
