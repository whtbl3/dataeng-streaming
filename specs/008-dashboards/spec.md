# Spec 008: Dashboards (live dispatch, KPIs, pipeline health and its alerts)
Status: Draft · Intent: roadmap item 8 · Author: project owner

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
Everything the intent promises a person can see now lives only in tables: vans and alerts in the
serving database, KPIs in gold, pipeline health in nobody's view. A dispatcher cannot watch a
map, a manager cannot read a KPI without SQL, and nobody is told when the pipeline stalls,
falls behind or fails a checkpoint (NFR-10). The intent's last outcome, "a dashboard shows
on-time %, ETA error and active alerts", is not met.

## Outcome
- A **dispatch** dashboard: the live map of vans, open alerts, and open orders at risk with their
  ETA against the promised window, refreshed every few seconds.
- A **KPI** dashboard per depot and day: on-time rate, failure rate, ETA error, stops per hour,
  alert precision and recall, where a provisional number is visibly marked and a final-only view
  exists (requirements, resolved question 3).
- A **pipeline health** dashboard: Kafka consumer lag, Flink throughput and checkpoints, the CDC
  replication slot, data freshness per layer and the maintenance runs.
- The four NFR-10 alerts, raised by the dashboards' own alerting.
- Everything provisioned from the repository (dashboards as code), and optional: the core stack
  runs without it.

## Users and scenarios
- **Dispatcher:** sees van 42 idle on the map and the at-risk order list growing; clicks an alert.
- **Operations manager:** compares depots for yesterday (final) and today (provisional).
- **Developer:** sees bronze stop receiving rows while Kafka still fills up, and is alerted.
- **Interviewer:** opens three dashboards after `make up` + `make demo`, no setup by hand.

## Acceptance criteria
- [ ] AC1: Given the stack is started with the dashboards option, then the dashboard service and the metrics store are healthy within the start-up time, and every data source (serving database, gold, metrics) is connected and answers.
- [ ] AC2: Given live data, then the dispatch dashboard shows each van's latest position on a map, the open alerts and the open orders with ETA past the promised end; a new ping or alert is visible within 10 s; the numbers match the serving tables.
- [ ] AC3: Given gold, then the KPI dashboard shows, per depot and local day, the KPIs of spec 007; rows that are not final carry a visible provisional marker; a final-only view shows closed days only; the numbers match gold.
- [ ] AC4: Given the running jobs, then the pipeline dashboard shows consumer lag per consumer group, records per second and checkpoint duration and failures per Flink job, the replication slot lag, the newest row time of bronze, silver and gold, and the last maintenance run per table.
- [ ] AC5: Given consumer lag that keeps growing for 5 min, a failed checkpoint, a replication slot lag over 500 MB, or messages arriving in Kafka while bronze gets no rows for 2 min, then the matching alert fires, and it clears when the condition ends (NFR-10).
- [ ] AC6: Given the repository, then every dashboard, data source and alert rule comes from versioned files; a change made in the UI is not silently kept; and every panel's query runs without error (test).
- [ ] AC7: Given the core and CDC stack with the dashboards running at dev size, then total container memory stays under 8 GB (local footprint), and without the dashboards option it stays under 6 GB as before.

## Out of scope
- Sending alerts anywhere (e-mail, chat, paging); acknowledging or silencing them from a phone.
- User accounts and permissions beyond a local admin and anonymous viewing.
- AWS dashboards and CloudWatch (features 9 and 10).
- Dashboards for the ML or routing topics (out of scope in the intent).

## Decisions that change
- New ADR 0015: how metrics are collected and where alerts are evaluated.
- The local-footprint rule gets its measured value with the dashboards (requirements, NFR).

## Open questions
1. **Optional or always on?** *Recommended:* an opt-in profile, so the core stack keeps its 6 GB
   budget; `make up` with the dashboards profile is what the demo and the README use.
2. **How does the dashboard read gold?** *Recommended:* through Trino, as ADR 0007 decided, with a
   community Trino data source plugin at a pinned version. The alternative, copying gold aggregates
   into Postgres, would add a second path to the same numbers.
3. **Which metrics, collected how?** *Recommended:* only built-in endpoints: Flink's Prometheus
   reporter and Redpanda's public metrics endpoint, scraped by Prometheus. The replication slot,
   freshness per layer and the maintenance log are read with SQL from Postgres and Trino. No extra
   exporters (memory).
4. **The "bronze silence" alert?** At dev size nothing is sent most of the time, so "no rows for
   2 min" would always fire. *Recommended:* fire only when the GPS topic received messages in the
   last 2 min and bronze committed none (the pipeline is stuck, not idle).
5. **Where are alerts evaluated?** *Recommended:* in the dashboard service's own alerting, with
   rules in files and no contact point (alerts are visible in the UI and its API).
6. **Map background?** *Recommended:* OpenStreetMap tiles, loaded by the browser; without internet
   the vans still show on a plain background.
7. **Access?** *Recommended:* anonymous read-only viewing on localhost and an admin whose password
   comes from the environment with a local default, like the other services.
