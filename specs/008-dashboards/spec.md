# Spec 008: Dashboard design and pipeline health checks
Status: Approved (2026-11-20) · Intent: roadmap item 8 · Author: project owner

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
What a person should see lives only in tables: vans and alerts in the serving database, KPIs in
gold, and pipeline health nowhere. Nobody is told when the pipeline stalls, falls behind or fails
a checkpoint (NFR-10). The project owner decided not to build and test real dashboards now: a
precise design is enough for the portfolio, and the checks behind NFR-10 must exist as a command.

## Outcome
- A dashboard design that a dashboard tool can be built from without new decisions: three
  dashboards (dispatch, KPIs, pipeline health), each panel with the question it answers, its
  source, its query, its refresh and how provisional numbers are marked.
- One command that checks the four NFR-10 conditions now and says, per condition, OK or ALERT,
  with the number behind it; it fails when any condition alerts.
- Building the real dashboards becomes a stretch item.

## Users and scenarios
- **Developer:** runs the health check after `make up`, or while a demo runs, and sees at once
  whether bronze is stuck while Kafka fills up.
- **Interviewer:** reads one page that shows what the dispatcher and the manager would see, and
  which table and query each panel comes from.

## Acceptance criteria
- [ ] AC1: Given the design, then it describes the dispatch, KPI and pipeline dashboards panel by panel: question, source (serving, gold or metrics), query, refresh, and the provisional marker for numbers that are not final (requirements, resolved question 3).
- [ ] AC2: Given the queries in the design, then each one runs without error against the running stack (one light test).
- [ ] AC3: Given the running stack, when the health check runs, then it reports for each consumer group whether lag grows, for each Flink job whether its latest checkpoint failed, the replication slot lag against 500 MB, and whether bronze got no rows while Kafka received messages in the last 2 min, each as OK or ALERT with its value.
- [ ] AC4: Given any condition alerts, then the health check exits with an error; on a healthy stack it exits cleanly (unit test of the rules; one e2e run).

## Out of scope
- A dashboard service, provisioned dashboards, alert delivery (stretch).
- A 5-minute history of consumer lag: the check compares two samples taken during the run
  (open question 2).
- AWS monitoring (features 9 and 10).

## Decisions that change
- Intent, desired outcome "a dashboard shows on-time %, ETA error and active alerts": the dashboards
  are designed, not built (changed by the project owner, 2026-11-20).
- ADR 0007: Grafana and Prometheus are deferred; the serving tables stay as designed for them.

## Resolved questions (approved 2026-11-20)
All three recommendations were accepted as written.
1. **Where does the design live?** *Recommended:* one page in the design documents, with the panel
   queries in a SQL file next to it so the light test can run them.
2. **"Lag grows for 5 min" without history?** *Recommended:* the check takes two lag samples 30 s
   apart and alerts when lag is above zero and grew; the 5-minute persistence belongs to the
   dashboard tool's alert rule, which the design specifies.
3. **"Bronze got no rows" when nothing is sent?** *Recommended:* alert only when the GPS topic's
   newest offset moved in the last 2 min and bronze's newest row is older than 2 min (stuck, not
   idle), as in the earlier spec draft.
