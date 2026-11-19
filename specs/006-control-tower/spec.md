# Spec 006: Control tower (live ETA, SLA-risk and abnormal-stop alerts)
Status: Draft · Intent: roadmap item 6 · Author: project owner + Claude

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
This is the reason the project exists (intent): dispatchers learn that a delivery is late only
after the promised window has passed. The lakehouse now holds clean GPS and order data, but
nothing combines a van's live position with the orders it still carries. Nobody sees an ETA, no
alert fires before a breach, and a van stuck somewhere it should not be goes unnoticed.

## Outcome
- Every open order on a van that is out for delivery has a live ETA, updated with each ping.
- An SLA-risk alert fires when the ETA passes the promised window (BR-3), early enough to act:
  the target is at least 15 minutes before the breach.
- A van idle for too long away from its stops and its depot raises an abnormal-stop alert.
- Dispatchers read the latest position per van, the ETA per order and the alerts from the serving
  database (ADR 0007), and other systems can subscribe to an alerts topic.
- An alert reaches the serving database within 10 s of the GPS event (NFR-1, p95).
- Restarts, duplicates and late pings neither lose alerts nor repeat them.

## Users and scenarios
- **Dispatcher:** sees "order X will be 12 min late" while the van is still 20 min away, and calls
  the customer; sees "van Y idle 14 min in District 7, no stop nearby".
- **Feature 7 (gold) and 8 (Grafana):** alert lead time, precision and recall (BR-5), live map.
- **Interviewer:** a simulated late order raises its alert well before its window closes.

## Acceptance criteria
- [ ] AC1: Given a seed and a number of abnormal stops, when a day is simulated, then exactly that many stops of known place, start and length are injected (long enough to alert, away from every stop and depot), and the clean day is otherwise unchanged (unit test).
- [ ] AC2: Given a van out for delivery with open orders, when a ping arrives, then each of those orders has an ETA that follows the agreed formula (unit tests on known routes), visible in the serving database within 10 s.
- [ ] AC3: Given a ping older than the van's latest position, then the latest position does not move backwards, and ETAs are not recomputed from it.
- [ ] AC4: Given an open order whose ETA passes its promised end by the agreed margin, then one SLA-risk alert is raised; it is not raised again until it has been resolved for at least 10 min (BR-3); it is resolved when the ETA is back inside the window or the order is delivered or failed.
- [ ] AC5: Given a simulated day with late deliveries, then the share of late orders alerted at least 15 min before their promised end (recall) and the share of alerts whose order was really late (precision) meet the agreed targets, and both are reported.
- [ ] AC6: Given an injected abnormal stop, then exactly one abnormal-stop alert is raised within the agreed idle time plus 10 s of event time, and resolved when the van moves; normal dwells at stops and parking at the depot never alert (zero on a clean day).
- [ ] AC7: Given live mode, when a ping that changes an alert is produced, then the alert is in the serving database within 10 s at p95 (NFR-1).
- [ ] AC8: Given the same day sent clean and with chaos (duplicates, delays, jitter, offline bursts), then the same alerts are raised (same type, order or van, and event time).
- [ ] AC9: Given the job stopped for 30 min while data keeps arriving, when it restarts, then events older than the alert horizon update positions and ETAs but raise no alert (NFR-4), the alert path is current within 15 min, and no alert raised before the stop is raised again.
- [ ] AC10: Given an alert is raised or resolved, then it is published exactly once to the alerts topic as registry-framed Avro under the existing compatibility rules (ADR 0008).
- [ ] AC11: Given the developer asks for control tower status, then they see open orders with an ETA, open alerts by type, and the newest position time per van.
- [ ] AC12: Given the core and CDC stack with all jobs, maintenance and the control tower running at dev size, then total container memory stays under 6 GB.

## Out of scope
- Dashboards and the live map (feature 8); alert precision and recall as gold KPIs (feature 7).
- Route optimisation, road routing, ML ETA (intent: out of scope; v1 is a distance/speed heuristic).
- Notifying anyone (SMS, e-mail, chat); acknowledging or snoozing alerts.
- ETA for orders not yet out for delivery (open question 3).

## Decisions that change
- New ADR 0013: how the alert path combines positions and orders, and how ETA is computed.
- Requirements, data-modeling rules: "the alert path uses a temporal join" is replaced (open question 2).
- Postgres moves from the optional CDC profile to the core stack, because the serving tables live
  there (ADR 0007).

## Open questions
1. **Where does the alert job read?** *Recommended:* straight from Kafka (GPS and the order change
   topic), decoded with the same contract code as bronze. Silver is 1–2 minutes behind bronze and
   cannot meet 10 s. Bad messages are skipped here; bronze already sends them to the dead-letter table.
2. **How are positions and orders combined?** Options:
   - (a) *Recommended:* state kept per van: its latest position, its open orders and its alert
     state, updated in event-time order, with timers for idle detection. Small state (about 200 vans
     and their open orders), deterministic on replay. Needs a Python stream function (one more Python
     worker, about 300 MB).
   - (b) SQL joins. A temporal join must key on the order, not the van, and a regular join gives
     results in arrival order; neither can detect "idle for 10 minutes" without more pings.
3. **ETA formula (v1)?** *Recommended:* from the van's position through its remaining stops in stop
   order up to this order's stop: straight-line distance ÷ the van's average moving speed over the
   last 10 min (25 km/h until known), plus 4 min dwell per stop before it. Only for orders out for
   delivery; earlier orders show "not dispatched".
4. **Alert rules?** *Recommended:* SLA risk as BR-3 (ETA ≥ 1 min past promised end; once per order;
   again only after 10 min resolved). Abnormal stop: speed under 3 km/h for at least 10 min, more than
   150 m from every stop of the van's open orders and from its depot; resolved when it moves again.
5. **Quality targets (AC5)?** *Recommended:* recall ≥ 80 % with ≥ 15 min lead, precision ≥ 70 %, on
   the default simulated day. They are checked after the first build; if the heuristic cannot reach
   them, the spec changes before the PR, not the test.
6. **Alert horizon?** *Recommended:* 10 min, as already approved (requirements, resolved question 1).
7. **Simulator?** *Recommended:* a new option for abnormal stops per day (off by default; on in the
   demo), each 12–25 min long, at a random point of a route away from stops.
