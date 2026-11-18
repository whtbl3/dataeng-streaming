# Spec 002: Order domain with CDC (Postgres → Debezium → Kafka → bronze)
Status: Implemented (2026-11-18) · Intent: roadmap item 2 · Author: project owner + Claude

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
The pipeline only knows where vans are, not what they are delivering. Orders live in an
operational database owned by "another team" (ADR 0004). To predict SLA breaches later
(feature 6), every order change must reach the lakehouse without touching that
application. Also, the GPS track and the order lifecycle must tell the **same story**: a
van must actually be at the address when its order turns "delivered". Today's random-walk
vans cannot do that.

## Outcome
- An order database holds depots, vans and orders.
- A seeded simulator runs a delivery day: orders are created, assigned to a van, driven
  out, then delivered or failed.
- The same simulation drives the vans' GPS, so movement and statuses agree.
- Every insert and update is captured by change data capture into Kafka, and lands in
  an append-only bronze changelog.

## Users and scenarios
- **Analyst:** sees the full history of every order (each change, with before and after state) in SQL.
- **Feature 4/6 (later):** builds current order state and ETAs from this changelog plus GPS.
- **Interviewer:** sees a realistic OLTP → CDC → lakehouse path, including snapshot vs streaming and replication slots.

## Acceptance criteria
- [ ] AC1: Given a fresh stack with the CDC components enabled, when it starts, then the order database, CDC connector and topics are healthy, and seed data exists (4 depots; 200 vans whose ids match the GPS vehicle ids).
- [ ] AC2: Given a seed, when a simulated day runs, then every order follows a valid lifecycle (created → assigned → out for delivery → delivered or failed), with non-decreasing timestamps and a promised window that starts after creation. Each order is served by a van of its own depot, and the same seed gives identical output (unit test).
- [ ] AC3: Given a simulated day, when an order becomes delivered or failed at time t, then that van's GPS ping at t is within 50 m of the destination. Vans start and end their shift at their depot (unit test).
- [ ] AC4: Given default settings, when a day is simulated, then about 10% (±3%) of delivered orders arrive after their promised window and about 2% fail, so later SLA alerts have true positives. Both rates are configurable (unit test).
- [ ] AC5: Given the stack is running, when the simulator makes N order changes, then the bronze changelog contains exactly N new rows for them. Each row has the operation type, before and after state, the source commit position and commit time, and per order the rows keep commit order.
- [ ] AC6: Given rows that existed before the connector started, when it starts, then they are captured once as snapshot reads.
- [ ] AC7: Given the CDC connector is stopped, when M order changes are written and the connector is started again, then all M changes arrive and none are duplicated.
- [ ] AC8: Given the stack is running, when the developer asks for CDC status, then they see the connector state and replication slot lag. Removing the local stack (`make clean`) leaves no replication slot behind.
- [ ] AC9: Given the core and CDC components run with the dev-size simulation, then total container memory stays under 6 GB.

## Out of scope
- Avro + Schema Registry for CDC topics (feature 3; JSON for now).
- Current-state ("latest version") order tables and dedup (feature 4).
- ETA, SLA-risk and abnormal-stop alerts (feature 6).
- Road-following routes (stretch); vans drive straight lines between stops.
- The outbox pattern and hard deletes (orders are never deleted, only change status).
- Backfill at 30-day scale (feature 5).

## Decisions that change
- Spec 001, resolved question 2 (vans move by random walk) is replaced: vans on shift
  follow their assigned stops. Random walk is no longer used.

## Resolved questions (approved 2026-11-18)
1. One coupled simulator drives both GPS and orders: **yes**.
2. Movement: **straight lines** depot → stops → depot at 20–40 km/h with random traffic slowdowns.
3. CDC captures **depots, vans and orders** with one connector.
4. **Full before-images** on order updates.
5. Bronze: **one changelog table per source table**, typed row columns + CDC metadata, append-only.
6. Parked vans **keep pinging every 5 s** at the depot with speed 0 (keeps ADR 0009 volumes).
7. Clock: **realtime** and **history** modes; business times use the simulated clock, CDC commit time is wall-clock.
