# Intent: Delivery Control Tower
Author: project owner · Date: 2026-11-17 · Status: approved (2026-11-17)

## Problem
A last-mile delivery company runs a fleet of vans that deliver parcels across a city.
Today, dispatchers learn that a delivery is late only after the promised window has
passed. Vehicle GPS pings and order status changes live in separate systems, so nobody
can see in real time where each parcel is or whether its ETA still fits the
promised window. Operations managers get KPIs (on-time rate, stops per hour) from a
nightly batch job, one day late.

## Users
- **Dispatcher** (real time): watches active routes, needs an alert *before* an order
  breaches its SLA so they can re-route or call the customer.
- **Operations manager** (hourly/daily): reviews on-time rate, ETA accuracy, and
  driver/depot performance.
- **Data analyst**: queries clean, historical delivery data with SQL.

## Desired outcome
- Every in-flight order has a live ETA, updated from the latest GPS position.
- An "at-risk" alert fires when the predicted ETA crosses the promised window, at
  least 15 minutes before the actual breach.
- Abnormal stops (vehicle idle > N minutes outside a delivery point) are flagged.
- A lakehouse holds raw → cleaned → business-ready tables, and late or duplicate
  events do not corrupt the KPIs.
- A dashboard shows on-time %, ETA error, and active alerts.

## Out of scope
- Route optimisation or vehicle routing algorithms.
- A driver mobile app or a customer-facing tracking page.
- ML-based ETA models (v1 uses a simple distance/speed heuristic; ML can come later).
- Real customer data; all data is synthetic.
- Multi-region, high availability, or production SLAs.

## Constraints
- **Timeline:** 4–6 weeks, solo, part-time.
- **Budget:** the whole stack runs locally for $0 (laptop, WSL2). The managed AWS
  deployment is created on demand once the pipeline is stable and destroyed afterwards,
  at under ~$5 per demo session (~$190 credit available).
- **Cloud:** AWS. Each local component must map to a named AWS managed service.
- **Data:** a simulator generates GPS pings and order events, including late,
  out-of-order and duplicate events on purpose.
- **Audience:** this is a portfolio project for Junior/Mid Data Engineer interviews,
  so design decisions and their trade-offs must be documented (ADRs).

## Success measures
- `make up` + `make demo` starts the whole pipeline locally in under 10 minutes on a
  fresh machine.
- The design scales to ~100M GPS events (30 days): every layer that would stop it from growing is
  fixed and explained (ADR 0012). *Changed 2026-11-19 (spec 005): a measured 30-day backfill and
  benchmark were dropped; they are a stretch item.*
- End-to-end latency (GPS event → alert) p95 under 10 seconds locally.
- Duplicate and late events give the same gold KPIs as clean data: intraday for events
  up to 2 h late, and in the nightly final close for anything that arrived that day (proven by tests).
- Data quality checks run in CI and block bad models.
- `terraform apply` / `destroy` deploys and removes the AWS version cleanly.
- The README explains the architecture, trade-offs and cost in a 5-minute read.

## Resolved questions
- Simulated city: a real bounding box (Ho Chi Minh City, OpenStreetMap road data).
- Scale target: 200 vans, ~5k orders/day, 1 GPS ping per van every 5 s (live).
- Data tiers (ADR 0009): `dev` ≈ 350k, `standard` = 30 days ≈ 100M, `stress` = backfill + 2,000 vans live. Since spec 005 these are design sizes, not runnable tiers (stretch).
- Runtime: local-first on this laptop; managed AWS services once stable (ADR 0005).
