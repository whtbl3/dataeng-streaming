# Diagrams

Each diagram has a source file (edit this) and a static SVG (shown in the main README).
The interactive HTML viewers are generated and not committed (`.gitignore`).

| Diagram | Source | Type | Question it answers |
|---|---|---|---|
| `data-flow.svg` | `data-flow.dataflow.json` | dataflow | Where does each event go, and what is built or planned? |
| `gps-ping.svg` | `gps-ping.sequence.json` | sequence | What happens to one ping, a duplicate and a bad message? |
| `order-lifecycle.svg` | `order-lifecycle.lifecycle.json` | lifecycle | Which statuses can an order reach, and in what order? |
| `model-silver.svg` | `model-silver.erd.json` | erd | Which columns and keys do the source and silver tables have? |
| `model-gold.svg` | `model-gold.erd.json` | erd | How will the gold star schema join (planned, feature 7)? |

Facts come from the code: `infra/local/postgres/init/001_schema.sql`, `infra/local/trino/init/*.sql`,
`flink/jobs/*.sql`, `contracts/` and `simulator/orders_db.py`. The gold model follows
`docs/requirements.md` section 4; its other columns and types are a draft.

## Regenerate after an edit
The Archify skill lives in `.claude/skills/archify/` and needs Node 18+ and Chrome or Chromium
(set `ARCHIFY_CHROME` to its path if it is not on `PATH`). For example:

```bash
node .claude/skills/archify/bin/archify.mjs finalize dataflow \
  docs/diagrams/data-flow.dataflow.json docs/diagrams/data-flow.html --quality showcase
```

Use the type from the table, and `--quality standard` for `model-silver` (a dense ERD).
Then open the HTML, choose **Export → SVG · Auto** (it follows the reader's light or dark theme),
and save it over the `.svg` file.

Keep the *built* / *planned* tags in step with [the roadmap](../roadmap.md).
