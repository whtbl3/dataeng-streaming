# Diagrams

| File | What it is |
|---|---|
| `data-flow.dataflow.json` | Source of the data-flow diagram (Archify JSON). Edit this file. |
| `data-flow.html` | Interactive viewer, generated. Open it locally in a browser. |
| `data-flow.svg` | Static export, generated. Shown in the main README. |

## Regenerate after an edit
The Archify skill lives in `.claude/skills/archify/` and needs Node 18+ and Chrome or Chromium
(set `ARCHIFY_CHROME` to its path if it is not on `PATH`).

```bash
node .claude/skills/archify/bin/archify.mjs finalize dataflow \
  docs/diagrams/data-flow.dataflow.json docs/diagrams/data-flow.html --quality showcase
```

Then open `data-flow.html`, choose **Export → SVG · Auto** (it follows the reader's light or dark theme), and save the file
as `docs/diagrams/data-flow.svg`. Delete the generated `*.delivery.json` and `*.finalize*.json`
receipts: they hold absolute local paths.

Keep the *built* / *planned* tags in step with [the roadmap](../roadmap.md).
