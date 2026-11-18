# Feature specs

One folder per change: `specs/NNN-short-name/` with `spec.md`, `plan.md`, `tasks.md`.
Names match GitHub Spec Kit, so you can trial Kit on a branch and compare.

Flow: Intent issue → `/new-feature <#>` → spec.md → **you approve** →
`/new-feature plan NNN` → plan.md + tasks.md → **you approve** → build (tests first) → PR.

Skip for: bug fixes, docs, one-sentence changes.
Specs stay after merge (spec-anchored); docs/project-spec.md is updated by each feature.
