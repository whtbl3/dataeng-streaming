---
name: bootstrap-project
description: One-time setup of a new project from this template. Interviews the user, writes docs/intent.md, drafts the project spec and tech stack, then fills every {{PLACEHOLDER}}. Run only when the user types /bootstrap-project.
disable-model-invocation: true
argument-hint: "[one-line idea]"
---

# Bootstrap a new project

You are turning this template into a real project. There are **three stops** where you
must wait for the user's explicit approval. Never skip a stop.

## Phase 1 — Intent (Plan stage)

1. If `$ARGUMENTS` has an idea, start from it. Otherwise ask for a one-line idea.
2. Interview the user **one question at a time** (use AskUserQuestion when available):
   problem, users, desired outcome, what is OUT of scope, constraints (budget, deadline,
   compliance, data), how success is measured. Do NOT propose technology yet.
3. Write `docs/intent.md` using its existing headings. Keep it under ~60 lines.
4. **STOP 1.** Show a short summary and ask: "Approve the intent, or what should change?"
   Loop until approved. Then suggest the user commit it.

## Phase 2 — Spec and stack (Design stage)

5. Draft `docs/project-spec.md` from the approved intent (what and why; architecture at
   box level; key data entities; non-functional requirements; Definition of Done).
6. For the tech stack, present **2–3 options with trade-offs** for each big decision
   (language/framework, storage, hosting, test tool). Let the user choose. Record the
   choice and the reason in `docs/tech-stack.md`. Each decision that is hard to reverse
   gets an ADR in `docs/decisions/NNNN-short-title.md` (copy `0000-template.md`).
7. Draft `docs/roadmap.md`: features in order, one line each, first item = walking skeleton.
8. **STOP 2.** Ask the user to approve spec, stack and roadmap. Loop until approved.

## Phase 3 — Fill the template (Build setup)

9. Replace every `{{PLACEHOLDER}}` in: `CLAUDE.md`, `Makefile`, `.github/workflows/ci.yml`,
   `.pre-commit-config.yaml`. Find them with: `grep -rn "{{" --exclude-dir=.git .`
   (ignore this skill file and README.md).
10. Adjust `.claude/hooks/format_after_edit.py` HANDLERS and `.claude/settings.json`
    allow-list to the chosen stack (e.g. add `Bash(uv run pytest*)` or `Bash(npm test)`).
11. Rename or delete `.claude/rules/example-path-rule.md` to match the real layout.
12. Run `bash scripts/test_hooks.sh`. It must pass.
13. **STOP 3.** Show a list of changed files and remaining manual GitHub steps from
    README.md ("Manual setup on GitHub"). Ask before building the walking skeleton.

## Phase 4 — Walking skeleton (only after STOP 3 approval)

14. Enter plan mode. Build the thinnest end-to-end slice from roadmap item 1, with one
    real test. `make check` must pass locally. Then propose a first commit/PR.

## Rules
- CLAUDE.md must stay short (target 20–40 lines). Do not paste the spec into it.
- Ask, don't assume, on anything that is expensive to change later.
- Write all files in the language recorded in CLAUDE.md (default: English).
