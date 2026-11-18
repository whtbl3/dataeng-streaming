---
name: new-feature
description: Spec-first flow for a new feature. Creates specs/NNN-name/spec.md, stops for approval, then plan.md and tasks.md, stops again, then builds test-first. Run only when the user types /new-feature.
disable-model-invocation: true
argument-hint: "<issue number or one-line intent> | plan <NNN>"
---

# New feature (spec-driven)

Skip this whole flow for bug fixes, docs, and any change you can describe in one sentence.

## Mode A: `/new-feature <issue # or intent>` → write the spec

1. Read the intent: the GitHub issue (`gh issue view <n>` if available) or the given text.
2. Read `docs/project-spec.md`, `docs/roadmap.md` and any ADR the feature touches.
   If the feature changes an existing decision, list it under "Decisions that change".
3. Pick the next number: highest `specs/NNN-*` + 1, zero-padded to 3 digits.
   Create branch `feat/NNN-short-name` and folder `specs/NNN-short-name/`.
4. Copy `specs/_template/spec.md` and fill it. **What and why only**: no file names,
   no code, no SQL. Acceptance criteria in Given / When / Then form.
5. Put every uncertainty under "Open questions", each with your recommended answer.
6. **STOP.** Present the open questions. Do NOT write plan.md or any code until the user
   explicitly approves the spec. Loop on the spec until approved, then set Status: Approved.

## Mode B: `/new-feature plan <NNN>` → plan and tasks

7. Require spec Status: Approved. If not, stop and say so.
8. Use plan mode. Read the code the change touches. Fill `plan.md` from the template:
   files that change, order of work, risks **and rollback**, how each criterion is proven.
9. Break the plan into `tasks.md`: small tasks, each independently testable, tests first.
10. **STOP.** Ask the user to approve plan and tasks.

## Mode C: build (after plan approval)

11. Work task by task. For each: write the failing test, make it pass, tick the box.
12. If you depart from the plan, update `plan.md` in the same commit.
13. Run `make check` and paste the output before reporting done.
14. Open the PR with the template; link `specs/NNN-*/spec.md`; update
    `docs/project-spec.md` and `docs/roadmap.md`; set spec Status: Implemented.
