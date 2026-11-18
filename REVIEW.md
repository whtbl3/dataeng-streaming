# Review instructions (for Claude PR review)

## Passes
Run three passes and tag each finding with its pass:
- **Bugs:** logic errors, broken edge cases, regressions.
- **Security:** injection, auth gaps, secrets or PII in code, logs or errors.
- **Compliance:** the change matches `specs/NNN-*/spec.md` and `plan.md`;
  nothing from "Out of scope" was built; every acceptance criterion has a test.

## Severity
- **Important:** breaks behavior, leaks data, breaks a spec criterion or a rule in CLAUDE.md.
- **Nit:** style, naming, small readability. Report at most five; summarize the rest as a count.

## Do not report
- Anything CI already enforces (lint, format).
- Generated files and lockfiles.

## Feedback loop
If the same mistake appears in two PRs, propose a one-line addition to CLAUDE.md.
