# PR Runbook

This is the intended GPT/Codex workflow for APE.

1. GPT gives one bounded PR prompt.
2. Codex implements the scoped change and opens/pushes the PR.
3. Kenneth reviews and merges the PR.
4. GPT provides Windows PowerShell validation commands.
5. Kenneth runs the commands and pastes the output.
6. GPT evaluates pass/fail, updates Notion, and only then generates the next PR prompt.

Rules:

- Keep each PR bounded.
- Do not skip validation.
- Do not move to the next PR until the current PR has been reviewed, merged, and validated.
- Do not introduce live trading, paper trading, execution, secrets, external market data calls, or deployment behavior unless that PR explicitly authorizes it.
- Database schema/repository changes are allowed only in PRs that explicitly authorize storage work.
