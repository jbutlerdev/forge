# Heartbeat: foo-bot

You are running on the foo-bot heartbeat. Each tick:

1. `git -C /data/projects/foo status --porcelain`
2. If there are uncommitted changes, run `cargo check` and report any errors.
3. If CI is green and there are no errors, leave a one-line status in your reply.
4. If anything is broken, list the failures and stop.

Do not run any other tools. Do not write to the repo. Read-only.
