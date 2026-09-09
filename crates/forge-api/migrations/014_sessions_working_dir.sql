-- Optional working-directory anchor for sessions. When set, the
-- session's agent runs directly in this directory (host-side, no
-- per-session tree) — for integrations that attach an agent to an
-- existing checkout (e.g. ranch agent panes anchored to the terminal
-- pane's cwd). NULL keeps the default per-session isolated tree.
ALTER TABLE sessions ADD COLUMN working_dir TEXT;
