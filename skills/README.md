# Skills

Pi skill packs for forge sessions. Each subdirectory is one
skill, named after the directory, containing a `SKILL.md`
markdown file with the skill's instructions.

A skill is loaded by the agent on demand — it's a way to
expose reference documentation for a tool the LLM might
need without burning context on every turn. Pi's skill
system is documented in the upstream pi package; in
short: a skill is a `SKILL.md` in a subdirectory, and the
agent sees the skill's name + description (from the
YAML frontmatter) in its skill list.

## How skills are loaded

`forge-api` passes the `skills/` directory at the repo
root to pi as `--skills-dir` when it spawns the agent
subprocess. Operators can override the location with
`FORGE_SKILLS_DIR` (see `docs/SEARCH-TOOL.md` for the
full env-var contract).

If neither the override nor any of the default fallback
paths resolve to an existing directory, forge-api logs a
warning and the agent runs with `--no-skills` (the legacy
behavior).

## Adding a new skill

1. `mkdir skills/<skill-name>/`
2. Create `skills/<skill-name>/SKILL.md` with a YAML
   frontmatter at the top:

   ```yaml
   ---
   name: <skill-name>
   description: <one-line description the agent sees in the skill list>
   ---
   ```

   The directory name and the `name:` field should match
   (pi uses both; the directory name is the file path
   and the frontmatter `name` is what shows up in the
   agent's UI).

3. Below the frontmatter, write the skill's body —
   whatever instructions, flag references, worked
   examples, etc. the agent should have when it loads
   the skill. Skill bodies are regular markdown; pi
   renders them as part of the agent's context.

4. No Rust change is required. The next time
   forge-api spawns a session, the new skill shows up
   in the agent's skill list automatically.

## What doesn't go here

- **Tool binaries.** Skills are documentation, not
  code. To add a new command to the sandbox, edit
  `sandbox/default.nix` and re-run `./sandbox/build.sh`
  (see `AGENTS.md` §15 for the package-management
  workflow).
- **TypeScript extensions.** Extensions are loaded by
  pi via `--extension` and live under `extensions/`,
  not here. The two are deliberately kept separate:
  extensions run TypeScript code in the pi host
  process; skills are pure markdown read by the LLM.

## Existing skills

- [`search-cli/`](search-cli/) — the
  [`mule-ai/search`](https://github.com/mule-ai/search)
  CLI. Flag reference, JSON-output patterns, and the
  "combine with `read` / `curl` / `jq`" workflows the
  agent should reach for when a task needs an
  up-to-date web result.
