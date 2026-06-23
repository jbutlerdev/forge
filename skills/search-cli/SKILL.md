---
name: search-cli
description: SearXNG-backed web search via the `search` CLI (mule-ai/search). Use when you need up-to-date information from the public web, look up library/API docs, research a topic, or fetch a list of authoritative URLs to `read` / `curl`.
---

# Search CLI — Agent instructions

The `search` binary is on `$PATH` in the sandbox (it was built
from the v1.0.1 release of <https://github.com/mule-ai/search>
and lives at `/usr/local/bin/search`). It is a SearXNG
metasearch CLI: one invocation, JSON / Markdown / plain-text
output, no scraping, no rate-limit dances against a single
search engine.

The default instance is `https://search.butler.ooo`. Override
with `-i <url>` or the `SEARCH_INSTANCE` env var. An API key
can be passed with `--api-key` or `SEARCH_API_KEY` (only
required for instances with auth enabled).

## Basic usage

```bash
search "<query>"
```

Example:

```bash
search "golang tutorials"
```

## Output formats

The tool supports three output formats.

### Text (default)

Human-readable plain text:

```bash
search "machine learning"
```

### JSON

Machine-readable JSON for parsing. This is the format you
should reach for when you'll be piping results into `jq`,
following URLs with `read` / `curl`, or extracting
structured data (infoboxes, attributes) into shell
variables.

```bash
search -f json "rust programming" | jq '.results[] | .title, .url'
```

JSON output structure:

```json
{
  "query": "rust programming",
  "results": [...],
  "infoboxes": [...],
  "answers": [],
  "suggestions": [],
  "total_results": 0,
  "metadata": {
    "instance": "https://search.butler.ooo",
    "search_time": "0.41s"
  }
}
```

### Markdown

Formatted markdown output:

```bash
search -f markdown "python async"
```

## Key flags

### Result count

`-n, --results <int>` — number of results to return (default 10).

```bash
search -n 20 "docker containers"
```

`--page <int>` — page number for pagination (default 1).

```bash
search --page 2 "kubernetes"
```

### Category

`-c, --category <string>` — search category (default
`general`). Available: `general`, `images`, `videos`, `news`,
`map`, `music`, `it`, `science`, `files`, `social media`.
(Run `search categories` for the full list with
descriptions.)

```bash
search -c images "cute cats"
search -c news "technology trends"
search -c "social media" "trending topics"
```

### Language

`-l, --language <code>` — ISO language code (default `en`).

```bash
search -l de "machine learning"
```

### Time range

`--time <range>` — filter by `day` / `week` / `month` / `year`.

```bash
search --time week "ai breakthrough"
```

### Instance

`-i, --instance <url>` — use a specific SearXNG instance.

```bash
search -i https://searx.work "privacy tools"
```

A list of public instances is at <https://searx.space/>.

### Safe search

`-s, --safe <level>` — 0=off, 1=moderate, 2=strict.

```bash
search -s 0 "medical research"
```

### Caching

`--cache` / `--no-cache` — enable / disable per-process
caching. `--clear-cache` clears before searching;
`--cache-stats` shows the cache state. Default is cached
(5-minute TTL, 100 entries).

```bash
search --no-cache "latest news"
```

### Output options

- `--no-color` — strip ANSI from output
- `--open` — open the first result in `$BROWSER`
- `--open-all` — open every result in `$BROWSER` (you will
  almost never want this from a sandbox)

### Performance & debugging

- `-v, --verbose` — verbose stderr
- `-t, --timeout <seconds>` — request timeout (default 30)

```bash
search -v -t 60 "complex query"
```

## Understanding results

### When no results are found

The default SearXNG instance may have some engines
rate-limited by CAPTCHA. The tool still surfaces:

1. **Infoboxes** — structured data from Wikidata,
   Wikipedia, etc. Often a comprehensive topic overview
   on its own.
2. **Answers** — direct answers from computational engines.
3. **Suggestions** — alternative queries the upstream
   SearXNG thinks are related.

Example output when engines are blocked:

```
go programming
==============

No results found.

## Go

programming language developed by Google and the open-source community

  • Inception: Tuesday, November 10, 2009
  • Developer: The Go Authors, Robert Griesemer, Rob Pike, Google

  Links:
  ★ Official website
      https://go.dev
  ...
```

### Infobox components

Infoboxes carry:

- **Title / name** — the subject
- **Content** — description / summary
- **Attributes** — key/value pairs (founded date, CEO,
  version, etc.)
- **Links** — official sites, Wikipedia, repositories,
  social media. Lines marked with `★` are the primary /
  official source.

## Best practices for agents

### 1. Use JSON for programmatic access

When you need to parse results, do the work in `jq`
rather than re-implementing text parsing:

```bash
# Titles and URLs of the first 10 results
search -f json "golang" | jq -r '.results[] | "\(.title) -> \(.url)"'

# Just the URLs (useful for `read` / `curl` follow-ups)
search -f json "golang" | jq -r '.results[].url'

# Infobox attributes
search -f json "rust language" | jq '.infoboxes[].attributes'

# Total result count (useful to decide whether to refine)
search -f json "topic" | jq '.total_results'
```

### 2. Adjust result count to the task

- Quick lookup: `search -n 5 "query"`
- Research sweep: `search -n 30 "query"`

### 3. Use time filters for current events

```bash
search --time day "breaking news"
search --time week "tech updates"
```

### 4. Use categories when you know the type

```bash
search -c news "election"
search -c images "sunset"
search -c videos "tutorial"
```

### 5. Handle rate limiting

If the default instance returns a CAPTCHA wall:

- Try `search --no-cache "..."` — cached empty results are
  the most common cause of "still empty" surprises
- Try a different instance: `search -i https://searx.work "..."`
- See <https://searx.space/> for a status board of public
  instances

### 6. Combine with other tools

```bash
# Pull the first official-looking link and fetch its
# body via the `read` tool. JSON-then-read is the
# canonical "search then summarize" pattern.
url=$(search -f json "kubernetes networking" | jq -r '.results[0].url')
read "$url"

# Chain a follow-up search off the infobox summary.
search -f json "python" | jq -r '.infoboxes[0].attributes[]' \
  | grep -i version
```

## Common workflows

### Research a topic

```bash
# Comprehensive results with infoboxes
search -n 20 "artificial intelligence"

# Recent news
search -c news --time week "ai regulation"

# Official documentation
search "golang official documentation"
```

### Find resources

```bash
# Tutorials
search "golang tutorial beginner"

# Libraries / packages
search "python http client library"

# Examples
search "react hooks examples"
```

### Quick facts

The infobox often provides structured facts directly:

```bash
search "python programming language"
search "linux kernel"
```

### Troubleshooting

```bash
# Error messages
search "docker error container already exists"

# Version info
search "go 1.21 release notes"

# Documentation
search "kubernetes pod networking"
```

## Performance tips

1. Caching is on by default; repeated queries are cheap
   (5-minute TTL, 100 entries).
2. Increase timeout for slow instances: `-t 60`.
3. Reduce result count for quick lookups: `-n 5`.
4. Pick a specific category when you know the type — the
   upstream SearXNG is faster and the result set is more
   focused.

## Troubleshooting

### "No results found" but infoboxes appear

Normal. Some engines are rate-limited; the infoboxes
still provide structured data.

### Slow responses

- Try a different instance: `-i https://searx.work`
- Reduce timeout: `-t 15`
- Reduce result count: `-n 5`

### Instance unavailable

Check <https://searx.space/> for the current status of
public instances.

## Advanced usage

### Batch queries

```bash
for term in "golang" "rust" "python"; do
    echo "### $term"
    search -n 5 "$term"
done
```

### Logging

```bash
search -v "query" 2>&1 | tee search_log.txt
```

## Summary

- **Binary:** `/usr/local/bin/search` (sandbox `$PATH`)
- **Default format:** text — use `-f json` for parsing
- **Default instance:** `https://search.butler.ooo`
- **Infoboxes** are returned even when main results are
  empty
- **Categories:** `general`, `images`, `videos`, `news`,
  `map`, `music`, `it`, `science`, `files`, `social media`
- **JSON output** is recommended for any follow-up tool
  call (read / curl / jq)
- **Multiple instances** are available if the default
  is rate-limited

For more help:

```bash
search --help
```

The upstream documentation is at
<https://github.com/mule-ai/search> (the `SYSTEM.md` and
`README.md` files in that repo are the canonical source —
this file is a snapshot of those, pinned to v1.0.1).
