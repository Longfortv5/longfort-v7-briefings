# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository purpose

Public, content-only repo. It holds **daily pre-open market briefings** (markdown) authored by Perplexity Computer ("PC", Intelligence Chief role) for the Longfort Desk. There is no application code, no build, no tests, and no CI in this repo.

A briefing is fetched unauthenticated by **n8n Workflow #1** (raw GitHub fetch) and posted to Slack `#desk` at **06:00 BST Mon–Fri**. The downstream consumer is the only reason files exist here, so the file path and format are part of the contract — see "Briefing file convention" below.

## Hard scope lock (do not violate)

Per `README.md` this repo is public and intentionally narrow. Never add:

- Code or scripts of any kind (no `.py`, `.js`, `.go`, `.sh`, no n8n JSON, no Dockerfiles, etc.)
- Credentials, tokens, API keys, droplet IPs in a credential context, broker IDs
- Position-level detail (sizes, P&L, fills, account balances)
- Strategy logic, thresholds, gating rules, or anything that would let a reader reconstruct the bot's edge

The existing briefing references operational facts (mode = DRY_RUN, signal counts, hazard names like `D11`/`CloseAllPositions`, vendor costs) — those are allowed because they are advisory context, not strategy. When in doubt, leave it out and ask.

Also: the briefing footer carries the rule **"£58k loss not referenced (HS lockdown rule)."** Do not introduce that figure or its synonyms anywhere.

## Briefing file convention

- Location: `comms/briefings/`
- Filename: `YYYY-MM-DD_preopen.md` where the date is the **session the briefing is for** (the next London cash open), not the date of authorship.
- One file per session. Re-baselines (e.g. a Sunday-evening update before Monday open) are revisions inside the same file, not new files — bump the `Revision:` header line and note the trigger.

A trivial smoke-test stub that lives only briefly is acceptable (see commit `3fa8abe` → `5a29803`) when verifying the n8n raw-fetch path, but it must be removed in a follow-up commit.

## Briefing template

The Monday 20 Apr 2026 briefing (`comms/briefings/2026-04-20_preopen.md`) is the canonical template. New briefings should preserve its section order and headers so n8n / Slack rendering stays stable:

1. Title line: `# <Day> <DD Mon YYYY> — Pre-Open Intelligence Briefing (v<n>)`
2. Header block: `Prepared by`, `For`, `Time prepared` (with T-minus to London cash open), `Revision`, `Valid for`
3. `## 1. TL;DR (60 seconds)` — bulleted, every claim either live-probed or linked
4. `## 2. Weekend Tape` (or session tape) — tables of instrument / close / move / driver
5. `## 3. Macro Calendar` — week-ahead table in **London time (BST)**, with cited sources
6. `## 4. Bot State` — point-in-time snapshot table, sourced from a live probe
7. `## 5. Known Hazards (D-Locks)` — `D<n>` named hazards
8. `## 6. Chief's First Heartbeat` / heartbeat expectations
9. `## 7. Weekend Alert Noise` (root-cause notes)
10. `## 8. Open Questions for HS` — decision-ready, no PC opinion
11. `## 9. Data Stack` — vendor table
12. `## 10. Routing` — per-actor action list
13. Footer: `*End of briefing. No fabrication. Numbers are live-probed or cited. £58k loss not referenced (HS lockdown rule).*`

Sections may be omitted if genuinely empty, but keep the numbering contiguous.

## Authoring rules carried over from the briefings themselves

- **No fabrication.** Every number must be either (a) live-probed from a system PC controls, or (b) inline-linked to a public source. The footer asserts this — keep it true.
- **All times in London time (BST/GMT as appropriate).** Convert vendor calendars before quoting.
- **Cite inline, not in a bibliography.** Use `[label](url)` next to the claim.
- **Open questions stay opinion-free.** Section 8 is for HS to decide; PC presents options, not recommendations.

## Actors / glossary

These short-codes appear throughout the briefings; preserve them verbatim:

- **HS** — human principal / desk owner
- **Chief** — Opus 4.7 advisory agent (London + NY session cadence)
- **PC** — Perplexity Computer (author of these briefings)
- **CC** — Claude Code (engineering on the bot repo, not this one)
- **Red** — Gemini cross-checker, flag-only, no veto
- **Windsurf**, **Cursor** — engineering tools held in reserve

## Working in this repo

- Default branch is `main`. Feature/agent work goes on a named branch and lands via PR.
- There is nothing to build, lint, or test. A "good" change is a markdown file that renders cleanly on GitHub and parses for n8n's raw fetch (i.e. valid UTF-8, no front-matter unless added intentionally).
- Before committing a new briefing, sanity-check: filename matches `YYYY-MM-DD_preopen.md`, header block is filled in, footer line is present, and every numeric claim is cited or sourced from a probe.
