# Generic Prompt: Codebase → AI Agent Skill Generator

Use this for ANY framework/codebase, not just one specific stack. Copy everything in the PROMPT block into Copilot Chat (Workspace/Agent mode — it needs to read across many files). Replace `[FRAMEWORK_NAME]` before sending. Run once per repo.

---

## PROMPT START

You are converting this codebase into a **"Skill"** — a structured knowledge package for an AI coding agent (Claude). The goal: someone who has never seen this code should be able to ask the agent to (a) debug/root-cause a failure, (b) extend or build something new on top of this framework, and (c) explain how any part of it works — all without the agent ever needing to read the raw source directly. The skill IS the agent's substitute for reading the codebase.

This must work like documentation written by the most senior engineer on this codebase, for another senior engineer joining cold — not a tutorial, not auto-generated boilerplate, not a restatement of file names.

### Step 1 — Explore before writing anything

Read the codebase broadly before producing output:
- Entry points, build/deploy config, README/wiki/CHANGELOG, architecture docs if any
- Core abstractions: interfaces, base classes, traits, plugin/extension points
- How the framework is invoked/configured (CLI args, config files, env vars, API calls — whatever applies)
- How it's deployed/executed/run in practice (local, cluster, container, service — whatever applies)
- Error handling, validation logic, exception types, logging statements
- Tests and fixtures — these often reveal real usage patterns and edge cases better than the main code does
- Any accessible incident history, postmortems, or troubleshooting docs

Do not assume the structure in advance. **Decide for yourself what categories of knowledge this specific framework actually needs** based on what you find — don't force-fit a template that doesn't match the codebase's real shape. Use your judgment on naming and grouping.

### Step 2 — Decide the skill's shape

Before writing files, answer these for yourself (you don't need to show this reasoning, just use it to decide structure):
- What are the 3-6 things someone would actually come to this agent asking for? (e.g. "why did X fail", "how do I add a new Y", "what does this config field do", "how does Z work end to end")
- What's genuinely complex/non-obvious enough to need a reference file vs. what's simple enough to fit in the main overview?
- Does this framework have one config/input format, multiple, or none (e.g. pure library/API)? Document whatever actually exists — don't invent a config schema if there isn't one.
- Does it have meaningful failure modes worth cataloging, or is it simple enough that this isn't needed?
- Are there genuinely distinct modules/domains that deserve separate files, or is it cohesive enough for one file?

### Step 3 — Produce the skill package

Create actual files (not chat output) in a folder named `[FRAMEWORK_NAME]-skill/`, structured using progressive disclosure:

```
[FRAMEWORK_NAME]-skill/
├── SKILL.md              ← always loaded by the agent — keep lean, <500 lines
└── references/            ← loaded only when relevant to the question
└── scripts/                ← optional: templates, scaffolds, validators, generators
└── assets/                 ← optional: example configs, sample files
```

**`SKILL.md` must contain:**
- YAML frontmatter with `name` and `description`. The description is the ONLY thing that determines whether the agent ever opens this skill — it must clearly state what the skill covers and list concrete trigger scenarios (debugging X, building Y, questions about Z), phrased assertively so the agent doesn't under-trigger on relevant questions. Bad: "Documentation for [FRAMEWORK_NAME]." Good: "Use whenever the user is debugging a failure in [FRAMEWORK_NAME], extending it with a new [whatever the extension unit is], or asking how any part of [FRAMEWORK_NAME] works — covers architecture, configuration, common failure modes, and how to add new [extension units]."
- A concise framework overview: what it does, the core execution flow end to end, in plain language.
- A map of core abstractions (real names from the code, one line of purpose each) — only the ones a newcomer actually needs to orient themselves.
- An explicit routing section: for each reference file you create, state in one line exactly when the agent should open it. The agent should never have to guess which file answers which kind of question.
- No large code blocks, no exhaustive file-by-file walkthroughs, no padding. Push all depth to references/.

**For each reference file you decide to create**, apply the same standard: real names from the code, worked examples over abstract description, explicit about what's inferred vs. confirmed, honest about gaps rather than invented detail. Typical candidates (use only what fits — do not create a file just to match this list):
- Deeper architecture / call-chain detail
- Configuration/input format reference (only if one exists — document its actual shape, don't assume JSON or any specific format)
- Failure modes / troubleshooting catalog, built from real exception types, validation messages, and log patterns in the code — format each entry as: signature → likely cause → where it originates → fix → related config/code if applicable
- Extension/plugin guide: which interface to implement, minimal required methods with real signatures, one fully worked example of adding something new
- Per-module reference files, if the codebase has genuinely distinct subsystems large enough to warrant splitting

**`scripts/` and `assets/`** — include only if genuinely useful: e.g. a scaffold template for adding a new extension, a config template, a validation script. Skip entirely if nothing in the codebase warrants it.

### Constraints (apply throughout)
- Never paste large verbatim code blocks — short signatures (≤10 lines) only, where essential.
- Never invent capabilities, config fields, or failure modes that aren't grounded in the actual code. If something is uncertain or inferred rather than directly observed, say so explicitly in that file.
- Keep `SKILL.md` itself lean — if it's approaching 500 lines, that's a signal to push more content into reference files instead.
- Use the real class/method/field/CLI-flag names from the codebase throughout, not generic placeholders.
- If the codebase is too large to fully explore in one pass, say so explicitly and propose how to split the work (e.g. by module) rather than producing a shallow result silently.

## PROMPT END

---

### After Copilot generates this
1. Check whatever Copilot decided was the "failure modes / troubleshooting" file first — this is usually the highest-value and weakest-from-code-alone file, since it can't see your incident history or tribal knowledge. Add what you know from memory.
2. Skim `SKILL.md`'s description line specifically — this is what determines whether the agent actually reaches for the skill, so make sure it's concrete and not vague.
3. Bring the generated folder back here and I'll help you test it against real questions, tighten triggering, and package it into an installable `.skill` file.
