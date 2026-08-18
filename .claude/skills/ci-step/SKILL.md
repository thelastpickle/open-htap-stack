---
name: ci-step
description: Run one step or one section of the CI workflow locally against the running stack, instead of pushing and waiting fifteen minutes to find out. Use before pushing a change to .github/workflows/test-podman-compose.yaml, when CI has failed and you want the failing assertion in front of you, or when adding an assertion for a feature you just built.
user-invocable: true
allowed-tools:
  - Bash
  - Read
  - Edit
---

# Running a CI step by hand

There is one test suite, `.github/workflows/test-podman-compose.yaml`, and it is a single job.  A push costs about fifteen minutes to reach the dashboard step, so a failed assertion there is an expensive way to learn about a quoting mistake.  Extract the step and run it against the stack that is already up.

## Extract

```bash
python3 .claude/skills/ci-step/extract-step.py --list                        # the steps
python3 .claude/skills/ci-step/extract-step.py --step dashboard --list       # its === sections ===
python3 .claude/skills/ci-step/extract-step.py --step dashboard \
    --section "One window" > .ci-tmp/window.sh
bash -e .ci-tmp/window.sh
```

`--step` and `--section` match on a case-insensitive substring and refuse an ambiguous one.  A section is emitted **after its step's preamble**, because the later sections read helpers the preamble writes (`$TMP_DIR/jq-path.py`, `$TMP_DIR/check-benchmark.py`).  Without `--section` you get the whole step.

Run it under `bash -e`, which is what the runner does (`shell: /usr/bin/bash -e {0}`): a step can pass by accident under a shell that ignores a failing command.  `TMP_DIR` falls back to `.ci-tmp`, which is gitignored.  `mkdir -p` it first; a missing scratch directory is the usual first error.

The extracted script talks to `localhost:8000` and the container CLIs exactly as CI does, so the stack must be up.  It is verbatim, which is the point: an assertion retyped from memory is a different assertion.

## Writing an assertion that will hold on a runner

CI's stack is minutes old, its tables are small, its Kafka backlog is still draining, and its clock has moved on since the workflow was written.  Anything that holds on a laptop that has been ingesting for an hour is not thereby true there.

- **Assert structure, not timing.**  That a bulk read reused a snapshot is checkable; that it was faster than a fresh one is not, on a table of a few hundred thousand rows.
- **Ask the backend what to name, rather than computing it in YAML.**  The window section reads `/api/query/window` for the bucket, the shard count and whether that window has closed.
- **Guard on the condition, not on hope.**  The three paths agree exactly only once a window has closed, so that equality check runs `if window["closed"]`.  This is the bug that failed CI: the newest complete window predated ingest, so all three analytical paths honestly returned no rows.
- **Print the numbers even when you cannot assert them.**  `[5320.5 MB snapshot]` in the log is how the next failure gets diagnosed.

Fixtures live where the assertion can see them: write the response to `$TMP_DIR/benchmark-*.json` and check it with a small Python script rather than a `jq` expression nested in three levels of quoting.

## Traps

- Editing `podman-compose.yml` from a CI step: the Spark `command` is one single-quoted shell string, so an apostrophe anywhere in it, comments included, truncates the script; and compose interpolates `${...}` everywhere, comments included, so a shell variable needs its dollar doubled.
- podman on the runner has no systemd, so healthchecks never leave `starting`.  The workflow does its own ordering and waiting; do not add `depends_on: service_healthy` and expect it to work.
- The extractor keys on `      - name: ` at six spaces and on `run: |`.  Reindent the workflow and it stops finding steps, which it will tell you rather than guessing.
