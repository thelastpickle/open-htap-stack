#!/usr/bin/env python3
"""Turn one part of the CI workflow into a script you can run against a live stack.

CI is one long job whose useful steps are large `run:` blocks, and the dashboard step
alone is hundreds of lines divided by `=== heading ===` echoes.  Retyping a fragment
of it by hand is how you end up testing something subtly different from what CI runs,
so this extracts it verbatim.

    python3 .claude/skills/ci-step/extract-step.py --list
    python3 .claude/skills/ci-step/extract-step.py --step dashboard --list
    python3 .claude/skills/ci-step/extract-step.py --step dashboard --section "One window" \
        > /tmp/step.sh

A `--section` is emitted after that step's preamble, because later sections read files
the preamble wrote ($TMP_DIR/check-benchmark.py, the request bodies, and so on).
"""
import argparse
import re
import sys
from pathlib import Path

WORKFLOW = Path(".github/workflows/test-podman-compose.yaml")
STEP_RE = re.compile(r"^      - name: (.+)$")
RUN_RE = re.compile(r"^(\s+)run: \|\s*$")
HEADING_RE = re.compile(r'^\s*echo -e "\\n=+ (.+?) =+"')


def steps(text):
    """Every step's name mapped to the lines of its run block, dedented."""
    lines = text.split("\n")
    found, name, run_indent, body = {}, None, None, []
    for line in lines:
        step = STEP_RE.match(line)
        if step:
            if name and body:
                found[name] = body
            name, run_indent, body = step.group(1), None, []
            continue
        if name is None:
            continue
        if run_indent is None:
            run = RUN_RE.match(line)
            if run:
                run_indent = len(run.group(1)) + 2
            continue
        if line.strip() and not line.startswith(" " * run_indent):
            found[name], name, run_indent, body = body, None, None, []
            continue
        body.append(line[run_indent:])
    if name and body:
        found[name] = body
    return found


def pick(names, wanted):
    """One name, matched case-insensitively on a substring."""
    hits = [n for n in names if wanted.lower() in n.lower()]
    if len(hits) != 1:
        sys.exit(
            f"'{wanted}' matched {len(hits)} of them"
            + (": " + ", ".join(map(repr, hits)) if hits else "")
        )
    return hits[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", help="substring of a step name")
    parser.add_argument("--section", help="substring of a '=== heading ===' inside it")
    parser.add_argument("--list", action="store_true", help="list steps, or a step's sections")
    args = parser.parse_args()

    if not WORKFLOW.exists():
        sys.exit(f"{WORKFLOW} not found; run this from the repository root")
    blocks = steps(WORKFLOW.read_text())

    if args.list and not args.step:
        print("\n".join(blocks))
        return
    if not args.step:
        sys.exit("give --step, or --list to see them")

    body = blocks[pick(blocks, args.step)]
    marks = [(i, m.group(1)) for i, line in enumerate(body) if (m := HEADING_RE.match(line))]

    if args.list:
        print("\n".join(name for _, name in marks) or "(no === headings === in this step)")
        return
    if not args.section:
        print("\n".join(body))
        return

    start = marks[[name for _, name in marks].index(pick([n for _, n in marks], args.section))][0]
    later = [i for i, _ in marks if i > start]
    end = later[0] if later else len(body)
    preamble = body[: marks[0][0]] if marks else []
    print("\n".join(preamble + body[start:end]))


if __name__ == "__main__":
    main()
