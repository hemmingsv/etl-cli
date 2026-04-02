#!/usr/bin/env python3
"""Regenerate README.md from HELP_TEXT in etl.py."""

import sys
from pathlib import Path

import etl

MARKER = "$ etl --help\n"

readme = Path("README.md")
text = readme.read_text()

before, sep, after = text.partition(MARKER)
if not sep:
    print("Could not find help text marker in README.md", file=sys.stderr)
    sys.exit(1)

# Everything after the marker until the closing ``` is the old help text
closing = after.find("```")
if closing == -1:
    print("Could not find closing ``` in README.md", file=sys.stderr)
    sys.exit(1)
tail = after[closing:]

new_text = before + sep + etl.HELP_TEXT + tail

if new_text != text:
    readme.write_text(new_text)
    print("README.md updated", file=sys.stderr)
    sys.exit(1)
