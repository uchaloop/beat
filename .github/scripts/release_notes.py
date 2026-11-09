"""Validate a root-module release and extract its exact changelog section."""
import datetime
import os
from pathlib import Path
import re
import sys

NUMBER = r"(?:0|[1-9][0-9]*)"
IDENTIFIER = rf"(?:{NUMBER}|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
TAG = re.compile(rf"v([01])\.{NUMBER}\.{NUMBER}(?:-{IDENTIFIER}(?:\.{IDENTIFIER})*)?")


def extract(tag, module, changelog):
    if not TAG.fullmatch(tag):
        raise ValueError("invalid version tag for a root module without a major suffix")
    if not re.search(r"^module\s+github\.com/uchaloop/beat\s*$", module, re.M):
        raise ValueError("unexpected module path")
    version = tag[1:]
    headers = list(re.finditer(r"^## .*?$", changelog, re.M))
    matches = [i for i, h in enumerate(headers)
               if re.match(r"^## \[" + re.escape(version) + r"\](?:\s|$)", h.group())]
    if len(matches) != 1:
        raise ValueError("expected exactly one changelog section for the tag")
    index = matches[0]
    header = headers[index]
    date = re.fullmatch(r"## \[" + re.escape(version) + r"\] - (\d{4}-\d{2}-\d{2})", header.group())
    if not date:
        raise ValueError("release section must have a date and no Unreleased marker")
    datetime.date.fromisoformat(date[1])
    end = headers[index + 1].start() if index + 1 < len(headers) else len(changelog)
    body = changelog[header.end():end].strip()
    # Reference definitions and headings alone are not release notes.
    content = [line for line in body.splitlines()
               if line.strip() and not line.lstrip().startswith('#')
               and not re.match(r"^\s*\[[^]]+\]:", line)]
    if not content:
        raise ValueError("release section is empty")
    return body + "\n", tag.startswith("v0.") or "-" in tag


def main():
    try:
        notes, is_prerelease = extract(os.environ['GITHUB_REF_NAME'],
                                      Path('go.mod').read_text(),
                                      Path('CHANGELOG.md').read_text())
        Path(os.environ['RUNNER_TEMP'], 'beat-release-notes.md').write_text(notes)
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            output.write(f"is_prerelease={str(is_prerelease).lower()}\n")
    except (ValueError, KeyError, OSError) as error:
        print(f"Release validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
