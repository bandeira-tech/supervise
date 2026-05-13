#!/usr/bin/env python3
"""supervise — continuous project status dashboard"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
import select
import termios
import tty
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich.console import Console, Group
from rich.live import Live
from rich.rule import Rule
from rich.style import Style
from rich.table import Table
from rich.text import Text


def run(cmd: list[str], cwd=None, timeout=60) -> tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as e:
        return -1, "", str(e)


def detect_test_cmd(target: Path) -> Optional[list[str]]:
    if (target / "deno.json").exists() or (target / "deno.jsonc").exists():
        fname = "deno.json" if (target / "deno.json").exists() else "deno.jsonc"
        try:
            cfg = json.loads((target / fname).read_text())
            if "test" in cfg.get("tasks", {}):
                return ["deno", "task", "test"]
        except Exception:
            pass
        return ["deno", "test"]
    if (target / "mix.exs").exists():
        return ["mix", "test"]
    if (target / "Cargo.toml").exists():
        return ["cargo", "test"]
    if (target / "go.mod").exists():
        return ["go", "test", "./..."]
    if (target / "package.json").exists():
        try:
            pkg = json.loads((target / "package.json").read_text())
            if "test" in pkg.get("scripts", {}):
                if (target / "pnpm-lock.yaml").exists():
                    return ["pnpm", "test", "--", "--run"]
                elif (target / "yarn.lock").exists():
                    return ["yarn", "test", "--watchAll=false"]
                return ["npm", "test", "--", "--watchAll=false"]
        except Exception:
            pass
    for f in ("pytest.ini", "pyproject.toml", "setup.cfg"):
        if (target / f).exists():
            return ["python", "-m", "pytest", "--tb=short", "-q"]
    if (target / "Makefile").exists():
        rc, _, _ = run(["make", "-n", "test"], cwd=target, timeout=5)
        if rc == 0:
            return ["make", "test"]
    return None


@dataclass
class GitState:
    branch: str = "?"
    dirty: bool = False
    status_output: str = ""
    commit_msg: str = ""
    commit_time: Optional[datetime] = None
    remote_url: Optional[str] = None
    ahead: int = 0
    behind: int = 0
    error: Optional[str] = None


@dataclass
class TestState:
    cmd: Optional[list[str]] = None
    running: bool = False
    run_started: Optional[datetime] = None
    passed: Optional[bool] = None
    output: str = ""
    last_run: Optional[datetime] = None
    duration: float = 0.0
    count: Optional[str] = None  # e.g. "42 passed" or "38 passed, 4 failed"


@dataclass
class PRState:
    prs: list = field(default_factory=list)
    last_fetch: Optional[datetime] = None
    fetching: bool = False
    fetch_started: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class PackageState:
    name: Optional[str] = None
    registry: str = "npm"               # "npm" or "jsr"
    local_version: Optional[str] = None # from config file
    published_version: Optional[str] = None
    fetching: bool = False
    fetch_started: Optional[datetime] = None
    last_fetch: Optional[datetime] = None
    error: Optional[str] = None


_WATCH_SKIP_DIRS = frozenset({
    ".git", "__pycache__", "node_modules", ".venv", "venv",
    "target", "_build", "dist", "build",
    ".mypy_cache", ".ruff_cache", ".pytest_cache",
    ".deno", ".gradle", ".npm", ".cache",
    "coverage", ".nyc_output", ".turbo", ".next", ".nuxt",
})

_WATCH_SKIP_FILES = frozenset({
    "deno.lock", "package-lock.json", "pnpm-lock.yaml",
    "yarn.lock", "Cargo.lock", "mix.lock", "uv.lock",
    "poetry.lock", "Gemfile.lock", "go.sum", "composer.lock",
})

_CODE_EXTS = frozenset({
    ".py", ".pyi", ".ts", ".tsx", ".mts", ".cts",
    ".js", ".jsx", ".mjs", ".cjs",
    ".ex", ".exs", ".go", ".rs", ".java", ".kt", ".scala",
    ".c", ".cpp", ".cc", ".h", ".hpp",
    ".swift", ".rb", ".php", ".cs",
    ".lua", ".zig", ".elm", ".clj", ".cljs",
    ".ml", ".mli", ".hs", ".jl",
    ".sh", ".bash", ".zsh", ".fish",
})


class Supervisor:
    def __init__(self, target: Path, pr_interval: int):
        self.target = target.resolve()
        self.pr_interval = pr_interval
        self._lock = threading.Lock()
        self.git = GitState()
        self.tests = TestState(cmd=detect_test_cmd(target))
        self.prs = PRState()
        self.pkg = PackageState()
        self._stop = threading.Event()
        self._pr_wake = threading.Event()
        self._pkg_wake = threading.Event()
        self._remote_url: Optional[str] = None  # None = not yet fetched, "" = no github remote
        self._pkg_info = _detect_package(target)  # (registry, name, local_version) or None
        if self._pkg_info:
            registry, name, local_version = self._pkg_info
            self.pkg = PackageState(name=name, registry=registry, local_version=local_version)

    def start(self):
        for fn in (self._git_loop, self._test_loop, self._pr_loop, self._pkg_loop):
            t = threading.Thread(target=fn, daemon=True)
            t.start()

    def stop(self):
        self._stop.set()
        self._pr_wake.set()
        self._pkg_wake.set()

    def force_refresh(self):
        self._pr_wake.set()
        self._pkg_wake.set()

    def _git_loop(self):
        while not self._stop.is_set():
            g = GitState()
            rc, out, _ = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=self.target)
            if rc != 0:
                g.error = "not a git repo"
            else:
                g.branch = out.strip()
                _, status, _ = run(["git", "status", "--porcelain"], cwd=self.target)
                g.dirty = bool(status.strip())
                g.status_output = status
                _, log, _ = run(
                    ["git", "log", "-1", "--format=%s\x1f%ct"],
                    cwd=self.target,
                )
                if log.strip():
                    parts = log.strip().split("\x1f")
                    g.commit_msg = parts[0].strip()
                    if len(parts) > 1:
                        try:
                            g.commit_time = datetime.fromtimestamp(int(parts[1]), tz=timezone.utc)
                        except ValueError:
                            pass
                _, ab_out, _ = run(
                    ["git", "rev-list", "--count", "--left-right", "HEAD...@{u}"],
                    cwd=self.target,
                )
                if ab_out.strip():
                    ab_parts = ab_out.strip().split()
                    if len(ab_parts) == 2:
                        try:
                            g.ahead, g.behind = int(ab_parts[0]), int(ab_parts[1])
                        except ValueError:
                            pass
                if self._remote_url is None:
                    _, remote, _ = run(["git", "remote", "get-url", "origin"], cwd=self.target)
                    self._remote_url = _parse_github_url(remote.strip()) or ""
                g.remote_url = self._remote_url or None
            with self._lock:
                self.git = g
            self._stop.wait(2)

    def _scan_mtimes(self) -> dict:
        mtimes: dict = {}
        try:
            for root, dirs, files in os.walk(self.target):
                dirs[:] = [d for d in dirs if d not in _WATCH_SKIP_DIRS]
                for f in files:
                    if f in _WATCH_SKIP_FILES:
                        continue
                    p = Path(root) / f
                    try:
                        mtimes[str(p)] = p.stat().st_mtime
                    except OSError:
                        pass
        except Exception:
            pass
        return mtimes

    def _test_loop(self):
        if not self.tests.cmd:
            return
        prev = self._scan_mtimes()
        self._run_tests()
        while not self._stop.is_set():
            self._stop.wait(2)
            curr = self._scan_mtimes()
            if curr != prev:
                prev = self._scan_mtimes()  # rescan after any settle
                self._run_tests()
                self._stop.wait(10)         # cooldown: ignore churn from the run itself

    def _run_tests(self):
        with self._lock:
            if self.tests.running:
                return
            self.tests.running = True
            self.tests.run_started = datetime.now(tz=timezone.utc)
            cmd = self.tests.cmd

        start = time.monotonic()
        rc, stdout, stderr = run(cmd, cwd=self.target, timeout=300)
        elapsed = time.monotonic() - start

        combined = (stdout + stderr).strip()
        lines = combined.splitlines()
        if len(lines) > 80:
            combined = "\n".join(lines[-80:])

        # parse test count from output (x passed, y failed patterns)
        test_count = _parse_test_counts(combined)

        with self._lock:
            self.tests.running = False
            self.tests.run_started = None
            self.tests.passed = rc == 0
            self.tests.output = combined
            self.tests.last_run = datetime.now(tz=timezone.utc)
            self.tests.duration = elapsed
            self.tests.count = test_count

    def _pr_loop(self):
        while not self._stop.is_set():
            self._fetch_prs()
            self._pr_wake.wait(timeout=self.pr_interval)
            self._pr_wake.clear()

    def _fetch_prs(self):
        with self._lock:
            self.prs.fetching = True
            self.prs.fetch_started = datetime.now(tz=timezone.utc)

        rc, out, err = run(
            ["gh", "pr", "list", "--json", "number,title,author,createdAt", "--limit", "10"],
            cwd=self.target,
            timeout=20,
        )
        p = PRState(last_fetch=datetime.now(tz=timezone.utc))
        if rc == 0:
            try:
                p.prs = json.loads(out)
            except Exception as e:
                p.error = f"parse: {e}"
        else:
            p.error = (err or "gh failed").strip().splitlines()[0]

        with self._lock:
            self.prs = p

    def _pkg_loop(self):
        if not self._pkg_info:
            return
        self._fetch_pkg()
        while not self._stop.is_set():
            self._pkg_wake.wait(timeout=600)
            self._pkg_wake.clear()
            if not self._stop.is_set():
                self._fetch_pkg()

    def _fetch_pkg(self):
        with self._lock:
            if not self.pkg.name:
                return
            self.pkg.fetching = True
            self.pkg.fetch_started = datetime.now(tz=timezone.utc)
            registry = self.pkg.registry
            name = self.pkg.name

        published = _fetch_published_version(registry, name)

        with self._lock:
            self.pkg.fetching = False
            self.pkg.fetch_started = None
            self.pkg.published_version = published
            self.pkg.last_fetch = datetime.now(tz=timezone.utc)

    def snapshot(self):
        with self._lock:
            return self.git, self.tests, self.prs, self.pkg


def _parse_github_url(remote: str) -> Optional[str]:
    m = re.match(r"git@github\.com:(.+?)(?:\.git)?$", remote)
    if m:
        return f"https://github.com/{m.group(1)}"
    m = re.match(r"https?://github\.com/(.+?)(?:\.git)?$", remote)
    if m:
        return f"https://github.com/{m.group(1)}"
    return None


def _detect_package(target: Path) -> Optional[tuple[str, str, Optional[str]]]:
    """Return (registry, name, local_version) from project config, or None."""
    for fname in ("deno.json", "deno.jsonc"):
        fp = target / fname
        if fp.exists():
            try:
                cfg = json.loads(fp.read_text())
                name = cfg.get("name", "")
                version = cfg.get("version") or None
                if name.startswith("@"):
                    return ("jsr", name, version)
            except Exception:
                pass
    fp = target / "package.json"
    if fp.exists():
        try:
            pkg = json.loads(fp.read_text())
            name = pkg.get("name", "")
            version = pkg.get("version") or None
            if name:
                return ("npm", name, version)
        except Exception:
            pass
    return None


def _fetch_published_version(registry: str, name: str) -> Optional[str]:
    try:
        if registry == "npm":
            url = f"https://registry.npmjs.org/{name}/latest"
        else:
            url = f"https://jsr.io/{name}/meta.json"
        req = urllib.request.Request(url, headers={"User-Agent": "supervise/0.1"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("version") if registry == "npm" else data.get("latest")
    except Exception:
        return None


def _version_gt(a: str, b: str) -> bool:
    def parts(v: str) -> list[int]:
        v = re.sub(r"^v", "", v)
        v = re.split(r"[-+]", v)[0]
        result = []
        for p in v.split("."):
            try:
                result.append(int(p))
            except ValueError:
                result.append(0)
        return result
    return parts(a) > parts(b)


def _parse_test_counts(output: str) -> Optional[str]:
    # Deno: "ok | 12 passed | 0 failed (123ms)"
    m = re.search(r"(\d+) passed.*?(\d+) failed", output)
    if m:
        p, f = int(m.group(1)), int(m.group(2))
        return f"{p}/{p+f}" if f == 0 else f"{p}/{p+f} ({f} failed)"
    m = re.search(r"(\d+) passed", output)
    if m:
        return f"{m.group(1)} passed"
    # pytest: "5 passed", "3 failed"
    m = re.search(r"(\d+) failed", output)
    if m:
        p_m = re.search(r"(\d+) passed", output)
        p = int(p_m.group(1)) if p_m else 0
        f = int(m.group(1))
        return f"{p}/{p+f} ({f} failed)"
    # mix test: "1 test, 0 failures" or "5 tests, 2 failures"
    m = re.search(r"(\d+) tests?, (\d+) failures?", output)
    if m:
        total, failures = int(m.group(1)), int(m.group(2))
        passed = total - failures
        return f"{passed}/{total}" if failures == 0 else f"{passed}/{total} ({failures} failed)"
    return None


def _clip_output(output: str, console: Optional["Console"] = None) -> str:
    try:
        height = console.size.height if console else os.get_terminal_size().lines
    except Exception:
        height = 40
    max_lines = max(5, height - 14)
    lines = output.splitlines()
    if len(lines) <= max_lines:
        return output
    clipped = len(lines) - max_lines
    return f"[↑ {clipped} more lines]\n" + "\n".join(lines[-max_lines:])


def _elapsed(since: Optional[datetime]) -> str:
    if since is None:
        return ""
    secs = max(0, int((datetime.now(tz=timezone.utc) - since).total_seconds()))
    return f"{secs}s" if secs < 60 else f"{secs // 60}m{secs % 60:02d}s"


def time_ago(dt: Optional[datetime]) -> str:
    if dt is None:
        return "never"
    now = datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    secs = max(0, int((now - dt).total_seconds()))
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def _sym(state: str) -> Text:
    return {
        "good": Text("✓", style="bold green"),
        "bad":  Text("✗", style="bold red"),
        "run":  Text("~", style="yellow"),
    }.get(state, Text(" "))


def _script_version(script: Path) -> str:
    try:
        h = hashlib.sha1(script.read_bytes()).hexdigest()[:7]
        ts = datetime.fromtimestamp(script.stat().st_mtime).strftime("%Y%m%dT%H%M")
        return f">>@v0.1.{h}-{ts}"
    except Exception:
        return ""


def _ops_body(
    git: GitState, tests: TestState, prs: PRState, console: Optional[Console]
) -> tuple[list, str]:
    tbl = Table(
        show_header=False, box=None,
        pad_edge=False, show_edge=False,
        padding=(0, 2, 0, 0),
        expand=True,
    )
    tbl.add_column("sym",   width=1,  no_wrap=True, min_width=1, max_width=1)
    tbl.add_column("value", ratio=1,  no_wrap=True, overflow="ellipsis")

    # ─ git ───────────────────────────────────────────────────────────────────
    if git.error:
        tbl.add_row(_sym("bad"), Text(git.error, style="red"))
    else:
        v = Text(git.branch, style="yellow")
        if git.ahead:
            v.append(f" ↑{git.ahead}", style="green")
        if git.behind:
            v.append(f" ↓{git.behind}", style="red")
        if git.commit_time:
            v.append("  ", style="")
            v.append(time_ago(git.commit_time), style="dim")
        if git.commit_msg:
            v.append("  ", style="")
            v.append(git.commit_msg, style="dim italic")
        tbl.add_row(_sym("bad" if git.dirty else "good"), v)

    # ─ tests ─────────────────────────────────────────────────────────────────
    if tests.cmd is not None:
        v = Text()
        if tests.running:
            v.append(_elapsed(tests.run_started), style="dim")
            tbl.add_row(_sym("run"), v)
        elif tests.passed is None:
            v.append("—", style="dim")
            tbl.add_row(_sym(""), v)
        elif tests.passed:
            if tests.count:
                v.append(tests.count, style="")
                v.append("  ", style="")
            if tests.last_run:
                v.append(time_ago(tests.last_run), style="dim")
                v.append(f"  {tests.duration:.1f}s", style="dim")
            tbl.add_row(_sym("good"), v)
        else:
            if tests.count:
                v.append(tests.count, style="red")
                v.append("  ", style="")
            if tests.last_run:
                v.append(time_ago(tests.last_run), style="dim")
            tbl.add_row(_sym("bad"), v)

    # ─ gh/pr (own table so test output can be inserted above it) ─────────────
    pr_tbl = Table(
        show_header=False, box=None,
        pad_edge=False, show_edge=False,
        padding=(0, 2, 0, 0),
        expand=True,
    )
    pr_tbl.add_column("sym",   width=1,  no_wrap=True, min_width=1, max_width=1)
    pr_tbl.add_column("value", ratio=1,  no_wrap=True, overflow="ellipsis")
    v = Text()
    if prs.fetching and prs.last_fetch is None:
        v.append(_elapsed(prs.fetch_started), style="dim")
        pr_sym = _sym("run")
    elif not prs.prs:
        v.append("none", style="dim")
        if prs.last_fetch:
            v.append(f"  {time_ago(prs.last_fetch)}", style="dim")
        pr_sym = _sym("good")
    else:
        n = len(prs.prs)
        v.append(f"{n} open", style="magenta")
        if prs.last_fetch:
            v.append(f"  {time_ago(prs.last_fetch)}", style="dim")
        for pr in prs.prs[:5]:
            author = (pr.get("author") or {}).get("login", "")
            pr_url = f"{git.remote_url}/pull/{pr['number']}" if git.remote_url else None
            v.append("\n   ")
            v.append(f"#{pr['number']} {pr['title'][:60]}", style=Style(link=pr_url) if pr_url else Style())
            if author:
                v.append(f"  {author}", style="dim")
        pr_sym = _sym("")
    pr_tbl.add_row(pr_sym, v)

    test_failed = tests.cmd and not tests.running and tests.passed is False
    parts: list = [tbl]
    if test_failed and tests.output:
        parts.append(Rule(style="dim red"))
        parts.append(Text(_clip_output(tests.output, console), style="dim"))
        parts.append(Rule(style="dim red"))
    parts.append(pr_tbl)
    if not test_failed and git.dirty and git.status_output:
        parts.append(Rule(style="dim yellow"))
        parts.append(Text(git.status_output.rstrip(), style="dim"))

    color = "red" if tests.passed is False else ("yellow" if git.dirty else "green")
    return parts, color


def _scan_dir(path: Path) -> tuple[list[str], list[str], list[str]]:
    """Return (code_dirs, code_files, rest_files) for one directory level."""
    try:
        entries = sorted(os.listdir(path))
    except Exception:
        return [], [], []
    dirs, code_files, rest_files = [], [], []
    for name in entries:
        p = path / name
        if p.is_dir():
            if not name.startswith(".") and name not in _WATCH_SKIP_DIRS:
                dirs.append(name)
        elif p.is_file():
            if Path(name).suffix.lower() in _CODE_EXTS:
                code_files.append(name)
            else:
                rest_files.append(name)
    return dirs, code_files, rest_files


def _content_body(target: Path) -> tuple[list, str]:
    src = target / "src"

    if src.is_dir():
        # top: contents of src/
        code_dirs, code_files, _ = _scan_dir(src)
        # bottom: full root listing, src included
        root_dirs, root_code, root_rest = _scan_dir(target)
        bottom_dirs  = root_dirs   # src stays in here
        bottom_files = root_code + root_rest
        open_dir     = "src"
    else:
        code_dirs, code_files, rest = _scan_dir(target)
        bottom_dirs  = []
        bottom_files = rest
        open_dir     = None

    parts: list = []

    if code_dirs or code_files:
        code_tbl = Table(
            show_header=False, box=None,
            pad_edge=False, show_edge=False,
            padding=(0, 0, 0, 1),
        )
        code_tbl.add_column("name", no_wrap=True, overflow="ellipsis")
        for d in code_dirs:
            code_tbl.add_row(Text(f"{d}/", style="cyan"))
        for f in code_files:
            code_tbl.add_row(Text(f))
        parts.append(code_tbl)

    if bottom_dirs or bottom_files:
        if parts:
            parts.append(Rule(style="dim"))
        rest_tbl = Table(
            show_header=False, box=None,
            pad_edge=False, show_edge=False,
            padding=(0, 0, 0, 1),
        )
        rest_tbl.add_column("name", no_wrap=True, overflow="ellipsis")
        for d in bottom_dirs:
            if d == open_dir:
                rest_tbl.add_row(Text(f"▸ {d}/", style="cyan"))
            else:
                rest_tbl.add_row(Text(f"  {d}/", style="dim cyan"))
        for f in bottom_files:
            rest_tbl.add_row(Text(f, style="dim"))
        parts.append(rest_tbl)

    return parts, "blue"


def _pkg_line(pkg: PackageState) -> Text:
    if not pkg.name:
        return Text("")
    label = f"{pkg.registry}:{pkg.name}"
    t = Text()
    if pkg.fetching and pkg.last_fetch is None:
        t.append("~", style="yellow")
        t.append(f"  {label}")
        t.append(f"  {_elapsed(pkg.fetch_started)}", style="dim")
    elif pkg.published_version is None:
        t.append(" ")
        t.append(f"  {label}", style="dim")
    else:
        local = pkg.local_version
        published = pkg.published_version
        match = (local == published) if local else True
        t.append("✓" if match else "✗", style="bold green" if match else "bold red")
        t.append(f"  {label}")
        if local and local != published and _version_gt(local, published):
            t.append(f"  ↑{local}", style="green")
        t.append(f"  {published}", style="dim")
        if pkg.last_fetch:
            t.append(f"  {time_ago(pkg.last_fetch)}", style="dim")
    return t


def render(sup: Supervisor, console: Optional[Console] = None, version: str = "", view: str = "ops", show_help: bool = False) -> Group:
    git, tests, prs, pkg = sup.snapshot()
    target = sup.target

    home = Path.home()
    try:
        display = "~/" + str(target.relative_to(home))
    except ValueError:
        display = str(target)

    header = Text(display, style="cyan")
    if git.remote_url:
        repo_label = git.remote_url.removeprefix("https://")
        header.append("  ")
        header.append(repo_label, style=Style(color="bright_black", link=git.remote_url))

    spacer = _pkg_line(pkg)

    if view == "content":
        body_parts, color = _content_body(target)
        top_parts: list = []
    else:
        body_parts, color = _ops_body(git, tests, prs, console)
        top_parts = [header, spacer]

    if show_help:
        body_parts.append(Rule(style="dim"))
        h = Text()
        h.append("1", style="bold")
        h.append("  ops    ", style="dim")
        h.append("2", style="bold")
        h.append("  content    ", style="dim")
        h.append("↵", style="bold")
        h.append("  refresh remote", style="dim")
        body_parts.append(h)

    bar = Table(
        show_header=False, box=None,
        expand=True, padding=(0, 1, 0, 1),
        show_edge=False, pad_edge=False,
    )
    bar.add_column("name", ratio=1, style=f"bold on {color}", no_wrap=True, overflow="ellipsis")
    bar.add_column("ver",           style=f"dim on {color}",  no_wrap=True)
    bar.add_row(target.name, version or "")

    return Group(bar, *top_parts, *body_parts)


def main():
    ap = argparse.ArgumentParser(
        prog="supervise",
        description="Continuous project status dashboard",
    )
    ap.add_argument("target", nargs="?", default=".", help="project directory (default: cwd)")
    ap.add_argument("-v", "--view", choices=["ops", "content"], default="ops", metavar="VIEW", help="view mode: ops (default) or content")
    ap.add_argument("--pr-interval", type=int, default=300, metavar="N", help="seconds between PR fetches (default: 300)")
    ap.add_argument("--refresh", type=float, default=1.0, metavar="S", help="display refresh rate in seconds (default: 1)")
    args = ap.parse_args()

    target = Path(args.target).expanduser().resolve()
    if not target.is_dir():
        print(f"error: {target} is not a directory", file=sys.stderr)
        sys.exit(1)

    script = Path(__file__).with_suffix(".py").resolve()
    try:
        script_mtime = script.stat().st_mtime
    except OSError:
        script_mtime = None
    version = _script_version(script)

    sup = Supervisor(target, pr_interval=args.pr_interval)
    sup.start()

    console = Console()
    view = args.view
    show_help = False
    reload_needed = False

    raw_mode = sys.stdin.isatty()
    fd = sys.stdin.fileno() if raw_mode else -1
    old_term = termios.tcgetattr(fd) if raw_mode else None
    if raw_mode:
        tty.setcbreak(fd)

    def _read_key() -> Optional[str]:
        if raw_mode and select.select([sys.stdin], [], [], 0)[0]:
            return sys.stdin.read(1)
        return None

    try:
        with Live(render(sup, console, version, view=view, show_help=show_help), console=console, refresh_per_second=1) as live:
            while True:
                if script_mtime is not None:
                    try:
                        if script.stat().st_mtime != script_mtime:
                            reload_needed = True
                            break
                    except OSError:
                        pass
                ch = _read_key()
                if ch in ("\x03", "\x04"):   # Ctrl-C / Ctrl-D
                    break
                elif ch == "1":
                    view, show_help = "ops", False
                elif ch == "2":
                    view, show_help = "content", False
                elif ch == "?":
                    show_help = not show_help
                elif ch == "\r":
                    sup.force_refresh()
                live.update(render(sup, console, version, view=view, show_help=show_help))
                if ch is None:
                    time.sleep(args.refresh)
    except KeyboardInterrupt:
        pass
    finally:
        if raw_mode and old_term is not None:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_term)
        sup.stop()

    if reload_needed:
        console.clear()
        os.execv(sys.executable, [sys.executable] + sys.argv)


if __name__ == "__main__":
    main()
