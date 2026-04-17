#!/usr/bin/env python3
"""
Facebook Video Downloader using yt-dlp
Rotates through no-tunnel → WireGuard configs; no backoff, no cookies.
"""

import os
import csv
import sys
import signal
import logging
import subprocess
import argparse
import tempfile
import shutil
import uuid
from pathlib import Path
from datetime import datetime
from typing import Optional

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("downloader.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
FAILED_CSV  = "failed_downloads.csv"
OUTPUT_DIR  = "downloads"
WG_DIR      = "wg"

_wg_pool: Optional["WireGuardPool"] = None   # set in main() when --wg-dir is given
_shutdown = False                             # set to True on SIGINT to stop after current download


def _handle_sigint(sig, frame) -> None:
    global _shutdown
    if not _shutdown:
        log.warning("Interrupt received — finishing current download then stopping…")
        _shutdown = True


# ══════════════════════════════════════════════════════════════════════════════
# Helper utilities
# ══════════════════════════════════════════════════════════════════════════════

def extract_post_id(url: str) -> Optional[str]:
    import re

    m = re.search(r'/(?:reels?|videos?)/(\d+)', url)
    if m:
        return m.group(1)
    m = re.search(r'[?&]v=(\d+)', url)
    if m:
        return m.group(1)
    m = re.search(r'story_fbid=(\d+)', url)
    if m:
        return m.group(1)
    m = re.search(r'/posts/(\d+)', url)
    if m:
        return m.group(1)
    candidates = re.findall(r'/(\d{8,})', url)
    if candidates:
        return candidates[-1]
    return None


def build_output_path(page_name: str, post_id: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, f"{page_name}_{post_id}.mp4")


def record_failure(page_name: str, url: str, reason: str) -> None:
    file_exists = os.path.isfile(FAILED_CSV)
    with open(FAILED_CSV, "a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if not file_exists:
            writer.writerow(["timestamp", "page_name", "url", "reason"])
        writer.writerow([datetime.now().isoformat(), page_name, url, reason])
    log.warning("Recorded failure → %s", FAILED_CSV)


# ══════════════════════════════════════════════════════════════════════════════
# WireGuard – network-namespace isolation (Linux only)
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_endpoint(endpoint: str) -> str:
    """Resolve the hostname in 'host:port' to an IP using the host's DNS."""
    import socket
    if not endpoint or endpoint.startswith("["):   # already an IPv6 literal
        return endpoint
    host, _, port = endpoint.rpartition(":")
    try:
        ip = socket.gethostbyname(host)
        if ip != host:
            log.info("│    resolved %s → %s", endpoint, f"{ip}:{port}")
        return f"{ip}:{port}"
    except socket.gaierror as exc:
        raise RuntimeError(f"Cannot resolve endpoint hostname '{host}': {exc}") from exc


def _parse_wg_conf(conf_path: Path) -> dict:
    """Parse a wg-quick .conf into {'interface': {...}, 'peers': [{...}]}."""
    result: dict = {"interface": {}, "peers": []}
    section: Optional[str] = None
    peer: dict = {}

    for raw in conf_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line == "[Interface]":
            section = "interface"
        elif line == "[Peer]":
            if peer:
                result["peers"].append(peer)
                peer = {}
            section = "peer"
        elif "=" in line and section:
            k, _, v = line.partition("=")
            k, v = k.strip().lower(), v.strip()
            if section == "interface":
                result["interface"][k] = v
            else:
                peer[k] = v

    if peer:
        result["peers"].append(peer)
    return result


class WireGuardNamespace:
    """
    Brings up a WireGuard tunnel inside a private Linux network namespace so it
    cannot disrupt downloads running on the host network in parallel threads.
    """

    _PEER_KEYS = {
        "publickey":           "PublicKey",
        "presharedkey":        "PresharedKey",
        "endpoint":            "Endpoint",
        "allowedips":          "AllowedIPs",
        "persistentkeepalive": "PersistentKeepalive",
    }

    def __init__(self, conf_path: Path) -> None:
        self.conf_path = conf_path
        uid            = uuid.uuid4().hex[:8]
        self.ns        = f"wgdl_{uid}"
        self.iface     = f"wgdl{uid[:6]}"      # ≤15 chars: Linux iface name limit
        self._parsed   = _parse_wg_conf(conf_path)
        self._dns_dir  = Path(f"/etc/netns/{self.ns}")

    def _run(self, *cmd: str, check: bool = True) -> subprocess.CompletedProcess:
        result = subprocess.run(list(cmd), check=False, capture_output=True, text=True)
        if check and result.returncode != 0:
            log.error("Command failed (exit %d): %s\n  stderr: %s",
                      result.returncode, " ".join(cmd), result.stderr.strip())
            raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
        return result

    def _ns(self, *cmd: str, check: bool = True) -> subprocess.CompletedProcess:
        return self._run("ip", "netns", "exec", self.ns, *cmd, check=check)

    def _addresses(self) -> list[str]:
        return [
            a.strip()
            for a in self._parsed["interface"].get("address", "").split(",")
            if a.strip()
        ]

    def _setconf_text(self) -> str:
        iface = self._parsed["interface"]
        lines = ["[Interface]", f"PrivateKey = {iface['privatekey']}"]
        if "listenport" in iface:
            lines.append(f"ListenPort = {iface['listenport']}")
        for peer in self._parsed["peers"]:
            lines.append("[Peer]")
            for lk, ok in self._PEER_KEYS.items():
                if lk in peer:
                    val = _resolve_endpoint(peer[lk]) if lk == "endpoint" else peer[lk]
                    lines.append(f"{ok} = {val}")
        return "\n".join(lines) + "\n"

    def _setup(self) -> None:
        conf   = self.conf_path.name
        peer_count = len(self._parsed["peers"])
        endpoint   = self._parsed["peers"][0].get("endpoint", "?") if self._parsed["peers"] else "?"
        addrs      = ", ".join(self._addresses()) or "?"

        allowed_ips = self._parsed["peers"][0].get("allowedips", "?") if self._parsed["peers"] else "?"

        log.info("┌─ Tunnel UP   conf=%-20s  ns=%s  iface=%s", conf, self.ns, self.iface)
        log.info("│  endpoint=%-30s  addresses=%s  peers=%d", endpoint, addrs, peer_count)
        log.info("│  allowedips=%s", allowed_ips)

        log.info("│  [1/6] creating network namespace %s", self.ns)
        self._run("ip", "netns", "add", self.ns)

        log.info("│  [2/6] adding WireGuard interface %s", self.iface)
        self._run("ip", "link", "add", self.iface, "type", "wireguard")

        log.info("│  [3/6] moving %s into namespace %s", self.iface, self.ns)
        self._run("ip", "link", "set", self.iface, "netns", self.ns)

        setconf_text = self._setconf_text()
        log.info("│  [4/6] applying wg config via setconf")
        log.debug("│  setconf content:\n%s", setconf_text)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as tmp:
            tmp.write(setconf_text)
            tmp_path = tmp.name
        try:
            self._ns("wg", "setconf", self.iface, tmp_path)
        finally:
            os.unlink(tmp_path)

        log.info("│  [5/6] assigning addresses and bringing interface up")
        for addr in self._addresses():
            log.info("│    ip addr add %s dev %s", addr, self.iface)
            self._ns("ip", "addr", "add", addr, "dev", self.iface)
        self._ns("ip", "link", "set", "lo", "up")
        self._ns("ip", "link", "set", self.iface, "up")

        log.info("│  [6/6] adding default route via %s", self.iface)
        self._ns("ip", "route", "add", "default", "dev", self.iface)

        dns_servers = [
            d.strip()
            for d in self._parsed["interface"].get("dns", "").split(",")
            if d.strip()
        ]
        if not dns_servers:
            # No DNS in config — copy host resolv.conf so the namespace can resolve names
            host_resolv = Path("/etc/resolv.conf")
            if host_resolv.exists():
                dns_servers = [
                    line.split()[1]
                    for line in host_resolv.read_text().splitlines()
                    if line.startswith("nameserver")
                ]
            if not dns_servers:
                dns_servers = ["8.8.8.8", "8.8.4.4"]
            log.info("│  DNS: no DNS in config — falling back to %s", ", ".join(dns_servers))
        else:
            log.info("│  DNS: %s (from config)", ", ".join(dns_servers))
        self._dns_dir.mkdir(parents=True, exist_ok=True)
        (self._dns_dir / "resolv.conf").write_text(
            "\n".join(f"nameserver {d}" for d in dns_servers) + "\n"
        )

        log.info("└─ Tunnel UP   ready  conf=%s", conf)

    def _teardown(self) -> None:
        log.info("┌─ Tunnel DOWN conf=%s  ns=%s", self.conf_path.name, self.ns)
        log.info("│  deleting namespace %s (removes interface %s)", self.ns, self.iface)
        self._run("ip", "netns", "del", self.ns, check=False)
        if self._dns_dir.exists():
            log.info("│  removing %s", self._dns_dir)
            shutil.rmtree(self._dns_dir, ignore_errors=True)
        log.info("└─ Tunnel DOWN done   conf=%s", self.conf_path.name)

    def setup(self) -> None:
        try:
            self._setup()
        except Exception:
            self._teardown()
            raise

    def teardown(self) -> None:
        self._teardown()

    def run_ytdlp(self, url: str, dest_path: str) -> tuple[bool, str]:
        """Run yt-dlp inside this network namespace."""
        cmd = _build_ytdlp_cmd(url, dest_path)
        ns_cmd = ["ip", "netns", "exec", self.ns] + cmd
        log.info("  [tunnel:%s] yt-dlp via namespace %s", self.conf_path.name, self.ns)
        try:
            result = subprocess.run(ns_cmd, capture_output=True, text=True, timeout=300)
            combined = result.stdout + result.stderr
            success = result.returncode == 0
            if not success:
                log.warning("  [tunnel:%s] yt-dlp failed: %s", self.conf_path.name, combined.strip()[:300])
            return success, combined
        except subprocess.TimeoutExpired:
            log.warning("  [tunnel:%s] yt-dlp timed out", self.conf_path.name)
            return False, "TimeoutExpired"


class WireGuardPool:
    """
    Brings up all WireGuard namespaces once at startup and keeps them alive.
    Slot 0 = no tunnel (None), slots 1..N = live WireGuardNamespace instances.
    Each download starts on the next slot; remaining slots are tried as fallback.
    Call teardown() once when done.
    """

    def __init__(self, wg_dir: str) -> None:
        configs = sorted(Path(wg_dir).glob("*.conf"))
        self._namespaces: list[WireGuardNamespace] = []
        self._index = 0

        if not configs:
            log.warning("No .conf files found in '%s'; WireGuard fallback disabled.", wg_dir)
            return

        for conf in configs:
            ns = WireGuardNamespace(conf)
            try:
                ns.setup()
                self._namespaces.append(ns)
            except Exception as exc:
                log.error("Failed to bring up tunnel %s: %s — skipping.", conf.name, exc)

        log.info(
            "WireGuard pool ready: %d/%d tunnel(s) up + no-tunnel slot = %d total",
            len(self._namespaces), len(configs), len(self._namespaces) + 1,
        )

    @property
    def enabled(self) -> bool:
        return bool(self._namespaces)

    def rotation_order(self) -> list[Optional["WireGuardNamespace"]]:
        """Return all slots starting from the current index, then advance the index."""
        options: list[Optional[WireGuardNamespace]] = [None] + self._namespaces
        n = len(options)
        order = [options[(self._index + i) % n] for i in range(n)]
        self._index = (self._index + 1) % n
        return order

    def teardown(self) -> None:
        for ns in self._namespaces:
            try:
                ns.teardown()
            except Exception as exc:
                log.error("Error tearing down tunnel %s: %s", ns.conf_path.name, exc)
        self._namespaces.clear()


# ══════════════════════════════════════════════════════════════════════════════
# yt-dlp bootstrap
# ══════════════════════════════════════════════════════════════════════════════

def ensure_ytdlp() -> None:
    """Install yt-dlp to ~/.local/bin if it is not already on PATH."""
    if shutil.which("yt-dlp"):
        return

    install_dir = os.path.expanduser("~/.local/bin")
    install_path = os.path.join(install_dir, "yt-dlp")
    log.info("yt-dlp not found — installing to %s …", install_path)

    os.makedirs(install_dir, exist_ok=True)

    result = subprocess.run(
        ["curl", "-L", "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp",
         "-o", install_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.critical("Failed to download yt-dlp:\n%s", result.stderr)
        sys.exit(1)

    os.chmod(install_path, 0o755)
    os.environ["PATH"] = install_dir + os.pathsep + os.environ.get("PATH", "")
    log.info("yt-dlp installed successfully.")


# ══════════════════════════════════════════════════════════════════════════════
# Core download logic
# ══════════════════════════════════════════════════════════════════════════════

def _build_ytdlp_cmd(url: str, dest_path: str) -> list[str]:
    return [
        "yt-dlp",
        "-o", dest_path,
        url,
        "--quiet",
        "--no-warnings",
        "--no-playlist",
        "--merge-output-format", "mp4",
    ]


def run_ytdlp(url: str, dest_path: str) -> tuple[bool, str]:
    """Invoke yt-dlp on the host network."""
    cmd = _build_ytdlp_cmd(url, dest_path)
    log.debug("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        combined = result.stdout + result.stderr
        return result.returncode == 0, combined
    except subprocess.TimeoutExpired:
        return False, "TimeoutExpired"
    except FileNotFoundError:
        log.critical("yt-dlp not found. Install it with:  pip install yt-dlp")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# CSV processing
# ══════════════════════════════════════════════════════════════════════════════

def _download_one(idx: int, total: int, page_name: str, url: str) -> bool:
    """Download a single video. Returns True on success (including skip)."""
    if not url:
        log.warning("[%d/%d] Empty URL for page '%s'; skipping.", idx, total, page_name)
        return True

    log.info("─" * 60)
    log.info("[%d/%d] %s  →  %s", idx, total, page_name, url)

    post_id = extract_post_id(url)
    if not post_id:
        log.error("  Could not extract post ID from URL; skipping.")
        record_failure(page_name, url, "Could not extract post ID")
        return False

    dest_path = build_output_path(page_name, post_id)
    log.info("  Output path: %s", dest_path)

    if os.path.isfile(dest_path) and os.path.getsize(dest_path) > 0:
        log.info("  Already exists; skipping.")
        return True

    slots = _wg_pool.rotation_order() if (_wg_pool and _wg_pool.enabled) else [None]

    for wg_ns in slots[:3]:
        if wg_ns is None:
            label = "no-tunnel"
            log.info("  Trying %s", label)
            ok, output = run_ytdlp(url, dest_path)
        else:
            label = f"tunnel:{wg_ns.conf_path.name}"
            log.info("  Trying %s", label)
            ok, output = wg_ns.run_ytdlp(url, dest_path)
        if ok:
            log.info("  ✓ Succeeded via %s", label)
            return True
        log.warning("  %s failed: %s", label, output.strip()[:200])
        if _shutdown or "Interrupted by user" in output:
            log.info("  Interrupted — skipping remaining tunnel attempts.")
            break

    record_failure(page_name, url, "All attempts failed")
    return False


def load_failed_urls() -> set[str]:
    """Return the set of URLs that have previously failed all attempts."""
    if not os.path.isfile(FAILED_CSV):
        return set()
    with open(FAILED_CSV, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return {row["url"].strip() for row in reader if row.get("url")}


def process_csv(csv_path: str, retry_failed: bool = False) -> None:
    """Read the input CSV and download every video entry sequentially."""
    if not os.path.isfile(csv_path):
        log.critical("Input CSV not found: %s", csv_path)
        sys.exit(1)

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)

        required = {"page_name", "url"}
        if not required.issubset(set(reader.fieldnames or [])):
            log.critical(
                "CSV must contain columns: %s  (found: %s)",
                required,
                reader.fieldnames,
            )
            sys.exit(1)

        rows = list(reader)

    failed_urls = set() if retry_failed else load_failed_urls()
    if failed_urls:
        log.info("Skipping %d previously-failed URL(s). Use --retry-failed to retry them.", len(failed_urls))

    rows = [r for r in rows if r.get("url", "").strip() not in failed_urls]
    total = len(rows)
    log.info("Starting download of %d video(s).", total)

    signal.signal(signal.SIGINT, _handle_sigint)

    succeeded = 0
    failed    = 0

    for idx, row in enumerate(rows, start=1):
        if _shutdown:
            log.info("Shutdown requested; stopping after %d/%d.", idx - 1, total)
            break
        if _download_one(idx, total, row["page_name"].strip(), row["url"].strip()):
            succeeded += 1
        else:
            failed += 1

    log.info("═" * 60)
    log.info("Done.  Succeeded: %d  |  Failed: %d  |  Total: %d", succeeded, failed, total)
    if failed:
        log.info("Failed downloads recorded in: %s", FAILED_CSV)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Facebook videos via yt-dlp with optional WireGuard tunnel rotation."
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="videos.csv",
        help="Path to the input CSV file (default: videos.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help=f"Destination folder for downloaded videos (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry URLs that previously failed all attempts (default: skip them)",
    )
    parser.add_argument(
        "--wg-dir",
        default=None,
        metavar="DIR",
        help=(
            "Directory containing WireGuard .conf files. "
            "When set, each failed download is retried through each tunnel in order. "
            "Linux only — requires root and iproute2/wireguard-tools."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    ensure_ytdlp()

    global OUTPUT_DIR, _wg_pool
    OUTPUT_DIR = args.output_dir

    if args.wg_dir:
        _wg_pool = WireGuardPool(args.wg_dir)

    try:
        process_csv(args.csv, retry_failed=args.retry_failed)
    finally:
        if _wg_pool:
            _wg_pool.teardown()


if __name__ == "__main__":
    main()
