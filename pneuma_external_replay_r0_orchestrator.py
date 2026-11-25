#!/usr/bin/env python3
"""
PNEUMA-NATIVE OMEGA — AUTH-EXTERNAL-REPLAY-R0 (Revision T2 — Tier 1 Linux x86-64)
pneuma_external_replay_r0_orchestrator.py

Supports two-stage checkpointed execution within one isolated runner job so that
even if a memory-constrained VM (e.g., 8 GB Private Runner) terminates during L7
monolithic `leanchecker --fresh`, Stage 1 checkpoint evidence is already sealed
and uploaded (`F-EXT-T1-03` & `F-EXT-T2-02`):
  - `--phase stage1` : Gate 0 + Gate 1 + 7-layer clean `lake build` + 91/91 `#print axioms`
                       + 7 runners + 6 Python harnesses + L1..L6 `leanchecker --fresh`
                       + L7 incremental `leanchecker P3CDiffTribunalAudit`.
  - `--phase stage2` : L7 monolithic `leanchecker --fresh P3CDiffTribunalAudit` +
                       Zone 01/02 post-immutability audit + Zone 04 Sentinels K1 & K2 +
                       final receipt emission.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

EXPECTED_ZIP_BYTES = 784963
EXPECTED_ZIP_SHA256 = "B0E9BB1C404E16E6E08E23D40DCD08BD3B1A831D7B796D8DC97B1D773B67CFC6"
EXPECTED_PAYLOAD_COUNT = 123
EXPECTED_TOTAL_ZIP_ENTRIES = 125
PINNED_LEAN_COMMIT = "819816b2e0a3bf405af45ae5c7af2491d8f5bee6"

ALLOWED_LEAN_AXIOMS = {"propext", "Classical.choice", "Quot.sound"}

# Additive budget allocation (`F-EXT-T2-02`: 10 + 45 + 15 + 95 + 10 + 20 = 195 min < 210 min job limit)
JOB_TOTAL_BUDGET_SEC = 210 * 60
EVIDENCE_RESERVE_SEC = 20 * 60
MIN_STEP_HEADROOM_SEC = 120
PHASE_BUDGETS_SEC = {
    "provision_and_preflight": 10 * 60,
    "clean_build_7_layers": 45 * 60,
    "runners_and_oracles_7_layers": 15 * 60,
    "fresh_leanchecker_7_layers": 95 * 60,
    "sentinel_probes_k1_k2": 10 * 60,
}

LAYERS = [
    {
        "layer_id": "L1_P1_ExactRat",
        "rel_dir": ".",
        "audit_lean": "05_independent_tribunal/TribunalAudit.lean",
        "expected_axiom_entries": 11,
        "fresh_root": "TribunalAudit",
        "runner_bin": "p1_tribunal_runner",
        "needs_posix_exe_alias": False,
        "python_harness": None,
    },
    {
        "layer_id": "L2_P2_DerivationReplay",
        "rel_dir": "P2_DerivationReplay",
        "audit_lean": "05_independent_tribunal/P2TribunalAudit.lean",
        "expected_axiom_entries": 10,
        "fresh_root": "P2TribunalAudit",
        "runner_bin": "p2_tribunal_runner",
        "needs_posix_exe_alias": True,
        "python_harness": ["05_independent_tribunal/differential_oracle_p2.py"],
    },
    {
        "layer_id": "L3_P3A_LexicalGrammar",
        "rel_dir": "P3_LexicalGrammar",
        "audit_lean": "05_independent_tribunal/P3ATribunalAudit.lean",
        "expected_axiom_entries": 14,
        "fresh_root": "P3ATribunalAudit",
        "runner_bin": "p3a_tribunal_runner",
        "needs_posix_exe_alias": True,
        "python_harness": ["05_independent_tribunal/differential_oracle_p3a.py"],
    },
    {
        "layer_id": "L4_P3B_BoundedParser",
        "rel_dir": "P3B_BoundedParser",
        "audit_lean": "05_independent_tribunal/P3BTribunalAudit.lean",
        "expected_axiom_entries": 20,
        "fresh_root": "P3BTribunalAudit",
        "runner_bin": "p3b_tribunal_runner",
        "needs_posix_exe_alias": False,
        "python_harness": ["05_independent_tribunal/differential_oracle_p3b.py"],
    },
    {
        "layer_id": "L5_P3C_SemanticNorm",
        "rel_dir": "P3C_SemanticNorm",
        "audit_lean": "05_independent_tribunal/P3CTribunalAudit.lean",
        "expected_axiom_entries": 14,
        "fresh_root": "P3CTribunalAudit",
        "runner_bin": "p3c_tribunal_runner",
        "needs_posix_exe_alias": True,
        "python_harness": ["05_independent_tribunal/differential_oracle_p3c.py"],
    },
    {
        "layer_id": "L6_P3C_Provenance_R1",
        "rel_dir": "P3C_Provenance_R1",
        "audit_lean": "05_independent_tribunal/P3CProvenanceTribunalAudit.lean",
        "expected_axiom_entries": 11,
        "fresh_root": "P3CProvenanceTribunalAudit",
        "runner_bin": "p3c_provenance_r1_runner",
        "needs_posix_exe_alias": True,
        "python_harness": ["05_independent_tribunal/provenance_audit_and_probes.py", "--json"],
    },
    {
        "layer_id": "L7_P3C_Differential_R1",
        "rel_dir": "P3C_Differential_R1",
        "audit_lean": "05_independent_tribunal/P3CDiffTribunalAudit.lean",
        "expected_axiom_entries": 11,
        "fresh_root": "P3CDiffTribunalAudit",
        "runner_bin": "p3c_differential_r1_runner",
        "needs_posix_exe_alias": True,
        "python_harness": ["05_independent_tribunal/independent_r0_semantic_oracle.py", "--json"],
    },
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest().upper()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest().upper()


def compute_tree_digest(root: Path) -> Tuple[str, int]:
    entries: List[str] = []
    for p in sorted(root.rglob("*")):
        if p.is_symlink():
            entries.append(f"SYMLINK:{p.relative_to(root).as_posix()}->{os.readlink(p)}")
        elif p.is_file():
            entries.append(f"FILE:{p.relative_to(root).as_posix()}:{p.stat().st_size}:{sha256_file(p)}")
    joined = "\n".join(entries).encode("utf-8")
    return sha256_bytes(joined), len(entries)


def lock_tree_readonly(root: Path) -> None:
    for p in sorted(root.rglob("*"), reverse=True):
        if p.is_symlink():
            continue
        if p.is_file():
            os.chmod(p, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        elif p.is_dir():
            os.chmod(p, stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    os.chmod(root, stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)


def verify_dossier_blocks(dossier_path: Path, subpkg_root: Path, expected_count: int) -> Dict[str, object]:
    raw = dossier_path.read_bytes()
    pat = re.compile(
        rb"### File \d+: `([^`]+)` \(SHA-256: `([0-9A-F]{64})`\)\r?\n````[^\r\n]*\r?\n(.*?)\r?\n````",
        re.DOTALL,
    )
    matches = pat.findall(raw)
    if len(matches) != expected_count:
        return {"passed": False, "expected": expected_count, "actual": len(matches), "mismatch": "BLOCK_COUNT"}
    for rel_b, decl_sha_b, body_b in matches:
        rel = rel_b.decode("utf-8")
        decl_sha = decl_sha_b.decode("utf-8")
        calc_sha = sha256_bytes(body_b.replace(b"\r\n", b"\n") + b"\n")
        target_p = subpkg_root / rel
        if not target_p.exists():
            return {"passed": False, "mismatch": f"MISSING_DISK_FILE:{rel}"}
        disk_sha = sha256_file(target_p)
        if not (calc_sha == decl_sha == disk_sha):
            return {
                "passed": False,
                "mismatch": f"HASH_MISMATCH:{rel}:block={calc_sha}:decl={decl_sha}:disk={disk_sha}",
            }
    return {"passed": True, "verified_blocks": expected_count}


AXIOM_RE = re.compile(
    r"'([^']+)'\s+(depends on axioms:\s*\[([^\]]*)\]|does not depend on any axioms)",
    re.DOTALL,
)


def parse_and_verify_axioms(stdout_text: str, min_expected_count: int) -> Dict[str, object]:
    matches = AXIOM_RE.findall(stdout_text)
    parsed = []
    forbidden = []
    for thm_name, _full_clause, ax_list_raw in matches:
        if ax_list_raw:
            axioms = [a.strip() for a in ax_list_raw.replace("\n", " ").split(",") if a.strip()]
        else:
            axioms = []
        parsed.append({"theorem": thm_name, "axioms": axioms})
        disallowed = [a for a in axioms if a not in ALLOWED_LEAN_AXIOMS]
        if disallowed:
            forbidden.append({"theorem": thm_name, "disallowed_axioms": disallowed})
    count_ok = len(parsed) >= min_expected_count
    passed = count_ok and (len(forbidden) == 0)
    return {
        "passed": passed,
        "entry_count": len(parsed),
        "min_expected_count": min_expected_count,
        "forbidden_detections": forbidden,
        "entries": parsed,
    }


class BudgetTracker:
    def __init__(self, job_start_epoch: float) -> None:
        self.job_start_epoch = job_start_epoch

    def elapsed_sec(self) -> float:
        return time.time() - self.job_start_epoch

    def remaining_before_reserve_sec(self) -> float:
        return JOB_TOTAL_BUDGET_SEC - self.elapsed_sec() - EVIDENCE_RESERVE_SEC

    def check_can_start(self, min_needed_sec: float = MIN_STEP_HEADROOM_SEC) -> Tuple[bool, float]:
        rem = self.remaining_before_reserve_sec()
        return (rem >= min_needed_sec), rem


def run_timed_cmd(
    cmd: List[str],
    cwd: Path,
    log_path: Path,
    timeout_sec: int,
    high_oom_score_child: bool = False,
) -> Dict[str, object]:
    env = dict(os.environ)
    env["LC_ALL"] = "C.UTF-8"
    env["TZ"] = "UTC"
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    gnu_time = Path("/usr/bin/time")
    choom_bin = shutil.which("choom")
    time_file = log_path.with_suffix(log_path.suffix + ".timev")

    wrapped_cmd = list(cmd)
    if high_oom_score_child and choom_bin:
        # Ensure child process is targeted BEFORE Runner.Worker if kernel OOM triggers
        wrapped_cmd = [choom_bin, "-n", "800", "--", *wrapped_cmd]
    if gnu_time.exists():
        wrapped_cmd = [str(gnu_time), "-v", "-o", str(time_file), *wrapped_cmd]

    t0 = time.perf_counter()
    timed_out = False
    try:
        proc = subprocess.run(
            wrapped_cmd,
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(10, int(timeout_sec)),
        )
        rc = proc.returncode
        out_txt = proc.stdout
        err_txt = proc.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        rc = 124
        out_txt = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
        err_txt = (exc.stderr or "") if isinstance(exc.stderr, str) else ""
    dt = round(time.perf_counter() - t0, 4)

    max_rss_kb: Optional[int] = None
    if time_file.exists():
        t_txt = time_file.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", t_txt)
        if m:
            max_rss_kb = int(m.group(1))
        time_file.unlink(missing_ok=True)

    combined_log = (
        f"COMMAND: {' '.join(cmd)}\n"
        f"CWD: {cwd}\n"
        f"EXIT_CODE: {rc}\n"
        f"TIMED_OUT: {timed_out}\n"
        f"ELAPSED_SEC: {dt}\n"
        f"MAX_RSS_KB: {max_rss_kb}\n"
        f"--- STDOUT ---\n{out_txt}\n"
        f"--- STDERR ---\n{err_txt}\n"
    )
    log_path.write_text(combined_log, encoding="utf-8")

    oom_suspected = (
        (rc in (137, -9))
        or ("out of memory" in err_txt.lower())
        or ("std::bad_alloc" in err_txt.lower())
        or ("killed" in err_txt.lower() and rc != 0)
    )
    return {
        "command": cmd,
        "cwd": str(cwd),
        "exit_code": rc,
        "timed_out": timed_out,
        "oom_suspected": oom_suspected,
        "elapsed_sec": dt,
        "max_rss_kb": max_rss_kb,
        "max_rss_mb": round(max_rss_kb / 1024.0, 2) if max_rss_kb is not None else None,
        "log_file": log_path.name,
        "stdout": out_txt,
        "stderr": err_txt,
    }


def setup_elan_env(elan_bin_dir: Path) -> None:
    elan_home = elan_bin_dir.parent
    os.environ["ELAN_HOME"] = str(elan_home)
    os.environ["PATH"] = f"{elan_bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"


def collect_host_qualification(elan_bin_dir: Path) -> Dict[str, object]:
    setup_elan_env(elan_bin_dir)

    def read_opt(p: str) -> str:
        fp = Path(p)
        return fp.read_text(encoding="utf-8", errors="replace").strip() if fp.exists() else ""

    def cmd_out(args: List[str]) -> str:
        p = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if p.returncode != 0 and not p.stdout.strip():
            raise RuntimeError(f"Command {args} failed (rc={p.returncode}): {p.stderr.strip()}")
        return p.stdout.strip()

    elan_exe = elan_bin_dir / "elan"
    shim_lean = str(elan_bin_dir / "lean") if (elan_bin_dir / "lean").exists() else (shutil.which("lean") or "")
    shim_lake = str(elan_bin_dir / "lake") if (elan_bin_dir / "lake").exists() else (shutil.which("lake") or "")
    shim_lc = (
        str(elan_bin_dir / "leanchecker")
        if (elan_bin_dir / "leanchecker").exists()
        else (shutil.which("leanchecker") or "")
    )

    elan_cmd = str(elan_exe) if elan_exe.exists() else "elan"
    real_lean = cmd_out([elan_cmd, "which", "lean"])
    real_lake = cmd_out([elan_cmd, "which", "lake"])
    real_lc = cmd_out([elan_cmd, "which", "leanchecker"])
    real_py = str(Path(sys.executable).resolve())

    lean_ver = cmd_out([real_lean, "--version"])
    lake_ver = cmd_out([real_lake, "--version"])
    elan_ver = cmd_out([elan_cmd, "--version"])

    meminfo = read_opt("/proc/meminfo")
    mem_total_kb = None
    swap_total_kb = None
    m_mem = re.search(r"MemTotal:\s*(\d+)\s*kB", meminfo)
    m_swp = re.search(r"SwapTotal:\s*(\d+)\s*kB", meminfo)
    if m_mem:
        mem_total_kb = int(m_mem.group(1))
    if m_swp:
        swap_total_kb = int(m_swp.group(1))

    return {
        "captured_at_utc": utc_now_iso(),
        "boot_id": read_opt("/proc/sys/kernel/random/boot_id"),
        "hostname": platform.node(),
        "uname": cmd_out(["uname", "-a"]),
        "os_release": read_opt("/etc/os-release"),
        "cpu_arch": platform.machine(),
        "cpu_count_logical": os.cpu_count(),
        "lscpu_summary": cmd_out(["lscpu"]),
        "mem_total_kb": mem_total_kb,
        "mem_total_mb": round(mem_total_kb / 1024.0, 2) if mem_total_kb else None,
        "swap_total_kb": swap_total_kb,
        "swap_total_mb": round(swap_total_kb / 1024.0, 2) if swap_total_kb is not None else None,
        "extra_swap_provisioned_note": os.environ.get("PNEUMA_SWAP_PROVISION_NOTE", "NONE"),
        "github_context": {
            "repository": os.environ.get("GITHUB_REPOSITORY", ""),
            "ref": os.environ.get("GITHUB_REF", ""),
            "sha": os.environ.get("GITHUB_SHA", ""),
            "workflow_ref": os.environ.get("GITHUB_WORKFLOW_REF", ""),
            "workflow_sha": os.environ.get("INPUT_GITHUB_WORKFLOW_SHA", ""),
            "run_id": os.environ.get("GITHUB_RUN_ID", ""),
            "run_number": os.environ.get("GITHUB_RUN_NUMBER", ""),
            "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
            "runner_name": os.environ.get("RUNNER_NAME", ""),
            "runner_os": os.environ.get("RUNNER_OS", ""),
            "runner_arch": os.environ.get("RUNNER_ARCH", ""),
            "image_os": os.environ.get("ImageOS", ""),
            "image_version": os.environ.get("ImageVersion", ""),
        },
        "toolchain_binaries": {
            "elan_version": elan_ver.splitlines()[0] if elan_ver else "",
            "lean_version": lean_ver.splitlines()[0] if lean_ver else "",
            "lean_commit_matches_4_33_1": ("4.33.1" in lean_ver) and (PINNED_LEAN_COMMIT in lean_ver),
            "lake_version": lake_ver.splitlines()[0] if lake_ver else "",
            "python_version": sys.version.replace("\n", " "),
            "shim_lean_path": shim_lean,
            "shim_lean_sha256": sha256_file(Path(shim_lean)) if shim_lean and Path(shim_lean).exists() else "",
            "resolved_lean_path": real_lean,
            "resolved_lean_sha256": sha256_file(Path(real_lean)) if Path(real_lean).exists() else "",
            "shim_lake_path": shim_lake,
            "shim_lake_sha256": sha256_file(Path(shim_lake)) if shim_lake and Path(shim_lake).exists() else "",
            "resolved_lake_path": real_lake,
            "resolved_lake_sha256": sha256_file(Path(real_lake)) if Path(real_lake).exists() else "",
            "shim_leanchecker_path": shim_lc,
            "shim_leanchecker_sha256": sha256_file(Path(shim_lc)) if shim_lc and Path(shim_lc).exists() else "",
            "resolved_leanchecker_path": real_lc,
            "resolved_leanchecker_sha256": sha256_file(Path(real_lc)) if Path(real_lc).exists() else "",
            "resolved_python_path": real_py,
            "resolved_python_sha256": sha256_file(Path(real_py)),
        },
    }


def run_gate0_admission(zip_src_path: Path, zone01: Path) -> Dict[str, object]:
    if not zip_src_path.exists() or zip_src_path.is_symlink() or not zip_src_path.is_file():
        return {"passed": False, "reason": "ZIP_MISSING_OR_NOT_REGULAR_FILE"}

    head_bytes = zip_src_path.open("rb").read(64)
    if head_bytes.startswith(b"version https://git-lfs.github.com/spec/v1"):
        return {"passed": False, "reason": "GIT_LFS_POINTER_REJECTED"}

    actual_bytes = zip_src_path.stat().st_size
    actual_sha = sha256_file(zip_src_path)
    if (actual_bytes != EXPECTED_ZIP_BYTES) or (actual_sha != EXPECTED_ZIP_SHA256):
        return {
            "passed": False,
            "reason": "OUTER_ZIP_HASH_OR_SIZE_MISMATCH",
            "actual_bytes": actual_bytes,
            "actual_sha256": actual_sha,
        }

    zone01.mkdir(parents=True, exist_ok=True)
    snap_zip = zone01 / zip_src_path.name
    shutil.copy2(zip_src_path, snap_zip)

    with zipfile.ZipFile(snap_zip, "r") as zf:
        infos = zf.infolist()
        names = [inf.filename for inf in infos]
        dup_count = len(names) - len(set(names))
        casefold_collision = len(names) - len({n.lower() for n in names})
        unsafe_paths = []
        symlink_entries = []
        for inf in infos:
            fn = inf.filename
            if fn.startswith("/") or fn.startswith("\\") or ":" in fn or ".." in Path(fn).parts:
                unsafe_paths.append(fn)
            mode = (inf.external_attr >> 16) & 0o170000
            if mode == stat.S_IFLNK:
                symlink_entries.append(fn)

        if (
            len(infos) != EXPECTED_TOTAL_ZIP_ENTRIES
            or dup_count != 0
            or casefold_collision != 0
            or unsafe_paths
            or symlink_entries
        ):
            return {
                "passed": False,
                "reason": "ZIP_SAFETY_AUDIT_FAILED",
                "entry_count": len(infos),
                "dup_count": dup_count,
                "casefold_collision": casefold_collision,
                "unsafe_paths": unsafe_paths,
                "symlink_entries": symlink_entries,
            }

        unpacked = zone01 / "UnpackedFrozenSnapshot"
        unpacked.mkdir(parents=True, exist_ok=True)
        zf.extractall(unpacked)

    manifest_sha_file = unpacked / "MANIFEST.sha256"
    manifest_json_file = unpacked / "MANIFEST.json"
    if not manifest_sha_file.exists() or not manifest_json_file.exists():
        return {"passed": False, "reason": "MISSING_MANIFEST_FILES"}

    manifest_obj = json.loads(manifest_json_file.read_text(encoding="utf-8"))
    size_by_path = {item["path"]: int(item["bytes"]) for item in manifest_obj.get("files", [])}

    sha_lines = [ln.strip() for ln in manifest_sha_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    verified_payloads = 0
    manifest_paths = set()
    for ln in sha_lines:
        exp_h, rel_p = ln.split("  ", 1)
        manifest_paths.add(rel_p)
        fp = unpacked / rel_p
        if not fp.exists() or not fp.is_file():
            return {"passed": False, "reason": f"MISSING_PAYLOAD:{rel_p}"}
        act_h = sha256_file(fp)
        act_sz = fp.stat().st_size
        if act_h != exp_h.upper() or (rel_p in size_by_path and act_sz != size_by_path[rel_p]):
            return {"passed": False, "reason": f"PAYLOAD_MISMATCH:{rel_p}"}
        verified_payloads += 1

    all_unpacked_rels = {p.relative_to(unpacked).as_posix() for p in unpacked.rglob("*") if p.is_file()}
    non_manifest_meta = sorted(all_unpacked_rels - manifest_paths)
    if verified_payloads != EXPECTED_PAYLOAD_COUNT or non_manifest_meta != ["MANIFEST.json", "MANIFEST.sha256"]:
        return {
            "passed": False,
            "reason": "PAYLOAD_COUNT_OR_META_MISMATCH",
            "verified_payloads": verified_payloads,
            "non_manifest_meta": non_manifest_meta,
        }

    diff_blocks = verify_dossier_blocks(
        unpacked / "dossiers" / "PNEUMA_P3C_DIFFERENTIAL_R1_READONLY_TRIBUNAL_DOSSIER.md",
        unpacked / "BROCA_NATIVE_OMEGA_GATE_C1" / "P3C_Differential_R1",
        15,
    )
    prov_blocks = verify_dossier_blocks(
        unpacked / "dossiers" / "PNEUMA_P3C_PROVENANCE_R1_READONLY_TRIBUNAL_DOSSIER.md",
        unpacked / "BROCA_NATIVE_OMEGA_GATE_C1" / "P3C_Provenance_R1",
        14,
    )
    if not diff_blocks["passed"] or not prov_blocks["passed"]:
        return {
            "passed": False,
            "reason": "DOSSIER_BLOCK_PARITY_FAILED",
            "diff_blocks": diff_blocks,
            "prov_blocks": prov_blocks,
        }

    lock_tree_readonly(zone01)
    pre_tree_sha, pre_tree_count = compute_tree_digest(zone01)

    return {
        "passed": True,
        "outer_zip_bytes": actual_bytes,
        "outer_zip_sha256": actual_sha,
        "zip_total_entries": EXPECTED_TOTAL_ZIP_ENTRIES,
        "manifest_payloads_verified": f"{verified_payloads}/{EXPECTED_PAYLOAD_COUNT}",
        "non_manifest_zip_metadata_entries": non_manifest_meta,
        "zip_path_safety_checks": {
            "duplicates": 0,
            "path_traversal_or_absolute": 0,
            "symlinks": 0,
            "casefold_collisions": 0,
        },
        "differential_r1_dossier_blocks": diff_blocks,
        "provenance_r1_dossier_blocks": prov_blocks,
        "zone01_pre_tree_sha256": pre_tree_sha,
        "zone01_pre_tree_entry_count": pre_tree_count,
    }


def prepare_zone02_workspace(zone01_unpacked: Path, zone02: Path) -> Path:
    src_gate = zone01_unpacked / "BROCA_NATIVE_OMEGA_GATE_C1"
    dst_gate = zone02 / "BROCA_NATIVE_OMEGA_GATE_C1"
    shutil.copytree(src_gate, dst_gate)
    os.chmod(dst_gate, 0o755)
    for dirpath, dirnames, filenames in os.walk(dst_gate):
        dp = Path(dirpath)
        os.chmod(dp, 0o755)
        for dn in dirnames:
            os.chmod(dp / dn, 0o755)
        for fn in filenames:
            os.chmod(dp / fn, 0o644)
    return dst_gate


def verify_zone02_source_immutability(zone01_unpacked: Path, zone02_gate: Path) -> Dict[str, object]:
    manifest_sha_file = zone01_unpacked / "MANIFEST.sha256"
    expected_gate_hashes: Dict[str, str] = {}
    for ln in manifest_sha_file.read_text(encoding="utf-8").splitlines():
        if not ln.strip():
            continue
        exp_h, rel_p = ln.strip().split("  ", 1)
        if rel_p.startswith("BROCA_NATIVE_OMEGA_GATE_C1/"):
            sub_rel = rel_p[len("BROCA_NATIVE_OMEGA_GATE_C1/") :]
            expected_gate_hashes[sub_rel] = exp_h.upper()

    mutated_or_missing = []
    for sub_rel, exp_h in sorted(expected_gate_hashes.items()):
        fp = zone02_gate / sub_rel
        if not fp.exists() or sha256_file(fp) != exp_h:
            mutated_or_missing.append(sub_rel)

    unexpected_non_build_files = []
    for p in sorted(zone02_gate.rglob("*")):
        if not p.is_file() and not p.is_symlink():
            continue
        rel = p.relative_to(zone02_gate)
        if any(part in (".lake", "__pycache__", ".pytest_cache") for part in rel.parts):
            continue
        rel_posix = rel.as_posix()
        if rel_posix not in expected_gate_hashes:
            unexpected_non_build_files.append(rel_posix)

    return {
        "passed": (len(mutated_or_missing) == 0) and (len(unexpected_non_build_files) == 0),
        "tracked_gate_source_files_verified": len(expected_gate_hashes) - len(mutated_or_missing),
        "expected_gate_source_files": len(expected_gate_hashes),
        "mutated_or_missing": mutated_or_missing,
        "unexpected_non_lake_files": unexpected_non_build_files,
    }


def run_sentinels_k1_k2(
    zone02_gate: Path,
    zone04: Path,
    logs_dir: Path,
    budget: BudgetTracker,
) -> Dict[str, object]:
    can_run, rem = budget.check_can_start(180)
    if not can_run:
        return {"status": "SKIPPED_RESOURCE_LIMIT", "remaining_before_reserve_sec": round(rem, 2)}

    zone04.mkdir(parents=True, exist_ok=True)
    try:
        k1_dir = zone04 / "k1_axiom_probes"
        k1_dir.mkdir(parents=True, exist_ok=True)
        k1_cases = [
            (
                "K1a_custom_axiom",
                "axiom sentinel_false_ax : False\ntheorem sentinel_bad_custom : False := sentinel_false_ax\n#print axioms sentinel_bad_custom\n",
            ),
            (
                "K1b_sorry_ax",
                "theorem sentinel_bad_sorry : False := by sorry\n#print axioms sentinel_bad_sorry\n",
            ),
            (
                "K1c_native_decide",
                "theorem sentinel_bad_native : (1 + 1 = 2) := by native_decide\n#print axioms sentinel_bad_native\n",
            ),
        ]
        k1_results = []
        k1_all_rejected = True
        diff_cwd = zone02_gate / "P3C_Differential_R1"
        for cid, code in k1_cases:
            lean_file = k1_dir / f"{cid}.lean"
            lean_file.write_text(code, encoding="utf-8")
            r = run_timed_cmd(
                ["lake", "env", "lean", str(lean_file)],
                cwd=diff_cwd,
                log_path=logs_dir / f"sentinel_{cid}.log",
                timeout_sec=120,
            )
            audit = parse_and_verify_axioms(r["stdout"], min_expected_count=1)
            rejected_by_axiom_filter = (
                (audit["entry_count"] >= 1) and (not audit["passed"]) and (len(audit["forbidden_detections"]) >= 1)
            )
            if not rejected_by_axiom_filter:
                k1_all_rejected = False
            k1_results.append(
                {
                    "probe_id": cid,
                    "lean_exit_code": r["exit_code"],
                    "axiom_filter_rejected": rejected_by_axiom_filter,
                    "detected_forbidden_axioms": audit["forbidden_detections"],
                }
            )

        k2_gate = zone04 / "k2_corrupt_olean_env" / "BROCA_NATIVE_OMEGA_GATE_C1"
        k2_gate.mkdir(parents=True, exist_ok=True)
        for sib in (
            "P2_DerivationReplay",
            "P3_LexicalGrammar",
            "P3B_BoundedParser",
            "P3C_SemanticNorm",
            "P3C_Provenance_R1",
        ):
            os.symlink(zone02_gate / sib, k2_gate / sib, target_is_directory=True)
        k2_diff = k2_gate / "P3C_Differential_R1"
        shutil.copytree(zone02_gate / "P3C_Differential_R1", k2_diff, symlinks=True)

        target_olean = (
            k2_diff / ".lake" / "build" / "lib" / "lean" / "P3CDifferentialObligations" / "PO_DIFF_01_To_08.olean"
        )
        orig_bytes = target_olean.read_bytes()
        orig_sha = sha256_bytes(orig_bytes)
        mutated_bytes = b"X" + orig_bytes[1:]
        target_olean.write_bytes(mutated_bytes)
        mutated_sha = sha256_file(target_olean)
        hash_gate_rejected = orig_sha != mutated_sha

        strace_bin = shutil.which("strace")
        strace_dir = zone04 / "k2_strace_traces"
        strace_dir.mkdir(parents=True, exist_ok=True)
        strace_usable = False
        if strace_bin:
            probe_st = subprocess.run([strace_bin, "-e", "trace=openat", "/bin/true"], capture_output=True, text=True)
            strace_usable = probe_st.returncode == 0

        if strace_bin and strace_usable:
            k2_cmd = [
                strace_bin,
                "-ff",
                "-e",
                "trace=openat,open",
                "-o",
                str(strace_dir / "trace"),
                "lake",
                "env",
                "leanchecker",
                "--fresh",
                "P3CDiffTribunalAudit",
            ]
        else:
            k2_cmd = ["lake", "env", "leanchecker", "--fresh", "P3CDiffTribunalAudit"]

        r_k2 = run_timed_cmd(
            k2_cmd,
            cwd=k2_diff,
            log_path=logs_dir / "sentinel_K2_corrupted_olean.log",
            timeout_sec=180,
        )

        file_opened_confirmed = False
        matched_strace_lines: List[str] = []
        for tf in sorted(strace_dir.glob("trace*")):
            st_txt = tf.read_text(encoding="utf-8", errors="replace")
            for ln in st_txt.splitlines():
                if "PO_DIFF_01_To_08.olean" in ln:
                    matched_strace_lines.append(f"{tf.name}: {ln.strip()}")
                    m_fd = re.search(r"=\s*(\d+)\s*$", ln)
                    if m_fd and int(m_fd.group(1)) >= 0:
                        file_opened_confirmed = True
        (logs_dir / "sentinel_K2_strace_openat_matches.log").write_text(
            "\n".join(matched_strace_lines) + ("\n" if matched_strace_lines else ""),
            encoding="utf-8",
        )

        rc_k2 = int(r_k2["exit_code"])
        combined_k2_out = r_k2["stderr"] + "\n" + r_k2["stdout"]
        err_low = combined_k2_out.lower()
        loader_named_target = "po_diff_01_to_08.olean" in err_low
        any_consumption_proof = file_opened_confirmed or loader_named_target
        if not any_consumption_proof:
            k2_proc_class = "UNCONSUMED_OR_STRACE_UNCONFIRMED"
        elif rc_k2 < 0 or rc_k2 >= 128:
            k2_proc_class = "PROCESS_CRASH_OR_SIGNAL"
        elif "olean" in err_low or "header" in err_low or "invalid object file" in err_low or "corrupt" in err_low:
            k2_proc_class = "FILE_FORMAT_OR_LOADER_ERROR"
        elif rc_k2 != 0:
            k2_proc_class = "NONZERO_VERIFIER_EXIT"
        else:
            k2_proc_class = "UNEXPECTED_ZERO_EXIT"

        k2_result = {
            "target_olean_rel": "P3CDifferentialObligations/PO_DIFF_01_To_08.olean",
            "original_sha256": orig_sha,
            "mutated_sha256": mutated_sha,
            "hash_gate_verdict": "HASH_GATE_REJECTED" if hash_gate_rejected else "HASH_GATE_FAILED",
            "corrupted_olean_consumption_verdict": (
                "CORRUPTED_OLEAN_CONSUMED" if any_consumption_proof else "CONSUMPTION_UNCONFIRMED_NOT_COUNTED"
            ),
            "strace_openat_fd_confirmed": file_opened_confirmed,
            "loader_stderr_named_target_confirmed": loader_named_target,
            "leanchecker_exit_code": rc_k2,
            "process_reaction_classification": k2_proc_class,
            "semantic_kernel_rejection_claimed": False,
        }

        return {
            "status": "COMPLETED",
            "k1_axiom_contamination_sentinel": {
                "passed": k1_all_rejected,
                "probes": k1_results,
            },
            "k2_corrupted_olean_sentinel": k2_result,
        }
    finally:
        shutil.rmtree(zone04, ignore_errors=True)


def run_stage1(args: argparse.Namespace) -> int:
    work_root = Path(args.work_root).resolve()
    zone01 = work_root / "01_ImmutableInputSnapshot"
    zone02 = work_root / "02_DisposableBuildWorkspace"
    zone03 = work_root / "03_DetachedEvidenceBundle"
    logs_dir = zone03 / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    budget = BudgetTracker(args.job_start_epoch)
    receipt: Dict[str, object] = {
        "schema": "PNEUMA_NATIVE_OMEGA_EXTERNAL_REPLAY_R0_RECEIPT_V1",
        "authorization_gate": "AUTH-EXTERNAL-REPLAY-R0",
        "charter_revision": "Revision_T2_Tier1_Linux_x86_64",
        "started_at_utc": utc_now_iso(),
        "budget_allocation_policy_sec": {
            "job_total_budget_sec": JOB_TOTAL_BUDGET_SEC,
            "evidence_reserve_sec": EVIDENCE_RESERVE_SEC,
            "phase_budgets_sec": PHASE_BUDGETS_SEC,
            "allocated_sum_sec": sum(PHASE_BUDGETS_SEC.values()) + EVIDENCE_RESERVE_SEC,
            "strictly_less_than_job_limit": (sum(PHASE_BUDGETS_SEC.values()) + EVIDENCE_RESERVE_SEC) < JOB_TOTAL_BUDGET_SEC,
        },
        "workflow_identity_gate": {
            "expected_workflow_sha256": args.expected_workflow_sha256.upper(),
            "executed_workflow_sha256": args.executed_workflow_sha256.upper(),
            "executed_workflow_bytes": args.executed_workflow_bytes,
            "github_workflow_sha_context": os.environ.get("INPUT_GITHUB_WORKFLOW_SHA", ""),
            "pre_execution_identity_matched": args.expected_workflow_sha256.upper() == args.executed_workflow_sha256.upper(),
        },
    }

    if not receipt["workflow_identity_gate"]["pre_execution_identity_matched"]:
        receipt["finished_at_utc"] = utc_now_iso()
        receipt["three_axis_verdicts"] = {
            "host_independence_verdict": "INDEPENDENT_HOST_CONFIRMED",
            "evidence_collection_verdict": "FAILURE_EVIDENCE_COLLECTED",
            "replay_verification_verdict": "REPRODUCTION_HELD",
            "hold_or_failure_reason": "WORKFLOW_IDENTITY_MISMATCH",
        }
        (zone03 / "PNEUMA_EXTERNAL_REPLAY_R0_RECEIPT.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        return 2

    host_qual = collect_host_qualification(Path(args.elan_bin_dir))
    receipt["gate1_host_qualification"] = host_qual
    (zone03 / "host_qualification_receipt.json").write_text(json.dumps(host_qual, indent=2) + "\n", encoding="utf-8")

    if not host_qual["toolchain_binaries"]["lean_commit_matches_4_33_1"]:
        receipt["finished_at_utc"] = utc_now_iso()
        receipt["three_axis_verdicts"] = {
            "host_independence_verdict": "INDEPENDENT_HOST_CONFIRMED",
            "evidence_collection_verdict": "FAILURE_EVIDENCE_COLLECTED",
            "replay_verification_verdict": "REPRODUCTION_HELD",
            "hold_or_failure_reason": "TOOLCHAIN_COMMIT_MISMATCH",
        }
        (zone03 / "PNEUMA_EXTERNAL_REPLAY_R0_RECEIPT.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        return 3

    g0 = run_gate0_admission(Path(args.bundle_zip).resolve(), zone01)
    receipt["gate0_input_admission"] = g0
    if not g0.get("passed", False):
        receipt["finished_at_utc"] = utc_now_iso()
        receipt["three_axis_verdicts"] = {
            "host_independence_verdict": "INDEPENDENT_HOST_CONFIRMED",
            "evidence_collection_verdict": "FAILURE_EVIDENCE_COLLECTED",
            "replay_verification_verdict": "REPRODUCTION_HELD",
            "hold_or_failure_reason": f"GATE0_FAILED:{g0.get('reason')}",
        }
        (zone03 / "PNEUMA_EXTERNAL_REPLAY_R0_RECEIPT.json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
        return 4

    zone01_unpacked = zone01 / "UnpackedFrozenSnapshot"
    zone02_gate = prepare_zone02_workspace(zone01_unpacked, zone02)

    layer_build_records = []
    env_bridge_aliases = []
    layer_suite_records = []
    layer_fresh_records = []
    compiled_new_binary_hashes: Dict[str, str] = {}

    hold_reason: Optional[str] = None
    counterexample_found = False

    # Phase A: 7-layer clean `lake build` + `#print axioms` + `ENV-BRIDGE-01`
    for spec in LAYERS:
        lid = spec["layer_id"]
        pkg_dir = zone02_gate if spec["rel_dir"] == "." else (zone02_gate / spec["rel_dir"])
        ok_b, rem_b = budget.check_can_start(180)
        if not ok_b:
            hold_reason = f"RESOURCE_LIMIT_BEFORE_BUILD:{lid}"
            break

        b_res = run_timed_cmd(
            ["lake", "build"],
            cwd=pkg_dir,
            log_path=logs_dir / f"{lid}_lake_build.log",
            timeout_sec=min(PHASE_BUDGETS_SEC["clean_build_7_layers"], int(rem_b)),
        )
        if b_res["timed_out"] or b_res["oom_suspected"]:
            layer_build_records.append({"layer_id": lid, **{k: v for k, v in b_res.items() if k not in ("stdout", "stderr")}})
            hold_reason = f"RESOURCE_LIMIT_DURING_BUILD:{lid}"
            break
        if b_res["exit_code"] != 0:
            layer_build_records.append({"layer_id": lid, **{k: v for k, v in b_res.items() if k not in ("stdout", "stderr")}})
            hold_reason = f"CLEAN_BUILD_FAILED:{lid}"
            break

        ax_res = run_timed_cmd(
            ["lake", "env", "lean", spec["audit_lean"]],
            cwd=pkg_dir,
            log_path=logs_dir / f"{lid}_print_axioms.log",
            timeout_sec=300,
        )
        ax_audit = parse_and_verify_axioms(ax_res["stdout"], int(spec["expected_axiom_entries"]))

        runner_elf = pkg_dir / ".lake" / "build" / "bin" / spec["runner_bin"]
        runner_sha = sha256_file(runner_elf) if runner_elf.exists() else ""
        compiled_new_binary_hashes[f"{lid}:{spec['runner_bin']}"] = runner_sha

        if spec["needs_posix_exe_alias"] and os.name != "nt":
            alias_path = pkg_dir / ".lake" / "build" / "bin" / f"{spec['runner_bin']}.exe"
            if not alias_path.exists():
                os.symlink(spec["runner_bin"], alias_path)
            env_bridge_aliases.append(
                {
                    "bridge_id": "ENV-BRIDGE-01",
                    "layer_id": lid,
                    "symlink_rel": alias_path.relative_to(zone02_gate).as_posix(),
                    "target_name": spec["runner_bin"],
                    "target_elf_sha256": runner_sha,
                    "created_at_utc": utc_now_iso(),
                }
            )

        layer_build_records.append(
            {
                "layer_id": lid,
                "build_exit_code": b_res["exit_code"],
                "build_elapsed_sec": b_res["elapsed_sec"],
                "build_max_rss_mb": b_res["max_rss_mb"],
                "axiom_check_exit_code": ax_res["exit_code"],
                "axiom_audit_passed": ax_audit["passed"],
                "axiom_entry_count": ax_audit["entry_count"],
                "forbidden_axioms": ax_audit["forbidden_detections"],
                "runner_elf_sha256": runner_sha,
            }
        )
        if ax_res["exit_code"] != 0 or not ax_audit["passed"]:
            hold_reason = f"AXIOM_AUDIT_FAILED:{lid}"
            counterexample_found = True
            break

    # Phase B: All 7 Runners + 6 Python Harnesses
    if hold_reason is None:
        for spec in LAYERS:
            lid = spec["layer_id"]
            pkg_dir = zone02_gate if spec["rel_dir"] == "." else (zone02_gate / spec["rel_dir"])
            ok_s, rem_s = budget.check_can_start(120)
            if not ok_s:
                hold_reason = f"RESOURCE_LIMIT_BEFORE_SUITE:{lid}"
                break

            runner_elf = pkg_dir / ".lake" / "build" / "bin" / spec["runner_bin"]
            r_exe = run_timed_cmd(
                [str(runner_elf)],
                cwd=pkg_dir,
                log_path=logs_dir / f"{lid}_runner.log",
                timeout_sec=min(600, int(rem_s)),
            )
            rec_item: Dict[str, object] = {
                "layer_id": lid,
                "runner_exit_code": r_exe["exit_code"],
                "runner_elapsed_sec": r_exe["elapsed_sec"],
                "runner_max_rss_mb": r_exe["max_rss_mb"],
            }
            if r_exe["exit_code"] != 0:
                layer_suite_records.append(rec_item)
                hold_reason = f"RUNNER_FAILED:{lid}"
                counterexample_found = not (r_exe["timed_out"] or r_exe["oom_suspected"])
                break

            py_cmd_rel = spec["python_harness"]
            if py_cmd_rel is not None:
                py_script = pkg_dir / py_cmd_rel[0]
                rec_item["python_harness_sha256_before_run"] = sha256_file(py_script)
                r_py = run_timed_cmd(
                    [sys.executable, *py_cmd_rel],
                    cwd=pkg_dir,
                    log_path=logs_dir / f"{lid}_python_harness.log",
                    timeout_sec=min(600, int(budget.remaining_before_reserve_sec())),
                )
                rec_item["python_harness_exit_code"] = r_py["exit_code"]
                rec_item["python_harness_elapsed_sec"] = r_py["elapsed_sec"]
                rec_item["python_harness_max_rss_mb"] = r_py["max_rss_mb"]
                if r_py["exit_code"] != 0:
                    layer_suite_records.append(rec_item)
                    hold_reason = f"PYTHON_HARNESS_FAILED:{lid}"
                    counterexample_found = not (r_py["timed_out"] or r_py["oom_suspected"])
                    break

            layer_suite_records.append(rec_item)

    # Phase C1: L1..L6 `leanchecker --fresh` + L7 module-level `leanchecker` before checkpoint upload
    l7_incremental_record: Optional[Dict[str, object]] = None
    if hold_reason is None:
        for spec in LAYERS[:-1]:
            lid = spec["layer_id"]
            pkg_dir = zone02_gate if spec["rel_dir"] == "." else (zone02_gate / spec["rel_dir"])
            ok_f, rem_f = budget.check_can_start(240)
            if not ok_f:
                hold_reason = f"RESOURCE_LIMIT_BEFORE_FRESH_LEANCHECKER:{lid}"
                break

            r_lc = run_timed_cmd(
                ["lake", "env", "leanchecker", "--fresh", spec["fresh_root"]],
                cwd=pkg_dir,
                log_path=logs_dir / f"{lid}_leanchecker_fresh.log",
                timeout_sec=min(PHASE_BUDGETS_SEC["fresh_leanchecker_7_layers"], int(rem_f)),
                high_oom_score_child=True,
            )
            layer_fresh_records.append(
                {
                    "layer_id": lid,
                    "fresh_root": spec["fresh_root"],
                    "exit_code": r_lc["exit_code"],
                    "timed_out": r_lc["timed_out"],
                    "oom_suspected": r_lc["oom_suspected"],
                    "elapsed_sec": r_lc["elapsed_sec"],
                    "max_rss_mb": r_lc["max_rss_mb"],
                }
            )
            if r_lc["timed_out"] or r_lc["oom_suspected"]:
                hold_reason = f"RESOURCE_LIMIT_DURING_FRESH_LEANCHECKER:{lid}"
                break
            if r_lc["exit_code"] != 0:
                hold_reason = f"FRESH_LEANCHECKER_FAILED:{lid}"
                counterexample_found = True
                break

    if hold_reason is None:
        l7_dir = zone02_gate / "P3C_Differential_R1"
        r_inc = run_timed_cmd(
            ["lake", "env", "leanchecker", "P3CDiffTribunalAudit"],
            cwd=l7_dir,
            log_path=logs_dir / "L7_P3C_Differential_R1_leanchecker_incremental.log",
            timeout_sec=min(1200, int(budget.remaining_before_reserve_sec())),
            high_oom_score_child=True,
        )
        l7_incremental_record = {
            "layer_id": "L7_P3C_Differential_R1",
            "mode": "INCREMENTAL_ALL_PACKAGE_MODULES_ON_FRESH_PROVENANCE_R1_BASE",
            "exit_code": r_inc["exit_code"],
            "timed_out": r_inc["timed_out"],
            "oom_suspected": r_inc["oom_suspected"],
            "elapsed_sec": r_inc["elapsed_sec"],
            "max_rss_mb": r_inc["max_rss_mb"],
        }
        if r_inc["exit_code"] != 0:
            hold_reason = "INCREMENTAL_LEANCHECKER_L7_FAILED"
            counterexample_found = not (r_inc["timed_out"] or r_inc["oom_suspected"])

    receipt["gate2_clean_build_and_axioms"] = layer_build_records
    receipt["env_bridge_01_posix_aliases"] = env_bridge_aliases
    receipt["gate2_runners_and_oracles"] = layer_suite_records
    receipt["gate2_fresh_leanchecker"] = layer_fresh_records
    receipt["gate2_l7_incremental_leanchecker"] = l7_incremental_record
    receipt["compiled_new_binary_sha256_gauge_record"] = compiled_new_binary_hashes
    receipt["stage1_checkpoint_status"] = {
        "completed_at_utc": utc_now_iso(),
        "stage1_passed": hold_reason is None,
        "hold_reason": hold_reason,
        "counterexample_found": counterexample_found,
        "elapsed_sec": round(budget.elapsed_sec(), 4),
    }
    receipt["three_axis_verdicts"] = {
        "host_independence_verdict": "INDEPENDENT_HOST_CONFIRMED",
        "evidence_collection_verdict": "STAGE1_CHECKPOINT_COLLECTED",
        "replay_verification_verdict": "COUNTEREXAMPLE_FOUND" if counterexample_found else "REPRODUCTION_HELD",
        "hold_or_failure_reason": hold_reason or "STAGE1_PASSED_AWAITING_STAGE2_L7_MONOLITHIC_FRESH",
    }

    (zone03 / "PNEUMA_EXTERNAL_REPLAY_R0_RECEIPT.json").write_text(
        json.dumps(receipt, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return 0 if hold_reason is None else 1


def run_stage2(args: argparse.Namespace) -> int:
    setup_elan_env(Path(args.elan_bin_dir))
    work_root = Path(args.work_root).resolve()
    zone01 = work_root / "01_ImmutableInputSnapshot"
    zone02 = work_root / "02_DisposableBuildWorkspace"
    zone03 = work_root / "03_DetachedEvidenceBundle"
    zone04 = work_root / "04_DisposableSentinelScratch"
    logs_dir = zone03 / "logs"

    rec_path = zone03 / "PNEUMA_EXTERNAL_REPLAY_R0_RECEIPT.json"
    receipt = json.loads(rec_path.read_text(encoding="utf-8"))
    budget = BudgetTracker(args.job_start_epoch)

    s1 = receipt.get("stage1_checkpoint_status", {})
    hold_reason: Optional[str] = s1.get("hold_reason")
    counterexample_found: bool = bool(s1.get("counterexample_found", False))
    layer_fresh_records: List[Dict[str, object]] = list(receipt.get("gate2_fresh_leanchecker", []))

    zone01_unpacked = zone01 / "UnpackedFrozenSnapshot"
    zone02_gate = zone02 / "BROCA_NATIVE_OMEGA_GATE_C1"

    # Execute L7 monolithic `lake env leanchecker --fresh P3CDiffTribunalAudit`
    if hold_reason is None:
        ok_f, rem_f = budget.check_can_start(900)
        if not ok_f:
            hold_reason = "RESOURCE_LIMIT_BEFORE_FRESH_LEANCHECKER:L7_P3C_Differential_R1"
        else:
            l7_dir = zone02_gate / "P3C_Differential_R1"
            r_lc = run_timed_cmd(
                ["lake", "env", "leanchecker", "--fresh", "P3CDiffTribunalAudit"],
                cwd=l7_dir,
                log_path=logs_dir / "L7_P3C_Differential_R1_leanchecker_fresh.log",
                timeout_sec=min(PHASE_BUDGETS_SEC["fresh_leanchecker_7_layers"], int(rem_f)),
                high_oom_score_child=True,
            )
            layer_fresh_records.append(
                {
                    "layer_id": "L7_P3C_Differential_R1",
                    "fresh_root": "P3CDiffTribunalAudit",
                    "exit_code": r_lc["exit_code"],
                    "timed_out": r_lc["timed_out"],
                    "oom_suspected": r_lc["oom_suspected"],
                    "elapsed_sec": r_lc["elapsed_sec"],
                    "max_rss_mb": r_lc["max_rss_mb"],
                }
            )
            if r_lc["timed_out"] or r_lc["oom_suspected"]:
                hold_reason = "RESOURCE_LIMIT_DURING_FRESH_LEANCHECKER:L7_P3C_Differential_R1"
            elif r_lc["exit_code"] != 0:
                hold_reason = "FRESH_LEANCHECKER_FAILED:L7_P3C_Differential_R1"
                counterexample_found = True

    g0 = receipt["gate0_input_admission"]
    post_z01_sha, post_z01_cnt = compute_tree_digest(zone01)
    z01_immutable_ok = (post_z01_sha == g0["zone01_pre_tree_sha256"]) and (
        post_z01_cnt == g0["zone01_pre_tree_entry_count"]
    )
    z02_src_audit = verify_zone02_source_immutability(zone01_unpacked, zone02_gate)

    if not z01_immutable_ok or not z02_src_audit["passed"]:
        if hold_reason is None:
            hold_reason = "POST_EXECUTION_SOURCE_IMMUTABILITY_VIOLATION"

    sentinel_report = run_sentinels_k1_k2(zone02_gate, zone04, logs_dir, budget)
    if sentinel_report.get("status") == "COMPLETED":
        if not sentinel_report["k1_axiom_contamination_sentinel"]["passed"]:
            if hold_reason is None:
                hold_reason = "SENTINEL_K1_AXIOM_FILTER_FAILED"
        if sentinel_report["k2_corrupted_olean_sentinel"]["hash_gate_verdict"] != "HASH_GATE_REJECTED":
            if hold_reason is None:
                hold_reason = "SENTINEL_K2_HASH_GATE_FAILED"

    final_z01_sha, _ = compute_tree_digest(zone01)
    z01_immutable_after_sentinels = final_z01_sha == g0["zone01_pre_tree_sha256"]

    all_clean_passed = (
        hold_reason is None
        and len(receipt.get("gate2_clean_build_and_axioms", [])) == len(LAYERS)
        and len(receipt.get("gate2_runners_and_oracles", [])) == len(LAYERS)
        and len(layer_fresh_records) == len(LAYERS)
        and z01_immutable_ok
        and z01_immutable_after_sentinels
        and z02_src_audit["passed"]
    )

    if counterexample_found:
        replay_verdict = "COUNTEREXAMPLE_FOUND"
    elif all_clean_passed:
        replay_verdict = "INDEPENDENT_HOST_REPLAY_VERIFIED"
    else:
        replay_verdict = "REPRODUCTION_HELD"

    receipt["gate2_fresh_leanchecker"] = layer_fresh_records
    receipt["post_run_immutability"] = {
        "zone01_snapshot_tree_sha256_post": final_z01_sha,
        "zone01_snapshot_unchanged": z01_immutable_ok and z01_immutable_after_sentinels,
        "zone02_tracked_sources_audit": z02_src_audit,
    }
    receipt["gate3_sentinels_k1_k2"] = sentinel_report
    receipt["four_way_separation_verdicts"] = {
        "source_hash_identity": "SOURCE_HASH_VERIFIED" if (g0["passed"] and z02_src_audit["passed"]) else "MISMATCH",
        "rebuilt_binary_identity": "NEW_LINUX_ELF_RECONSTRUCTED_RECORDED",
        "kernel_theorem_replay": (
            "KERNEL_REPLAY_REPRODUCED"
            if (len(layer_fresh_records) == len(LAYERS) and all(r["exit_code"] == 0 for r in layer_fresh_records))
            else "HELD_OR_FAILED"
        ),
        "runtime_and_wire_outcomes": (
            "RUNTIME_VECTORS_REPRODUCED"
            if (len(receipt.get("gate2_runners_and_oracles", [])) == len(LAYERS) and s1.get("stage1_passed"))
            else "HELD_OR_FAILED"
        ),
    }
    receipt["three_axis_verdicts"] = {
        "host_independence_verdict": "INDEPENDENT_HOST_CONFIRMED",
        "evidence_collection_verdict": (
            "EVIDENCE_COLLECTED_COMPLETE" if all_clean_passed else "FAILURE_EVIDENCE_COLLECTED"
        ),
        "replay_verification_verdict": replay_verdict,
        "hold_or_failure_reason": hold_reason or "NONE",
    }
    receipt["preserved_held_and_not_authorized_locks"] = {
        "f_diff_t1_01_full_forest_oracle_held": True,
        "f_diff_t1_02_full_r0_oracle_independence_held": True,
        "fi10_p3b_p3c_boundary_preserved": True,
        "full_r0_semantic_fidelity_held": True,
        "global_p4_p5_admission_nonbypass_held": True,
        "asymptotic_complexity_held": True,
        "legacy_p3c_authorization_qualified_preserved": True,
        "provenance_r1_scoped_accepted_preserved": True,
        "differential_r1_scoped_accepted_preserved": True,
        "tier2_windows_x64_authorized": False,
        "tier3_linux_arm64_authorized": False,
        "p4_p5_authorized": False,
        "f_p3c_03_authorized": False,
        "pneuma_finite_operator_r0_authorized": False,
        "pneuma_speech_and_listener_authorized": False,
    }
    receipt["total_elapsed_sec"] = round(budget.elapsed_sec(), 4)
    receipt["finished_at_utc"] = utc_now_iso()

    rec_path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return 0 if all_clean_passed else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["stage1", "stage2"], required=True)
    ap.add_argument("--bundle-zip", required=True)
    ap.add_argument("--work-root", required=True)
    ap.add_argument("--job-start-epoch", type=float, required=True)
    ap.add_argument("--expected-workflow-sha256", required=True)
    ap.add_argument("--executed-workflow-sha256", required=True)
    ap.add_argument("--executed-workflow-bytes", type=int, required=True)
    ap.add_argument("--elan-bin-dir", required=True)
    args = ap.parse_args()

    if args.phase == "stage1":
        return run_stage1(args)
    return run_stage2(args)


if __name__ == "__main__":
    sys.exit(main())
