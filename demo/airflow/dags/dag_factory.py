"""
DAG factory — scans /opt/airflow/app/config/*.yaml and registers one DAG per file.

Each YAML must have: read_type, write_type, metric_type, source, sink, metric.
Optional: spark (dict of SparkSession config key-value pairs).

Git sync helpers
----------------
fetch_and_get_changed_configs(config_dir)
    git-fetch the remote, return YAML configs added/modified in commits not yet local.

get_changed_configs_in_latest_commit(config_dir)
    Return YAML configs touched by the latest local commit (HEAD).
    Call this after a git pull to see what just arrived.

Both functions can also be run from the command line:
    python dag_factory.py fetch      # fetch remote + print changed configs
    python dag_factory.py latest     # show configs changed in HEAD
"""
from __future__ import annotations

import logging
import subprocess
from datetime import timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.utils.dates import days_ago

from adapters.factory.adapter_config import MetricAdapterType
from orchestration.operators.init_operator import InitOperator
from orchestration.operators.metric_operator import MetricPushOperator
from orchestration.operators.spark_run_operator import SparkRunOperator

log = logging.getLogger(__name__)

_CONFIG_DIR = Path("/opt/airflow/app/config")
_GIT_REMOTE  = "origin"


# ── Git helpers ───────────────────────────────────────────────────────────────

def _run_git(args: list[str], cwd: Path) -> str:
    """Run a git command rooted at cwd and return stdout. Raises on failure."""
    result = subprocess.run(
        ["git"] + args,
        capture_output=True, text=True, check=True, cwd=str(cwd),
    )
    return result.stdout.strip()


def _git_root(start: Path) -> Path:
    """Return the root of the git repository containing start."""
    return Path(_run_git(["rev-parse", "--show-toplevel"], start))


def _filter_configs(changed_names: set[str], config_dir: Path) -> list[Path]:
    """Intersect a set of bare filenames with *.yaml files in config_dir."""
    return [p for p in sorted(config_dir.glob("*.yaml")) if p.name in changed_names]


def fetch_and_get_changed_configs(
    config_dir: Path = _CONFIG_DIR,
    remote: str = _GIT_REMOTE,
) -> list[Path]:
    """
    Run `git fetch <remote>`, then return YAML files inside config_dir that
    were added or modified in remote commits not yet in the local branch.

    Returns an empty list when already up-to-date or if git is unavailable.
    """
    try:
        repo = _git_root(config_dir)
        _run_git(["fetch", remote], repo)
        raw = _run_git(
            ["diff", "--name-only", "--diff-filter=AM", f"HEAD..{remote}/HEAD"],
            repo,
        )
    except subprocess.CalledProcessError as exc:
        log.warning("[dag_factory] git fetch failed: %s", exc.stderr.strip())
        return []

    changed_names = {Path(line).name for line in raw.splitlines() if line.strip()}
    matched = _filter_configs(changed_names, config_dir)
    log.info(
        "[dag_factory] fetch: %d config(s) changed on remote — %s",
        len(matched), [p.name for p in matched],
    )
    return matched


def get_changed_configs_in_latest_commit(
    config_dir: Path = _CONFIG_DIR,
) -> list[Path]:
    """
    Return YAML files in config_dir that were added or modified in HEAD.
    Call this after `git pull` to identify which DAGs just changed.
    """
    try:
        repo = _git_root(config_dir)
        raw = _run_git(
            ["show", "--name-only", "--diff-filter=AM", "--pretty=format:", "HEAD"],
            repo,
        )
    except subprocess.CalledProcessError as exc:
        log.warning("[dag_factory] git show failed: %s", exc.stderr.strip())
        return []

    changed_names = {Path(line).name for line in raw.splitlines() if line.strip()}
    return _filter_configs(changed_names, config_dir)


# ── DAG construction ──────────────────────────────────────────────────────────

def _dag_id_from_path(path: Path) -> str:
    """pipeline_config.yaml  →  pipeline_config"""
    return path.stem


def _make_dag(config_path: Path) -> DAG:
    cfg = yaml.safe_load(config_path.read_text())
    dag_id = _dag_id_from_path(config_path)
    metric_type = MetricAdapterType(cfg["metric_type"])
    metric_config = cfg["metric"]
    spark_config = cfg.get("spark", {})

    default_args = {
        "owner": cfg.get("owner", "ingestion"),
        "retries": cfg.get("retries", 0),
        "retry_delay": timedelta(minutes=cfg.get("retry_delay_minutes", 5)),
    }

    with DAG(
        dag_id=dag_id,
        description=cfg.get("description", f"Ingestion pipeline — {dag_id}"),
        schedule_interval=cfg.get("schedule_interval"),
        start_date=days_ago(1),
        default_args=default_args,
        catchup=False,
        tags=cfg.get("tags", ["ingestion"]),
    ) as dag:

        init = InitOperator(
            task_id="init",
            config_path=str(config_path),
            checkpoint_conn_id="postgres_checkpoint",
            openbao_conn_id="openbao_default",
            xcom_key="run_context",
        )

        spark_run = SparkRunOperator(
            task_id="spark_run",
            init_task_id="init",
            xcom_key="run_context",
            checkpoint_conn_id="postgres_checkpoint",
            metric_type=metric_type,
            metric_config_raw=metric_config,
            spark_config=spark_config,
        )

        metric_push = MetricPushOperator(
            task_id="metric_push",
            init_task_id="init",
            spark_task_id="spark_run",
            metric_type=metric_type,
            metric_config_raw=metric_config,
            status="success",
            extra_payload={"pipeline": dag_id},
            checkpoint_conn_id="postgres_checkpoint",
        )

        init >> spark_run >> metric_push

    return dag


# ── Factory registration ──────────────────────────────────────────────────────
# Airflow discovers DAG objects at module level.
# We register all configs but flag which ones changed in the latest commit.

try:
    _recently_changed = {p.name for p in get_changed_configs_in_latest_commit(_CONFIG_DIR)}
except Exception:
    _recently_changed = set()

for _config_file in sorted(_CONFIG_DIR.glob("*.yaml")):
    try:
        _dag = _make_dag(_config_file)
        globals()[_dag.dag_id] = _dag
        _status = "new/updated" if _config_file.name in _recently_changed else "registered"
        log.info("[dag_factory] %s DAG '%s' from %s", _status, _dag.dag_id, _config_file.name)
    except Exception as exc:
        log.error("[dag_factory] failed to load '%s': %s", _config_file.name, exc)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.WARNING)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "latest"

    if cmd == "fetch":
        configs = fetch_and_get_changed_configs(_CONFIG_DIR)
        label = "configs changed on remote (not yet pulled)"
    elif cmd == "latest":
        configs = get_changed_configs_in_latest_commit(_CONFIG_DIR)
        label = "configs changed in HEAD"
    else:
        print(f"Usage: python dag_factory.py [fetch|latest]")
        sys.exit(1)

    if configs:
        print(f"{label}:")
        for p in configs:
            print(f"  {p.name}  →  DAG: {_dag_id_from_path(p)}")
    else:
        print(f"No changes found ({label}).")
