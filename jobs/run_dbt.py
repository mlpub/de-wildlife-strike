import os
import subprocess
from pathlib import Path


def _resolve_profiles_dir(env: dict[str, str]) -> Path:
    configured = env.get("DBT_PROFILES_DIR")
    if configured:
        return Path(configured).expanduser().resolve()

    repo_local = Path(__file__).resolve().parent.parent / ".dbt"
    home_dir = Path.home() / ".dbt"
    docker_dir = Path("/app/.dbt")

    for candidate in (repo_local, home_dir, docker_dir):
        if candidate.exists():
            return candidate.resolve()

    return repo_local.resolve()


def _validate_profiles_dir(profiles_dir: Path) -> None:
    checked_paths = [
        str((Path(__file__).resolve().parent.parent / ".dbt").resolve()),
        str((Path.home() / ".dbt").resolve()),
        "/app/.dbt",
    ]

    if not profiles_dir.exists():
        raise RuntimeError(
            "dbt profiles directory does not exist: "
            f"{profiles_dir}. Checked defaults: {', '.join(checked_paths)}. "
            "Set DBT_PROFILES_DIR to a valid directory."
        )

    profile_file = profiles_dir / "profiles.yml"
    if not profile_file.exists():
        raise RuntimeError(
            f"dbt profiles file is missing: {profile_file}. "
            "Create profiles.yml or set DBT_PROFILES_DIR to a directory containing it."
        )


def run_dbt(batch_id: str) -> None:
    print(f"Running dbt for batch_id={batch_id}")

    env = os.environ.copy()
    profiles_dir = _resolve_profiles_dir(env)
    _validate_profiles_dir(profiles_dir)
    env["DBT_PROFILES_DIR"] = str(profiles_dir)

    print(f"Using DBT_PROFILES_DIR={profiles_dir}")

    result = subprocess.run(
        [
            "dbt",
            "build",
            "--vars",
            f"batch_id: {batch_id}",
        ],
        env=env,
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt failed with exit code {result.returncode}")
