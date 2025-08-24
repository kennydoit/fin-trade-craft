"""Feature build pre-processing.

Implements the steps from prompts/feature_build.py.md:
  1) Read features/config.yaml
  2) Resolve `universe` to a concrete universe_id (UUID):
	 - If `universe` looks like a UUID, validate it exists in transformed.symbol_universes
	 - Else treat as universe_name and pick the latest by load_date_time
	3) Create an output folder in features:
		 - If config has key `folder`, treat it as a template and replace tokens:
				 {timestamp} -> current UTC timestamp (YYYYMMDDTHHMMSSZ)
				 {universe_id} -> resolved UUID
			 Otherwise, default to {universe_id}_{timestamp}
		 - If the target folder already exists, remove it and recreate (start fresh)
	4) Place a copy of config.yaml in the output folder

Stops after these steps and awaits further instructions for actual feature building.
"""

from __future__ import annotations

import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

# Allow imports from the repository root (so we can import db.postgres_database_manager)
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
	sys.path.append(str(REPO_ROOT))

from db.postgres_database_manager import PostgresDatabaseManager


def _is_uuid(value: str) -> bool:
	try:
		uuid.UUID(str(value))
		return True
	except Exception:
		return False


def _resolve_universe_id(db: PostgresDatabaseManager, universe_value: str) -> uuid.UUID:
	"""Resolve a universe value (UUID or name) to a concrete universe_id.

	Rules per prompt:
	  - If value is a UUID: validate it exists in transformed.symbol_universes
	  - Else treat as universe_name and select the latest load by load_date_time
	"""
	if _is_uuid(universe_value):
		rows = db.fetch_query(
			"""
			SELECT 1
			FROM transformed.symbol_universes
			WHERE universe_id = %s
			LIMIT 1
			""",
			(str(universe_value),),
		)
		if not rows:
			raise ValueError(
				f"universe_id {universe_value} not found in transformed.symbol_universes"
			)
		return uuid.UUID(str(universe_value))

	# Treat as universe_name; pick the latest instance by load_date_time
	rows = db.fetch_query(
		"""
		SELECT universe_id
		FROM transformed.symbol_universes
		WHERE universe_name = %s
		ORDER BY load_date_time DESC
		LIMIT 1
		""",
		(universe_value,),
	)
	if not rows:
		raise ValueError(
			f"universe_name '{universe_value}' not found in transformed.symbol_universes"
		)
	resolved = rows[0][0]
	return uuid.UUID(str(resolved))


def _load_config(config_path: Path) -> dict[str, Any]:
	if not config_path.exists():
		raise FileNotFoundError(f"Config file not found: {config_path}")
	with config_path.open("r", encoding="utf-8") as f:
		data = yaml.safe_load(f) or {}
	if not isinstance(data, dict):
		raise ValueError("config.yaml must parse to a mapping (YAML object)")
	if "universe" not in data:
		raise ValueError("config.yaml missing required key: 'universe'")
	return data


def main() -> None:
	features_dir = Path(__file__).resolve().parent
	config_path = features_dir / "config.yaml"

	# 1) Read config
	config = _load_config(config_path)

	# 2) Resolve universe -> universe_id
	db = PostgresDatabaseManager()
	db.connect()
	try:
		universe_value = str(config["universe"]).strip()
		universe_id = _resolve_universe_id(db, universe_value)
	finally:
		db.close()

	# 3) Create output folder using config folder template if provided
	ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
	folder_template = str(config.get("folder", "{universe_id}_{timestamp}"))
	folder_name = (
		folder_template.replace("{timestamp}", ts).replace("{universe_id}", str(universe_id))
	)
	out_dir = features_dir / folder_name
	if out_dir.exists():
		# Remove existing folder to start fresh per instructions
		shutil.rmtree(out_dir)
	out_dir.mkdir(parents=True, exist_ok=True)

	# 4) Copy config.yaml into output folder
	shutil.copy2(config_path, out_dir / "config.yaml")

	print(f"Resolved universe_id: {universe_id}")  # noqa: T201
	print(f"Output folder ready: {out_dir}")  # noqa: T201
	print("STOP: Await further instructions")  # noqa: T201


if __name__ == "__main__":  # pragma: no cover - CLI entry point
	try:
		main()
	except Exception as e:  # keep simple and visible for script usage
		print(f"Error: {e}")  # noqa: T201
		sys.exit(1)

