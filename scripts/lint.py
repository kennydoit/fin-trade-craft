#!/usr/bin/env python3
"""
Linting script for the fin-trade-craft project.
Runs ruff linter and black formatter on the codebase.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n{'='*50}")
    print(f"Running {description}...")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*50}")

    try:
        result = subprocess.run(cmd, check=False, capture_output=False)
        if result.returncode == 0:
            print(f"✅ {description} passed!")
            return True
        print(f"❌ {description} failed with exit code {result.returncode}")
        return False
    except Exception as e:
        print(f"❌ Error running {description}: {e}")
        return False


def main():
    """Run linting and formatting checks."""
    project_root = Path(__file__).parent.parent

    print(f"Running linting for fin-trade-craft project at: {project_root}")

    # Change to project directory
    import os

    os.chdir(project_root)

    success = True

    # Run ruff linter
    success &= run_command(["uv", "run", "ruff", "check", "."], "Ruff linting")

    # Run black formatter check
    success &= run_command(
        ["uv", "run", "black", "--check", "."], "Black formatting check"
    )

    print(f"\n{'='*50}")
    if success:
        print("🎉 All linting checks passed!")
        sys.exit(0)
    else:
        print("💥 Some linting checks failed!")
        print("\nTo auto-fix issues, run:")
        print("  uv run ruff check --fix .")
        print("  uv run black .")
        sys.exit(1)


if __name__ == "__main__":
    main()
