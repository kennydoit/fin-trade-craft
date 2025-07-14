#!/usr/bin/env python3
"""
Auto-fix script for the fin-trade-craft project.
Runs ruff auto-fix and black formatter to clean up code issues.
"""

import subprocess
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n{'='*50}")
    print(f"Running {description}...")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*50}")

    try:
        result = subprocess.run(cmd, check=False, capture_output=False)
        print(f"✅ {description} completed with exit code {result.returncode}")
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error running {description}: {e}")
        return False


def main():
    """Run auto-fixing tools."""
    project_root = Path(__file__).parent.parent

    print(f"Running auto-fix for fin-trade-craft project at: {project_root}")

    # Change to project directory
    import os

    os.chdir(project_root)

    # Run ruff auto-fix
    run_command(["uv", "run", "ruff", "check", "--fix", "."], "Ruff auto-fix")

    # Run black formatter
    run_command(["uv", "run", "black", "."], "Black formatting")

    print(f"\n{'='*50}")
    print("🔧 Auto-fix completed!")
    print("Run 'python scripts/lint.py' to check remaining issues.")


if __name__ == "__main__":
    main()
