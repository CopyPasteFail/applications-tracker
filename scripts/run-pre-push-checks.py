import subprocess
import sys

commands = [
    [sys.executable, "-m", "ruff", "check", "."],
    [sys.executable, "-m", "pytest"],
]

for cmd in commands:
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(result.returncode)
