from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

PIPELINE_STAGES = [
    ("ingest", SRC_DIR / "01_ingest.py"),
    ("clean", SRC_DIR / "02_clean.py"),
    ("transform", SRC_DIR / "03_transform.py"),
    ("validate", SRC_DIR / "04_validate.py"),
]


def load_stage(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load pipeline stage: {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    for stage_name, stage_path in PIPELINE_STAGES:
        print(f"\n=== Running {stage_name} stage ===")
        load_stage(stage_path).main()

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
