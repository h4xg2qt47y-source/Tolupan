"""
Write TTS training snapshots every N global steps (default 250).

Used as a Trainer `on_train_step_end` callback from train_tts_finetune.py.
Updates:
  - TTS_Model_v2/TTS_PROGRESS.md       (human-readable, latest only)
  - TTS_Model_v2/TTS_PROGRESS_HISTORY.jsonl  (one JSON object per snapshot)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def _scalar(x):
    if x is None:
        return None
    if hasattr(x, "item"):
        return float(x.item())
    try:
        return float(x)
    except (TypeError, ValueError):
        return str(x)


def make_on_train_step_end(progress_dir: Path, every_n_steps: int = 250):
    """Return a callback suitable for Trainer(..., callbacks={...})."""

    progress_dir = Path(progress_dir)
    progress_dir.mkdir(parents=True, exist_ok=True)
    md_path = progress_dir / "TTS_PROGRESS.md"
    jsonl_path = progress_dir / "TTS_PROGRESS_HISTORY.jsonl"

    def on_train_step_end(trainer):
        gs = int(trainer.total_steps_done)
        if gs <= 0 or gs % every_n_steps != 0:
            return

        avg = {}
        if trainer.keep_avg_train is not None:
            for k, v in trainer.keep_avg_train.avg_values.items():
                avg[k] = _scalar(v)

        row = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "global_step": gs,
            "epoch": int(trainer.epochs_done),
            "epochs_total": int(trainer.config.epochs),
            "steps_per_epoch": len(trainer.train_loader) if trainer.train_loader else None,
            "output_path": str(getattr(trainer, "output_path", "")),
            "avg": avg,
        }

        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

        mel = avg.get("avg_loss_mel")
        kl = avg.get("avg_loss_kl")
        dur = avg.get("avg_loss_duration")
        gen = avg.get("avg_loss_gen")
        disc = avg.get("avg_loss_disc")
        st = avg.get("avg_step_time")

        lines = [
            "# Tol TTS — training progress",
            "",
            f"**Last snapshot:** `{row['ts_utc']}`  ",
            f"**Global step:** {gs}  ",
            f"**Epoch:** {row['epoch'] + 1} / {row['epochs_total']} (0-based epoch index: {row['epoch']})  ",
            "",
            "## Running-average losses (since epoch start)",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| avg_loss_mel | {mel} |",
            f"| avg_loss_kl | {kl} |",
            f"| avg_loss_duration | {dur} |",
            f"| avg_loss_gen | {gen} |",
            f"| avg_loss_disc | {disc} |",
            f"| avg_step_time (s) | {st} |",
            "",
            "---",
            "",
            f"Full history: `{jsonl_path.name}` (one JSON line per {every_n_steps} steps).  ",
            f"Detailed console log: run folder `trainer_0_log.txt`.  ",
            "",
        ]
        md_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"\n  [TTS progress] Wrote {md_path.name} @ global_step={gs}\n")

    return on_train_step_end
