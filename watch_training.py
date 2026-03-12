import json
import time
import os
from pathlib import Path

progress_file = Path("train_progress.json")

print("Watching training progress... (Ctrl+C to stop)\n")

last_data = None
while True:
    try:
        if progress_file.exists():
            data = json.loads(progress_file.read_text(encoding="utf-8"))
            if data != last_data:
                last_data = data
                stage = data.get("stage", "?")

                if stage == "train_step":
                    epoch = data.get("epoch", "?")
                    total_epochs = data.get("total_epochs", "?")
                    step = data.get("step", "?")
                    total_steps = data.get("total_steps", "?")
                    loss = data.get("loss", 0)
                    pct = round(100 * step / total_steps) if total_steps else 0
                    bar = ("█" * (pct // 5)).ljust(20)
                    updated = data.get("updated_at", "")
                    print(f"\r[{updated}]  Epoch {epoch}/{total_epochs}  |{bar}| {pct}%  step {step}/{total_steps}  loss={loss:.6f}    ", end="", flush=True)

                elif stage == "train_epoch_done":
                    epoch = data.get("epoch", "?")
                    total_epochs = data.get("total_epochs", "?")
                    avg_loss = data.get("avg_loss", 0)
                    print(f"\n✓ Epoch {epoch}/{total_epochs} complete — avg loss={avg_loss:.6f}", flush=True)

                elif stage == "train_epoch_start":
                    epoch = data.get("epoch", "?")
                    total_epochs = data.get("total_epochs", "?")
                    print(f"\n▶ Starting epoch {epoch}/{total_epochs}...", flush=True)

                else:
                    print(f"\n[{data.get('updated_at','')}] {data}", flush=True)
        else:
            print("\rWaiting for training to start...", end="", flush=True)

    except Exception:
        pass

    time.sleep(2)
