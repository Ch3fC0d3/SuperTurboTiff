#!/usr/bin/env python3

import argparse
import base64
import datetime
import json
import math
import re
import warnings
from pathlib import Path

import cv2
import numpy as np
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset
from torch.utils.data._utils.collate import default_collate

try:
    from PIL import Image
except Exception:
    Image = None
else:
    try:
        Image.MAX_IMAGE_PIXELS = None
        warnings.filterwarnings("ignore", category=Image.DecompressionBombWarning)
    except Exception:
        pass

from ai_tracer import CurveSegNet


def _normalize_mode_name(mode: str | None) -> str:
    mode_name = str(mode or "").strip().lower()
    if not mode_name:
        return "black"
    return mode_name


def _decode_data_url_image(data_url: str) -> np.ndarray:
    b64 = data_url.split(",", 1)[1]
    img_bytes = base64.b64decode(b64)
    nparr = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("Failed to decode image")
    return img


def _load_roi_from_path(image_path: str, top: int, bot: int, left: int, right: int):
    if Image is None:
        return None, None

    try:
        with Image.open(image_path) as img:
            w, h = img.size
            top = max(0, min(top, h))
            bot = max(0, min(bot, h))
            left = max(0, min(left, w))
            right = max(0, min(right, w))
            if bot <= top or right <= left:
                return None, None
            roi_img = img.crop((left, top, right, bot)).convert("RGB")
            return np.array(roi_img), (right - left)
    except Exception:
        return None, None


def _iter_json_items(path: Path):
    use_jsonl = path.suffix.lower() == ".jsonl"
    if use_jsonl:
        with path.open("r", encoding="utf-8", buffering=65536) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
        return

    def _iter_streamed_array(fh, initial_buffer: str):
        decoder = json.JSONDecoder()
        buffer = initial_buffer
        idx = 0

        def _fill():
            nonlocal buffer, idx
            chunk = fh.read(262144)
            if not chunk:
                return False
            if idx > 0:
                buffer = buffer[idx:] + chunk
                idx = 0
            else:
                buffer += chunk
            return True

        while True:
            while True:
                while idx < len(buffer) and buffer[idx].isspace():
                    idx += 1
                if idx < len(buffer):
                    break
                if not _fill():
                    return

            if buffer[idx] == "]":
                return

            while True:
                try:
                    item, next_idx = decoder.raw_decode(buffer, idx)
                    idx = next_idx
                    break
                except json.JSONDecodeError:
                    if not _fill():
                        raise

            yield item

            while True:
                while idx < len(buffer) and buffer[idx].isspace():
                    idx += 1
                if idx < len(buffer):
                    break
                if not _fill():
                    return

            if buffer[idx] == ",":
                idx += 1
            elif buffer[idx] == "]":
                return

            if idx > 524288:
                buffer = buffer[idx:]
                idx = 0

    file_size = path.stat().st_size if path.exists() else 0
    if file_size > 64 * 1024 * 1024:
        with path.open("r", encoding="utf-8", buffering=262144) as f:
            header = f.read(65536)
            if not header.strip():
                return

            stripped = header.lstrip()
            if stripped.startswith("["):
                array_start = header.find("[")
                yield from _iter_streamed_array(f, header[array_start + 1:])
                return

            match = re.search(r'"(results|items|data)"\s*:\s*\[', header)
            while match is None and len(header) < 1048576:
                chunk = f.read(65536)
                if not chunk:
                    break
                header += chunk
                match = re.search(r'"(results|items|data)"\s*:\s*\[', header)

            if match is not None:
                yield from _iter_streamed_array(f, header[match.end():])
                return

    with path.open("r", encoding="utf-8", buffering=262144) as f:
        text = f.read().strip()
    if not text:
        return
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
        return

    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            for item in payload["items"]:
                yield item
            return
        if isinstance(payload.get("data"), list):
            for item in payload["data"]:
                yield item
            return
        if isinstance(payload.get("results"), list):
            for item in payload["results"]:
                yield item
            return
        yield payload
        return

    if isinstance(payload, list):
        for item in payload:
            yield item


def _build_training_tensors_from_roi(
    roi: np.ndarray,
    trace,
    orig_w: int,
    out_h: int,
    out_w: int,
    null_val: float,
    label_sigma_px: float,
):
    if roi is None or roi.size == 0:
        return None

    roi_gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
    roi_resized = cv2.resize(roi_gray, (out_w, out_h), interpolation=cv2.INTER_AREA)
    image_tensor = (roi_resized.astype(np.float32) / 255.0)[None, :, :]

    trace = np.asarray(trace, dtype=np.float32)
    orig_h = int(trace.shape[0])
    if orig_h <= 0:
        return None

    orig_w = max(1, int(orig_w))
    ys = np.linspace(0.0, max(0.0, orig_h - 1), out_h, dtype=np.float32)
    y0 = np.floor(ys).astype(np.int64)
    y1 = np.clip(y0 + 1, 0, orig_h - 1)
    wa = (ys - y0).astype(np.float32)
    trace_resampled = trace[y0] * (1.0 - wa) + trace[y1] * wa

    valid_rows = np.isfinite(trace_resampled) & (trace_resampled != float(null_val)) & (trace_resampled >= 0.0) & (trace_resampled <= float(orig_w - 1))
    trace_resampled = np.clip(trace_resampled, 0.0, float(orig_w - 1))

    if orig_w <= 1:
        center_px = np.zeros((out_h,), dtype=np.float32)
    else:
        center_px = (trace_resampled / float(orig_w - 1)) * float(out_w - 1)

    xs = np.arange(out_w, dtype=np.float32)[None, :]
    sigma_px = max(1.0, float(label_sigma_px))
    target = np.exp(-0.5 * ((xs - center_px[:, None]) / sigma_px) ** 2).astype(np.float32)
    target[~valid_rows] = 0.0

    row_mask = valid_rows.astype(np.float32)
    center_norm = np.zeros((out_h,), dtype=np.float32)
    if out_w > 1:
        center_norm = center_px / float(out_w - 1)

    return {
        "x": torch.from_numpy(image_tensor),
        "target": torch.from_numpy(target),
        "row_mask": torch.from_numpy(row_mask),
        "center": torch.from_numpy(center_norm.astype(np.float32)),
    }


class CurveTraceDataset(Dataset):
    def __init__(
        self,
        configs_path: Path,
        results_path: Path,
        out_h: int = 256,
        out_w: int = 128,
        curve_filter: str | None = None,
        mode_filter: str | None = None,
        label_sigma_px: float = 2.5,
    ):
        self.out_h = int(out_h)
        self.out_w = int(out_w)
        self.label_sigma_px = float(label_sigma_px)
        self.items = []
        self.supported_modes: set[str] = set()

        curve_filter_norm = str(curve_filter or "").strip().upper()
        allowed_modes = {
            _normalize_mode_name(x)
            for x in str(mode_filter or "").split(",")
            if str(x).strip()
        }

        pair_count = 0
        item_count = 0
        skipped_count = 0

        for cfg_job, res_item in zip(_iter_json_items(configs_path), _iter_json_items(results_path)):
            pair_count += 1
            if pair_count % 100 == 0:
                print(f"[dataset_loading] Processed {pair_count} config-result pairs, created {item_count} items so far")

            res = res_item
            if isinstance(res_item, dict) and "result" in res_item and "source" in res_item:
                res = res_item["result"]

            if not isinstance(res, dict) or not res.get("success"):
                skipped_count += 1
                continue

            img_data_url = cfg_job.get("image")
            img_path = cfg_job.get("image_path")
            if not img_data_url and not img_path:
                skipped_count += 1
                continue

            depth_cfg = (cfg_job.get("config") or {}).get("depth") or {}
            top_px = int(depth_cfg.get("top_px", 0))
            bottom_px = int(depth_cfg.get("bottom_px", 0))
            if bottom_px <= top_px:
                skipped_count += 1
                continue

            curves = (cfg_job.get("config") or {}).get("curves") or []
            curve_traces = res.get("curve_traces") or {}
            global_opts = (cfg_job.get("config") or {}).get("global_options") or {}
            null_val = float(global_opts.get("null", -999.25))

            for curve_cfg in curves:
                curve_name = curve_cfg.get("las_mnemonic") or curve_cfg.get("name")
                if not curve_name:
                    continue
                curve_name_norm = str(curve_name).strip().upper()
                if curve_filter_norm and curve_name_norm != curve_filter_norm:
                    continue

                mode_name = _normalize_mode_name(curve_cfg.get("mode"))
                if allowed_modes and mode_name not in allowed_modes:
                    continue

                left_px = int(curve_cfg.get("left_px", 0))
                right_px = int(curve_cfg.get("right_px", 0))
                if right_px <= left_px:
                    continue

                trace = curve_traces.get(curve_name)
                if trace is None:
                    continue

                self.items.append({
                    "image_data_url": img_data_url,
                    "image_path": img_path,
                    "top_px": top_px,
                    "bottom_px": bottom_px,
                    "left_px": left_px,
                    "right_px": right_px,
                    "trace": trace,
                    "null_val": null_val,
                    "curve_name": curve_name_norm,
                    "mode_name": mode_name,
                })
                self.supported_modes.add(mode_name)
                item_count += 1

        print(f"[dataset_loading] Complete: processed {pair_count} pairs, created {item_count} items, skipped {skipped_count}")

        if not self.items:
            raise ValueError("No training items found. Make sure configs/results files match and contain traces.")

    def __len__(self):
        return len(self.items)

    def __getitem__(self, idx: int):
        it = self.items[idx]
        top = max(0, int(it["top_px"]))
        bot = int(it["bottom_px"])
        left = max(0, int(it["left_px"]))
        right = int(it["right_px"])

        roi = None
        orig_w = None

        if it.get("image_path"):
            roi, orig_w = _load_roi_from_path(it["image_path"], top, bot, left, right)
            if roi is None:
                try:
                    img = cv2.imread(it["image_path"], cv2.IMREAD_COLOR)
                except Exception:
                    return None
                if img is None:
                    return None
                h, w = img.shape[:2]
                top = max(0, min(top, h))
                bot = max(0, min(bot, h))
                left = max(0, min(left, w))
                right = max(0, min(right, w))
                if bot <= top or right <= left:
                    return None
                roi = img[top:bot, left:right]
                orig_w = max(1, right - left)
        else:
            try:
                img = _decode_data_url_image(it["image_data_url"])
            except Exception:
                return None
            h, w = img.shape[:2]
            top = max(0, min(top, h))
            bot = max(0, min(bot, h))
            left = max(0, min(left, w))
            right = max(0, min(right, w))
            if bot <= top or right <= left:
                return None
            roi = img[top:bot, left:right]
            orig_w = max(1, right - left)

        if roi is None or roi.size == 0:
            return None

        sample = _build_training_tensors_from_roi(
            roi=roi,
            trace=it["trace"],
            orig_w=max(1, int(orig_w or (right - left))),
            out_h=self.out_h,
            out_w=self.out_w,
            null_val=float(it["null_val"]),
            label_sigma_px=self.label_sigma_px,
        )
        if sample is None:
            return None

        sample.update({
            "curve_name": it["curve_name"],
            "mode_name": it.get("mode_name", "black"),
        })
        return sample


class CurveExampleDataset(Dataset):
    def __init__(
        self,
        examples_path: Path,
        out_h: int = 256,
        out_w: int = 128,
        curve_filter: str | None = None,
        mode_filter: str | None = None,
        label_sigma_px: float = 2.5,
    ):
        self.out_h = int(out_h)
        self.out_w = int(out_w)
        self.label_sigma_px = float(label_sigma_px)
        self.items = []
        self.supported_modes: set[str] = set()

        curve_filter_norm = str(curve_filter or "").strip().upper()
        allowed_modes = {
            _normalize_mode_name(x)
            for x in str(mode_filter or "").split(",")
            if str(x).strip()
        }

        total_records = 0
        for item in _iter_json_items(examples_path):
            if not isinstance(item, dict):
                continue
            total_records += 1
            curve_name = str(item.get("curve_name") or item.get("name") or "").strip().upper()
            if curve_filter_norm and curve_name != curve_filter_norm:
                continue

            mode_name = _normalize_mode_name(item.get("mode"))
            if allowed_modes and mode_name not in allowed_modes:
                continue

            if not item.get("roi_image") or item.get("trace") is None:
                continue

            self.items.append({
                "roi_image": item.get("roi_image"),
                "trace": item.get("trace"),
                "null_val": float(item.get("null_value", -999.25)),
                "curve_name": curve_name or "UNKNOWN",
                "mode_name": mode_name,
            })
            self.supported_modes.add(mode_name)

        print(
            f"[examples_dataset] Complete: scanned {total_records} records, "
            f"using {len(self.items)} examples across modes={sorted(self.supported_modes)}"
        )

        if not self.items:
            raise ValueError("No usable exported training examples found.")

    def __len__(self):
        return len(self.items)

    def __getitem__(self, idx: int):
        it = self.items[idx]
        try:
            roi = _decode_data_url_image(it["roi_image"])
        except Exception:
            return None
        if roi is None or roi.size == 0:
            return None

        sample = _build_training_tensors_from_roi(
            roi=roi,
            trace=it["trace"],
            orig_w=roi.shape[1],
            out_h=self.out_h,
            out_w=self.out_w,
            null_val=float(it["null_val"]),
            label_sigma_px=self.label_sigma_px,
        )
        if sample is None:
            return None

        sample.update({
            "curve_name": it["curve_name"],
            "mode_name": it["mode_name"],
        })
        return sample


class SparseCorrectionDataset(Dataset):
    """Train from logged correction crops with sparse row supervision."""

    def __init__(
        self,
        corrections_dir: Path,
        out_h: int = 160,
        out_w: int = 192,
        curve_filter: str | None = None,
        mode_filter: str | None = None,
        label_sigma_px: float = 2.5,
        center_window_px: int = 160,
    ):
        self.out_h = int(out_h)
        self.out_w = int(out_w)
        self.label_sigma_px = float(label_sigma_px)
        self.center_window_px = int(max(16, center_window_px))
        self.items = []
        self.supported_modes: set[str] = set()

        curve_filter_norm = str(curve_filter or "").strip().upper()
        allowed_modes = {
            _normalize_mode_name(x)
            for x in str(mode_filter or "").split(",")
            if str(x).strip()
        }

        total_records = 0
        used_records = 0

        for jsonl_path in sorted(Path(corrections_dir).rglob("corrections.jsonl")):
            base_dir = jsonl_path.parent
            with jsonl_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    total_records += 1
                    mode_name = _normalize_mode_name(rec.get("mode"))
                    curve_name = str(rec.get("curve_id") or "").strip().upper()
                    if curve_filter_norm and curve_name != curve_filter_norm:
                        continue
                    if allowed_modes and mode_name not in allowed_modes:
                        continue

                    after = rec.get("after") or {}
                    track = rec.get("track") or {}
                    image_path_raw = rec.get("image_path")
                    if not image_path_raw:
                        continue
                    image_path = base_dir / Path(str(image_path_raw)).name
                    if not image_path.exists():
                        continue

                    try:
                        x_abs = float(after.get("x"))
                        y_abs = float(after.get("y"))
                        left_x = float(track.get("leftX"))
                        right_x = float(track.get("rightX"))
                    except Exception:
                        continue

                    if not (math.isfinite(x_abs) and math.isfinite(y_abs) and math.isfinite(left_x) and math.isfinite(right_x)):
                        continue
                    if right_x <= left_x:
                        continue

                    self.items.append({
                        "image_path": image_path,
                        "curve_name": curve_name or "UNKNOWN",
                        "mode_name": mode_name,
                        "x_abs": x_abs,
                        "y_abs": y_abs,
                        "left_x": left_x,
                        "right_x": right_x,
                    })
                    self.supported_modes.add(mode_name)
                    used_records += 1

        print(
            f"[corrections_dataset] Complete: scanned {total_records} records, "
            f"using {used_records} sparse corrections across modes={sorted(self.supported_modes)}"
        )

        if not self.items:
            raise ValueError("No usable correction records found.")

    def __len__(self):
        return len(self.items)

    def __getitem__(self, idx: int):
        it = self.items[idx]
        try:
            img = cv2.imread(str(it["image_path"]), cv2.IMREAD_COLOR)
        except Exception:
            return None
        if img is None or img.size == 0:
            return None

        crop_h, crop_w = img.shape[:2]
        if crop_h <= 0 or crop_w <= 0:
            return None

        roi_gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        roi_resized = cv2.resize(roi_gray, (self.out_w, self.out_h), interpolation=cv2.INTER_AREA)
        image_tensor = (roi_resized.astype(np.float32) / 255.0)[None, :, :]

        track_w = max(1.0, float(it["right_x"]) - float(it["left_x"]))
        local_x = float(it["x_abs"]) - float(it["left_x"])
        local_x = max(0.0, min(track_w - 1.0, local_x))
        local_x = (local_x / max(1.0, track_w - 1.0)) * float(self.out_w - 1)

        # Correction crops are centered around the edited row with a default
        # +/-80px window. Most logged corrections therefore land on the middle row.
        y_abs = float(it["y_abs"])
        if y_abs < (self.center_window_px / 2.0):
            local_y = y_abs
        else:
            local_y = min(float(crop_h - 1), self.center_window_px / 2.0)
        local_y = (max(0.0, min(float(crop_h - 1), local_y)) / max(1.0, float(crop_h - 1))) * float(self.out_h - 1)
        row_idx = int(round(local_y))
        row_idx = max(0, min(self.out_h - 1, row_idx))

        xs = np.arange(self.out_w, dtype=np.float32)[None, :]
        target = np.zeros((self.out_h, self.out_w), dtype=np.float32)
        sigma_px = max(1.0, float(self.label_sigma_px))
        target[row_idx, :] = np.exp(-0.5 * ((xs[0] - local_x) / sigma_px) ** 2).astype(np.float32)

        row_mask = np.zeros((self.out_h,), dtype=np.float32)
        row_mask[row_idx] = 1.0

        center_norm = np.zeros((self.out_h,), dtype=np.float32)
        if self.out_w > 1:
            center_norm[row_idx] = float(local_x) / float(self.out_w - 1)

        return {
            "x": torch.from_numpy(image_tensor),
            "target": torch.from_numpy(target),
            "row_mask": torch.from_numpy(row_mask),
            "center": torch.from_numpy(center_norm.astype(np.float32)),
            "curve_name": it["curve_name"],
            "mode_name": it["mode_name"],
        }


def _masked_bce_with_logits(logits: torch.Tensor, target: torch.Tensor, row_mask: torch.Tensor) -> torch.Tensor:
    weight = row_mask.unsqueeze(-1).float()
    loss = F.binary_cross_entropy_with_logits(logits, target, reduction="none")
    denom = weight.sum().clamp(min=1.0) * float(logits.shape[-1])
    return (loss * weight).sum() / denom


def _masked_soft_dice(logits: torch.Tensor, target: torch.Tensor, row_mask: torch.Tensor, eps: float = 1e-6) -> torch.Tensor:
    prob = torch.sigmoid(logits)
    weight = row_mask.unsqueeze(-1).float()
    intersection = (prob * target * weight).sum()
    union = ((prob + target) * weight).sum()
    return 1.0 - ((2.0 * intersection + eps) / (union + eps))


def _masked_centerline_loss(logits: torch.Tensor, center: torch.Tensor, row_mask: torch.Tensor) -> torch.Tensor:
    prob = torch.softmax(logits, dim=-1)
    xs = torch.linspace(0.0, 1.0, logits.shape[-1], device=logits.device).view(1, 1, -1)
    pred_center = (prob * xs).sum(dim=-1)
    denom = row_mask.sum().clamp(min=1.0)
    return (((pred_center - center) ** 2) * row_mask).sum() / denom


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--configs", default="training_data.configs.json")
    ap.add_argument("--results", default="training_data.json")
    ap.add_argument("--examples", default="")
    ap.add_argument("--corrections-dir", default="")
    ap.add_argument("--curve", default=None)
    ap.add_argument("--mode-filter", default="")
    ap.add_argument("--out", default="curve_trace_model.pt")
    ap.add_argument("--epochs", type=int, default=6)
    ap.add_argument("--batch-size", type=int, default=4)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--h", type=int, default=256)
    ap.add_argument("--w", type=int, default=128)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--base", type=int, default=16)
    ap.add_argument("--target-width-px", type=float, default=2.5)
    ap.add_argument("--mask-blur-sigma", type=float, default=0.8)
    ap.add_argument("--log-every", type=int, default=25)
    ap.add_argument("--progress-file", default="")
    ap.add_argument("--resume", action="store_true", help="Resume from latest epoch checkpoint if available")
    args = ap.parse_args()

    device = torch.device(args.device)
    progress_path = Path(args.progress_file) if args.progress_file else None

    def _write_progress(payload: dict):
        if progress_path is None:
            return
        payload = dict(payload)
        payload["updated_at"] = datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        try:
            progress_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            pass

    _write_progress({"stage": "dataset_loading"})

    if args.examples:
        ds = CurveExampleDataset(
            examples_path=Path(args.examples),
            out_h=args.h,
            out_w=args.w,
            curve_filter=args.curve,
            mode_filter=args.mode_filter,
            label_sigma_px=args.target_width_px,
        )
    elif args.corrections_dir:
        ds = SparseCorrectionDataset(
            corrections_dir=Path(args.corrections_dir),
            out_h=args.h,
            out_w=args.w,
            curve_filter=args.curve,
            mode_filter=args.mode_filter,
            label_sigma_px=args.target_width_px,
        )
    else:
        ds = CurveTraceDataset(
            configs_path=Path(args.configs),
            results_path=Path(args.results),
            out_h=args.h,
            out_w=args.w,
            curve_filter=args.curve,
            mode_filter=args.mode_filter,
            label_sigma_px=args.target_width_px,
        )

    supported_modes = []
    if hasattr(ds, "supported_modes"):
        supported_modes = sorted([m for m in ds.supported_modes if m])
    if not supported_modes:
        supported_modes = ["black"]

    def _collate(batch):
        batch = [b for b in batch if b is not None]
        if not batch:
            return None
        return default_collate(batch)

    dl = DataLoader(ds, batch_size=args.batch_size, shuffle=True, num_workers=0, collate_fn=_collate)

    model = CurveSegNet(base=args.base).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=args.lr)

    out_path = Path(args.out)
    start_epoch = 1
    if args.resume:
        ckpt_path = out_path.with_suffix(".ckpt.pt")
        if ckpt_path.exists():
            ckpt = torch.load(str(ckpt_path), map_location=device)
            model.load_state_dict(ckpt["state_dict"])
            opt.load_state_dict(ckpt["opt_state_dict"])
            start_epoch = ckpt["epoch"] + 1
            print(f"Resumed from checkpoint: epoch {ckpt['epoch']} (loss={ckpt.get('avg_loss', 0.0):.6f})")
        else:
            print("No checkpoint found, starting from scratch.")

    for epoch in range(start_epoch, args.epochs + 1):
        model.train()
        total = 0.0
        steps = 0
        total_steps = len(dl)
        _write_progress({"stage": "train_epoch_start", "epoch": epoch, "total_epochs": args.epochs})

        for batch in dl:
            if batch is None:
                continue

            x = batch["x"].to(device)
            target = batch["target"].to(device)
            row_mask = batch["row_mask"].to(device)
            center = batch["center"].to(device)

            logits = model(x)
            loss_bce = _masked_bce_with_logits(logits, target, row_mask)
            loss_dice = _masked_soft_dice(logits, target, row_mask)
            loss_center = _masked_centerline_loss(logits, center, row_mask)
            loss = 0.55 * loss_bce + 0.25 * loss_dice + 0.20 * loss_center

            opt.zero_grad(set_to_none=True)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            opt.step()

            total += float(loss.item())
            steps += 1

            if args.log_every > 0 and steps % args.log_every == 0:
                print(
                    f"epoch={epoch} step={steps}/{total_steps} "
                    f"loss={loss.item():.6f} bce={loss_bce.item():.6f} "
                    f"dice={loss_dice.item():.6f} center={loss_center.item():.6f}"
                )
                _write_progress({
                    "stage": "train_step",
                    "epoch": epoch,
                    "total_epochs": args.epochs,
                    "step": steps,
                    "total_steps": total_steps,
                    "loss": float(loss.item()),
                    "loss_bce": float(loss_bce.item()),
                    "loss_dice": float(loss_dice.item()),
                    "loss_center": float(loss_center.item()),
                })

        avg = total / max(1, steps)
        print(f"epoch={epoch} loss={avg:.6f}")
        _write_progress({
            "stage": "train_epoch_done",
            "epoch": epoch,
            "total_epochs": args.epochs,
            "avg_loss": float(avg),
            "steps": steps,
            "total_steps": total_steps,
        })

        ckpt_path = out_path.with_suffix(".ckpt.pt")
        ckpt_path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            "epoch": epoch,
            "state_dict": model.state_dict(),
            "opt_state_dict": opt.state_dict(),
            "avg_loss": float(avg),
            "input_h": int(args.h),
            "input_w": int(args.w),
            "curve": args.curve,
            "model_type": "segmentation_v2",
            "supported_modes": supported_modes,
            "target_width_px": float(args.target_width_px),
            "mask_blur_sigma": float(args.mask_blur_sigma),
        }, str(ckpt_path))
        print(f"checkpoint saved: {ckpt_path}")

    payload = {
        "state_dict": model.state_dict(),
        "input_h": int(args.h),
        "input_w": int(args.w),
        "curve": args.curve,
        "model_type": "segmentation_v2",
        "supported_modes": supported_modes,
        "target_width_px": float(args.target_width_px),
        "mask_blur_sigma": float(args.mask_blur_sigma),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(payload, str(out_path))
    print(f"saved: {out_path}")


if __name__ == "__main__":
    main()
