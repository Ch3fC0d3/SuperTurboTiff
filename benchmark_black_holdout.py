#!/usr/bin/env python3

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_tracer import AITracer
from train_curve_trace_model import _iter_json_items, _load_roi_from_path
from web_app import (
    compute_prob_map,
    postprocess_black_trace,
    trace_black_curve_ai_hybrid,
    trace_black_curve_classical,
)


def _stable_holdout_bucket(key: str) -> float:
    digest = hashlib.sha1(key.encode("utf-8", errors="replace")).hexdigest()
    return (int(digest[:8], 16) % 10000) / 100.0


def _iter_black_holdout_samples(
    configs_path: Path,
    results_path: Path,
    holdout_pct: float,
    limit: int = 0,
    window_rows: int = 0,
):
    yielded = 0
    seen_images: set[str] = set()

    for cfg_job, res_item in zip(_iter_json_items(configs_path), _iter_json_items(results_path)):
        res = res_item.get("result", res_item) if isinstance(res_item, dict) else res_item
        if not isinstance(res, dict) or not res.get("success"):
            continue

        image_path = str(cfg_job.get("image_path") or "").strip()
        image_key = image_path or str((cfg_job.get("header_metadata") or {}).get("api") or "")
        if not image_key:
            continue
        if _stable_holdout_bucket(image_key) >= float(holdout_pct):
            continue

        depth_cfg = ((cfg_job.get("config") or {}).get("depth") or {})
        top_px = int(depth_cfg.get("top_px", 0))
        bottom_px = int(depth_cfg.get("bottom_px", 0))
        if bottom_px <= top_px:
            continue

        curve_traces = res.get("curve_traces") or {}
        for curve_cfg in ((cfg_job.get("config") or {}).get("curves") or []):
            if str(curve_cfg.get("mode") or "").strip().lower() != "black":
                continue

            curve_name = curve_cfg.get("las_mnemonic") or curve_cfg.get("name")
            if not curve_name:
                continue
            trace = curve_traces.get(curve_name)
            if trace is None:
                continue

            left_px = int(curve_cfg.get("left_px", 0))
            right_px = int(curve_cfg.get("right_px", 0))
            if right_px <= left_px:
                continue

            roi, orig_w = _load_roi_from_path(image_path, top_px, bottom_px, left_px, right_px)
            if roi is None or orig_w is None:
                continue

            hot_side = curve_cfg.get("hot_side")
            left_value = float(curve_cfg.get("left_value", 0.0))
            right_value = float(curve_cfg.get("right_value", 0.0))
            if not hot_side and math.isfinite(left_value) and math.isfinite(right_value):
                hot_side = "right" if right_value >= left_value else "left"

            seen_images.add(image_key)
            trace_arr = np.asarray(trace, dtype=np.float32)
            roi_h = int(roi.shape[0])
            chunk_rows = int(window_rows) if int(window_rows) > 0 else roi_h

            for row0 in range(0, roi_h, chunk_rows):
                row1 = min(roi_h, row0 + chunk_rows)
                roi_chunk = roi[row0:row1]
                trace_chunk = trace_arr[row0:row1]
                if roi_chunk is None or roi_chunk.size == 0 or trace_chunk.size == 0:
                    continue

                yielded += 1
                yield {
                    "image_key": image_key,
                    "curve_name": str(curve_name),
                    "curve_type": str(curve_cfg.get("type") or curve_name),
                    "left_value": left_value,
                    "right_value": right_value,
                    "hot_side": hot_side,
                    "trace": trace_chunk,
                    "roi": roi_chunk,
                    "orig_w": int(orig_w),
                    "unique_image_count": len(seen_images),
                    "row_window": [int(row0), int(row1)],
                }
                if limit > 0 and yielded >= limit:
                    return


@dataclass
class MetricsAccumulator:
    method: str
    sample_count: int = 0
    target_rows: int = 0
    overlap_rows: int = 0
    predicted_rows_on_target: int = 0
    error_sum: float = 0.0
    hit1: int = 0
    hit3: int = 0
    hit5: int = 0
    conf_sum: float = 0.0
    conf_count: int = 0

    def update(self, pred_x, pred_conf, target, orig_w: int):
        pred_x = np.asarray(pred_x, dtype=np.float32).reshape(-1)
        pred_conf = np.asarray(pred_conf, dtype=np.float32).reshape(-1)
        target = np.asarray(target, dtype=np.float32).reshape(-1)

        n = min(pred_x.size, pred_conf.size if pred_conf.size else pred_x.size, target.size)
        if n <= 0:
            return

        pred_x = pred_x[:n]
        if pred_conf.size:
            pred_conf = pred_conf[:n]
        else:
            pred_conf = np.zeros(n, dtype=np.float32)
        target = target[:n]

        valid_target = np.isfinite(target) & (target != -999.25) & (target >= 0.0) & (target <= float(max(0, orig_w - 1)))
        if not np.any(valid_target):
            return

        valid_pred = np.isfinite(pred_x)
        valid = valid_target & valid_pred

        self.sample_count += 1
        self.target_rows += int(np.sum(valid_target))
        self.predicted_rows_on_target += int(np.sum(valid_pred[valid_target]))

        if np.any(valid):
            err = np.abs(pred_x[valid] - target[valid])
            self.overlap_rows += int(err.size)
            self.error_sum += float(np.sum(err))
            self.hit1 += int(np.sum(err <= 1.0))
            self.hit3 += int(np.sum(err <= 3.0))
            self.hit5 += int(np.sum(err <= 5.0))
            conf = np.clip(pred_conf[valid], 0.0, 1.0)
            self.conf_sum += float(np.sum(conf))
            self.conf_count += int(conf.size)

    def summary(self):
        return {
            "method": self.method,
            "samples": self.sample_count,
            "target_rows": self.target_rows,
            "coverage": (float(self.predicted_rows_on_target) / float(self.target_rows)) if self.target_rows else float("nan"),
            "mae_px": (float(self.error_sum) / float(self.overlap_rows)) if self.overlap_rows else float("nan"),
            "hit1": (float(self.hit1) / float(self.overlap_rows)) if self.overlap_rows else float("nan"),
            "hit3": (float(self.hit3) / float(self.overlap_rows)) if self.overlap_rows else float("nan"),
            "hit5": (float(self.hit5) / float(self.overlap_rows)) if self.overlap_rows else float("nan"),
            "mean_conf": (float(self.conf_sum) / float(self.conf_count)) if self.conf_count else float("nan"),
        }


def _format_metric(value: float) -> str:
    if not math.isfinite(value):
        return "nan"
    return f"{value:.4f}"


def _evaluate_methods(args):
    dense_model_path = Path(args.dense_model)
    legacy_model_path = Path(args.legacy_model)

    methods: list[tuple[str, object]] = [("classical", None)]
    if legacy_model_path.exists():
        methods.append(("legacy_model", AITracer(str(legacy_model_path))))
    if dense_model_path.exists():
        dense_tracer = AITracer(str(dense_model_path))
        methods.append(("dense_model", dense_tracer))
        methods.append(("hybrid_dense", dense_tracer))

    metrics = {name: MetricsAccumulator(name) for name, _ in methods}
    processed = 0
    unique_images = 0

    for sample in _iter_black_holdout_samples(
        configs_path=Path(args.configs),
        results_path=Path(args.results),
        holdout_pct=args.holdout_pct,
        limit=args.limit,
        window_rows=args.window_rows,
    ):
        processed += 1
        unique_images = max(unique_images, int(sample.get("unique_image_count", 0)))

        roi = sample["roi"]
        mask = compute_prob_map(roi, mode="black", ui_filters={})
        common_kwargs = {
            "scale_min": sample["left_value"],
            "scale_max": sample["right_value"],
            "curve_type": sample["curve_type"],
            "max_step": 30 if str(sample["curve_type"]).upper() == "GR" else 50,
            "smooth_lambda": 0.001 if str(sample["curve_type"]).upper() == "GR" else 0.02,
            "curv_lambda": 0.001 if str(sample["curve_type"]).upper() == "GR" else 0.005,
            "hot_side": sample["hot_side"],
            "curve_smooth_window": args.smooth_window,
            "outlier_threshold": 3.0,
        }

        for method_name, tracer in methods:
            if method_name == "classical":
                pred_x, pred_conf = trace_black_curve_classical(mask=mask, **common_kwargs)
                pred_x = postprocess_black_trace(
                    mask,
                    pred_x,
                    confidence=pred_conf,
                    curve_type=sample["curve_type"],
                    curve_smooth_window=args.smooth_window,
                    min_run=args.min_run,
                )
            elif method_name == "hybrid_dense":
                pred_x, pred_conf, _debug = trace_black_curve_ai_hybrid(
                    roi=roi,
                    mask=mask,
                    tracer=tracer,
                    **common_kwargs,
                )
                pred_x = postprocess_black_trace(
                    mask,
                    pred_x,
                    confidence=pred_conf,
                    curve_type=sample["curve_type"],
                    curve_smooth_window=args.smooth_window,
                    min_run=args.min_run,
                )
            else:
                pred_x, pred_conf = tracer.trace_with_confidence(roi)

            metrics[method_name].update(
                pred_x=pred_x,
                pred_conf=pred_conf,
                target=sample["trace"],
                orig_w=sample["orig_w"],
            )

        if args.progress_every > 0 and processed % args.progress_every == 0:
            print(f"[progress] processed {processed} black holdout samples across {unique_images} images")

    return {
        "processed_samples": processed,
        "unique_images": unique_images,
        "holdout_pct": float(args.holdout_pct),
        "methods": [metrics[name].summary() for name, _ in methods],
    }


def main():
    ap = argparse.ArgumentParser(description="Benchmark black-curve tracing on a holdout split from the dense TestTiflas archive.")
    ap.add_argument("--configs", default=r"D:\Users\gabep\Desktop\TestTiflas\temp_train_configs.json")
    ap.add_argument("--results", default=r"D:\Users\gabep\Desktop\TestTiflas\temp_train_results.json")
    ap.add_argument("--dense-model", default=str(ROOT / "models" / "testtiflas_black_seg_v2.pt"))
    ap.add_argument("--legacy-model", default=str(ROOT / "curve_trace_model.pt"))
    ap.add_argument("--holdout-pct", type=float, default=20.0)
    ap.add_argument("--limit", type=int, default=64, help="0 means evaluate the full holdout split.")
    ap.add_argument("--window-rows", type=int, default=2500, help="Split tall black traces into smaller evaluation windows.")
    ap.add_argument("--smooth-window", type=int, default=5)
    ap.add_argument("--min-run", type=int, default=2)
    ap.add_argument("--progress-every", type=int, default=10)
    ap.add_argument("--json-out", default="")
    args = ap.parse_args()

    result = _evaluate_methods(args)
    print(
        f"Holdout benchmark: samples={result['processed_samples']} "
        f"images={result['unique_images']} holdout_pct={result['holdout_pct']:.1f}"
    )
    for method in result["methods"]:
        print(
            f"{method['method']:>12}  "
            f"samples={method['samples']:>4}  "
            f"coverage={_format_metric(method['coverage'])}  "
            f"mae_px={_format_metric(method['mae_px'])}  "
            f"hit1={_format_metric(method['hit1'])}  "
            f"hit3={_format_metric(method['hit3'])}  "
            f"hit5={_format_metric(method['hit5'])}  "
            f"mean_conf={_format_metric(method['mean_conf'])}"
        )

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"saved: {out_path}")


if __name__ == "__main__":
    main()
