#!/usr/bin/env python3

import argparse
import base64
import datetime
import json
import math
import re
from pathlib import Path

import cv2
import lasio
import numpy as np

from web_app import compute_prob_map, postprocess_black_trace, trace_black_curve_classical


NULL_VALUE = -999.25
DEFAULT_MODE = "black"

CURVE_ALIASES = {
    "GR": ["GR", "CGR", "SGR", "GAMMA", "GAM", "NGR"],
    "CAL": ["CAL", "CALI", "CALIPER", "HCAL"],
    "BD": ["BD", "RHOB", "DEN", "DENS", "ZDEN"],
    "DPHI": ["DPHI", "NPHI", "TNPH", "NPHI_LS", "PHI"],
    "RILD": ["RILD", "ILD", "LLD", "RT", "RES", "COND"],
    "TEMP": ["TEMP", "TMP", "TEMPERATURE"],
}


def _normalize_curve_name(curve_name: str) -> str:
    return str(curve_name or "").strip().upper()


def _resolve_las_curve(las, curve_name: str) -> str | None:
    target = _normalize_curve_name(curve_name)
    if not target:
        return None

    available = {_normalize_curve_name(c.mnemonic): c.mnemonic for c in las.curves}
    aliases = CURVE_ALIASES.get(target, [target])
    for alias in aliases:
        alias_norm = _normalize_curve_name(alias)
        if alias_norm in available:
            return available[alias_norm]
    return None


def _curve_hints_from_filename(stem: str) -> set[str] | None:
    name = stem.lower()
    if "dgc" in name:
        return {"BD", "GR", "CAL"}
    if "ngc" in name:
        return {"DPHI", "GR", "CAL"}
    if "ilz" in name or re.search(r"(?:^|[^a-z])i(?:[^a-z]|$)", name):
        return {"RILD"}
    if re.search(r"(?:^|[^a-z])t(?:[^a-z]|$)", name):
        return {"TEMP"}
    return None


def _panel_count_from_filename(stem: str) -> int:
    name = stem.lower()
    if "dgc" in name or "ngc" in name:
        return 3
    return 1


def _iter_pair_dirs(root: Path, allowed_sources: set[str], pair_id_filter: set[str]):
    for source_dir in sorted(root.iterdir()):
        if not source_dir.is_dir():
            continue
        source_name = source_dir.name.lower()
        if allowed_sources and source_name not in allowed_sources:
            continue
        pairs_dir = source_dir / "pairs"
        if not pairs_dir.exists():
            continue
        for pair_dir in sorted(pairs_dir.iterdir()):
            if not pair_dir.is_dir():
                continue
            if pair_id_filter and pair_dir.name not in pair_id_filter:
                continue
            if pair_dir.is_dir():
                yield source_name, pair_dir


def _find_content_bounds(gray: np.ndarray) -> tuple[int, int]:
    row_dark = (gray < 210).mean(axis=1)
    if row_dark.size == 0:
        return 0, gray.shape[0]
    threshold = max(0.03, float(np.max(row_dark)) * 0.20)
    idx = np.where(row_dark > threshold)[0]
    if idx.size == 0:
        return 0, gray.shape[0]
    top = max(0, int(idx[0]) - 8)
    bottom = min(gray.shape[0], int(idx[-1]) + 9)
    return top, bottom


def _fallback_black_mask(roi_bgr: np.ndarray) -> np.ndarray:
    gray = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, mask = cv2.threshold(blur, 205, 255, cv2.THRESH_BINARY_INV)
    kernel = np.ones((3, 3), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)
    return mask


def _trace_black_roi(roi_bgr: np.ndarray, curve_type: str) -> tuple[np.ndarray | None, np.ndarray | None]:
    try:
        mask = compute_prob_map(roi_bgr, DEFAULT_MODE)
    except Exception:
        mask = _fallback_black_mask(roi_bgr)

    try:
        xs, conf = trace_black_curve_classical(
            mask,
            scale_min=0.0,
            scale_max=150.0,
            curve_type=curve_type,
            max_step=30,
            smooth_lambda=0.001,
            curv_lambda=0.001,
            hot_side="right",
        )
    except Exception:
        return None, None

    try:
        xs = postprocess_black_trace(mask, xs, confidence=conf, curve_type=curve_type)
    except Exception:
        pass
    return xs, conf


def _score_trace_against_curve(xs: np.ndarray, values: np.ndarray, width_px: int) -> float:
    xs = np.asarray(xs, dtype=np.float32)
    values = np.asarray(values, dtype=np.float32)
    valid = np.isfinite(xs) & np.isfinite(values)
    if int(np.sum(valid)) < 100:
        return float("-inf")

    x_valid = xs[valid]
    v_valid = values[valid]
    x2, x98 = np.nanpercentile(x_valid, [2, 98])
    if not np.isfinite(x2) or not np.isfinite(x98):
        return float("-inf")
    x_span = float(x98 - x2)
    if x_span < max(8.0, 0.05 * float(width_px)):
        return float("-inf")
    x_norm = np.clip((x_valid - x2) / max(1e-6, x_span), 0.0, 1.0)
    if float(np.std(x_norm)) < 1e-4:
        return float("-inf")

    best_corr = float("-inf")
    for sign in (1.0, -1.0):
        vv = sign * v_valid
        p2, p98 = np.nanpercentile(vv, [2, 98])
        if not np.isfinite(p2) or not np.isfinite(p98) or abs(float(p98 - p2)) < 1e-6:
            continue
        v_norm = np.clip((vv - p2) / max(1e-6, float(p98 - p2)), 0.0, 1.0)
        if float(np.std(v_norm)) < 1e-4:
            continue
        corr = float(np.corrcoef(x_norm, v_norm)[0, 1])
        if math.isfinite(corr) and corr > best_corr:
            best_corr = corr

    if not math.isfinite(best_corr):
        return float("-inf")

    valid_frac = float(np.mean(np.isfinite(xs)))
    range_frac = x_span / max(1.0, float(width_px - 1))
    return best_corr * max(0.25, valid_frac) * max(0.25, min(1.0, range_frac / 0.25))


def _resample_curve_window(depth: np.ndarray, values: np.ndarray, out_len: int, start_depth: float, end_depth: float) -> np.ndarray:
    mask = np.isfinite(depth) & np.isfinite(values)
    if int(np.sum(mask)) < 10:
        return np.full(out_len, np.nan, dtype=np.float32)
    ys = np.linspace(start_depth, end_depth, out_len, dtype=np.float32)
    return np.interp(ys, depth[mask], values[mask]).astype(np.float32)


def _encode_png_data_url(image_bgr: np.ndarray) -> str:
    ok, buf = cv2.imencode(".png", image_bgr)
    if not ok:
        raise ValueError("Failed to encode ROI")
    return "data:image/png;base64," + base64.b64encode(buf.tobytes()).decode("ascii")


def _select_panel_bounds(content_w: int, panel_count: int) -> list[tuple[int, int]]:
    if panel_count <= 1:
        return [(0, content_w)]
    panels = []
    for idx in range(panel_count):
        left = int(round(idx * content_w / panel_count))
        right = int(round((idx + 1) * content_w / panel_count))
        if right - left >= 32:
            panels.append((left, right))
    return panels or [(0, content_w)]


def _compute_window_starts(total_h: int, window_h: int, stride: int, max_windows: int) -> list[int]:
    if total_h <= window_h:
        return [0]
    starts = list(range(0, max(1, total_h - window_h + 1), max(1, stride)))
    if not starts or starts[-1] != (total_h - window_h):
        starts.append(total_h - window_h)
    starts = sorted(set(int(s) for s in starts))
    if len(starts) <= max_windows:
        return starts
    picks = np.linspace(0, len(starts) - 1, max_windows, dtype=int)
    return [starts[i] for i in sorted(set(int(x) for x in picks))]


def _export_pair_dir(
    source_name: str,
    pair_dir: Path,
    out_dir: Path,
    target_curve: str,
    score_height: int,
    window_height: int,
    window_stride: int,
    max_windows_per_panel: int,
    min_panel_score: float,
    min_window_score: float,
    min_valid_fraction: float,
    inline_images: bool,
    allow_full_scans: bool,
    max_scans_per_pair: int,
) -> dict:
    las_files = sorted(pair_dir.glob("*.las"))
    tif_files = sorted(list(pair_dir.glob("*.tif")) + list(pair_dir.glob("*.tiff")))
    if not las_files or not tif_files:
        return {"status": "skipped", "reason": "missing_las_or_tif"}

    las_path = las_files[0]
    try:
        las = lasio.read(str(las_path))
    except Exception as exc:
        return {"status": "skipped", "reason": f"las_read_failed:{exc.__class__.__name__}"}

    target_mnemonic = _resolve_las_curve(las, target_curve)
    if not target_mnemonic:
        return {"status": "skipped", "reason": f"curve_not_found:{target_curve}"}

    depth = np.asarray(las.index, dtype=np.float32)
    target_values_full = np.asarray(las[target_mnemonic], dtype=np.float32)
    if depth.size < 50 or target_values_full.size != depth.size:
        return {"status": "skipped", "reason": "invalid_las_curve"}

    examples = []
    skipped_scans = []
    pair_image_dir = out_dir / "images" / source_name / pair_dir.name
    pair_image_dir.mkdir(parents=True, exist_ok=True)

    for tif_idx, tif_path in enumerate(tif_files):
        if max_scans_per_pair and tif_idx >= max_scans_per_pair:
            break
        hints = _curve_hints_from_filename(tif_path.stem)
        if hints is not None and _normalize_curve_name(target_curve) not in hints:
            skipped_scans.append({"scan": tif_path.name, "reason": "curve_hint_excludes_target"})
            continue

        img = cv2.imread(str(tif_path), cv2.IMREAD_COLOR)
        if img is None or img.size == 0:
            skipped_scans.append({"scan": tif_path.name, "reason": "image_read_failed"})
            continue

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        top, bottom = _find_content_bounds(gray)
        if bottom - top < max(256, window_height // 2):
            skipped_scans.append({"scan": tif_path.name, "reason": "content_too_small"})
            continue

        content = img[top:bottom]
        content_h, content_w = content.shape[:2]
        panel_count = _panel_count_from_filename(tif_path.stem)

        if panel_count == 1 and (content_w > 2600) and not allow_full_scans:
            skipped_scans.append({"scan": tif_path.name, "reason": "full_scan_requires_allow_full_scans"})
            continue

        panel_bounds = _select_panel_bounds(content_w, panel_count)
        if not panel_bounds:
            skipped_scans.append({"scan": tif_path.name, "reason": "no_panels"})
            continue

        best_panel = None
        best_score = float("-inf")

        for panel_idx, (left_px, right_px) in enumerate(panel_bounds):
            panel = content[:, left_px:right_px]
            if panel.size == 0 or panel.shape[1] < 32:
                continue
            score_w = min(panel.shape[1], 900)
            resized = cv2.resize(panel, (score_w, score_height), interpolation=cv2.INTER_AREA)
            xs_small, _ = _trace_black_roi(resized, target_curve)
            if xs_small is None:
                continue
            valid_frac = float(np.mean(np.isfinite(xs_small)))
            if valid_frac < min_valid_fraction:
                continue
            curve_window = _resample_curve_window(depth, target_values_full, resized.shape[0], float(depth[0]), float(depth[-1]))
            panel_score = _score_trace_against_curve(xs_small, curve_window, resized.shape[1])
            if panel_score > best_score:
                best_score = panel_score
                best_panel = {
                    "panel_idx": panel_idx,
                    "left_px": left_px,
                    "right_px": right_px,
                    "valid_frac": valid_frac,
                }

        if best_panel is None or best_score < min_panel_score:
            skipped_scans.append({
                "scan": tif_path.name,
                "reason": "no_panel_match",
                "best_score": None if not math.isfinite(best_score) else float(best_score),
            })
            continue

        panel = content[:, best_panel["left_px"]:best_panel["right_px"]]
        starts = _compute_window_starts(panel.shape[0], window_height, window_stride, max_windows_per_panel)

        for start_row in starts:
            end_row = min(panel.shape[0], start_row + window_height)
            roi = panel[start_row:end_row]
            if roi.shape[0] < 128 or roi.shape[1] < 32:
                continue

            xs, conf = _trace_black_roi(roi, target_curve)
            if xs is None:
                continue
            valid_frac = float(np.mean(np.isfinite(xs)))
            if valid_frac < min_valid_fraction:
                continue

            start_depth = float(depth[0] + (start_row / max(1, panel.shape[0] - 1)) * (depth[-1] - depth[0]))
            end_depth = float(depth[0] + ((end_row - 1) / max(1, panel.shape[0] - 1)) * (depth[-1] - depth[0]))
            curve_window = _resample_curve_window(depth, target_values_full, roi.shape[0], start_depth, end_depth)
            window_score = _score_trace_against_curve(xs, curve_window, roi.shape[1])
            if window_score < min_window_score:
                continue

            image_id = f"{pair_dir.name}_{tif_path.stem}_panel{best_panel['panel_idx']}_r{start_row:05d}"
            roi_path = pair_image_dir / f"{image_id}.png"
            cv2.imwrite(str(roi_path), roi)

            record = {
                "schema": "log_pair_bootstrap_v1",
                "source": source_name,
                "pair_id": pair_dir.name,
                "scan_name": tif_path.name,
                "las_name": las_path.name,
                "curve_name": _normalize_curve_name(target_curve),
                "curve_mnemonic": target_mnemonic,
                "mode": DEFAULT_MODE,
                "roi_image_path": str(roi_path),
                "roi_image": _encode_png_data_url(roi) if inline_images else None,
                "trace": [
                    float(v) if np.isfinite(v) else float(NULL_VALUE)
                    for v in np.asarray(xs, dtype=np.float32).tolist()
                ],
                "null_value": float(NULL_VALUE),
                "panel_index": int(best_panel["panel_idx"]),
                "panel_left_px": int(best_panel["left_px"]),
                "panel_right_px": int(best_panel["right_px"]),
                "panel_match_score": float(best_score),
                "window_match_score": float(window_score),
                "trace_valid_fraction": float(valid_frac),
                "depth_start": start_depth,
                "depth_end": end_depth,
                "saved_at_utc": datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            }
            examples.append(record)

    return {
        "status": "ok" if examples else "skipped",
        "pair_dir": str(pair_dir),
        "examples": examples,
        "skipped_scans": skipped_scans,
        "reason": None if examples else "no_examples",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=r"D:\Users\gabep\Desktop\TestTiflas\log_pairs_out")
    ap.add_argument("--out-dir", default="pair_training_examples")
    ap.add_argument("--curve", default="GR")
    ap.add_argument("--sources", default="wvgs")
    ap.add_argument("--max-pairs", type=int, default=0)
    ap.add_argument("--pair-id", default="")
    ap.add_argument("--max-scans-per-pair", type=int, default=0)
    ap.add_argument("--score-height", type=int, default=1800)
    ap.add_argument("--window-height", type=int, default=1400)
    ap.add_argument("--window-stride", type=int, default=1000)
    ap.add_argument("--max-windows-per-panel", type=int, default=18)
    ap.add_argument("--min-panel-score", type=float, default=0.10)
    ap.add_argument("--min-window-score", type=float, default=0.08)
    ap.add_argument("--min-valid-fraction", type=float, default=0.70)
    ap.add_argument("--inline-images", action="store_true")
    ap.add_argument("--allow-full-scans", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    allowed_sources = {s.strip().lower() for s in str(args.sources).split(",") if s.strip()}
    pair_id_filter = {s.strip() for s in str(args.pair_id).split(",") if s.strip()}
    records_path = out_dir / "examples.jsonl"
    summary_path = out_dir / "summary.json"

    pair_count = 0
    example_count = 0
    processed = []

    with records_path.open("w", encoding="utf-8") as out_fh:
        for source_name, pair_dir in _iter_pair_dirs(root, allowed_sources, pair_id_filter):
            if args.max_pairs and pair_count >= args.max_pairs:
                break
            pair_count += 1
            result = _export_pair_dir(
                source_name=source_name,
                pair_dir=pair_dir,
                out_dir=out_dir,
                target_curve=args.curve,
                score_height=max(256, int(args.score_height)),
                window_height=max(256, int(args.window_height)),
                window_stride=max(64, int(args.window_stride)),
                max_windows_per_panel=max(1, int(args.max_windows_per_panel)),
                min_panel_score=float(args.min_panel_score),
                min_window_score=float(args.min_window_score),
                min_valid_fraction=float(args.min_valid_fraction),
                inline_images=bool(args.inline_images),
                allow_full_scans=bool(args.allow_full_scans),
                max_scans_per_pair=max(0, int(args.max_scans_per_pair)),
            )
            processed.append({
                "pair_dir": result.get("pair_dir"),
                "status": result.get("status"),
                "reason": result.get("reason"),
                "example_count": len(result.get("examples") or []),
                "skipped_scans": result.get("skipped_scans") or [],
            })
            for rec in result.get("examples") or []:
                out_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                example_count += 1
            print(
                f"[pair_export] {pair_dir.name}: status={result.get('status')} "
                f"examples={len(result.get('examples') or [])}"
            )

    summary = {
        "root": str(root),
        "out_dir": str(out_dir),
        "curve": _normalize_curve_name(args.curve),
        "sources": sorted(allowed_sources),
        "pair_count": pair_count,
        "example_count": example_count,
        "processed": processed,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"saved: {records_path}")
    print(f"summary: {summary_path}")
    print(f"pairs={pair_count} examples={example_count}")


if __name__ == "__main__":
    main()
