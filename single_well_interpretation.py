from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


INTERPRETATION_SCHEMA = "single_well_interpretation_v1"
INTERPRETATION_ALGORITHM = "gr_only_rules_v1"
INTERVAL_CLASSES = [
    "shale-rich",
    "cleaner clastic candidate",
    "carbonate candidate",
    "possible gas effect",
    "uncertain",
]

LABEL_COLORS = {
    "shale-rich": "#6b7280",
    "cleaner clastic candidate": "#f59e0b",
    "carbonate candidate": "#60a5fa",
    "possible gas effect": "#f472b6",
    "uncertain": "#c084fc",
}

NULL_MARKERS = {-999.25, -999.0, -9999.0, 999.25}


@dataclass
class CurveSpec:
    key: str
    curve_type: Optional[str]
    unit: str
    values: np.ndarray


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _sanitize_curve_values(values, null_value: float = -999.25) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float32).copy()
    for marker in NULL_MARKERS | {float(null_value)}:
        arr[arr == float(marker)] = np.nan
    return arr


def _infer_curve_type(key: str) -> Optional[str]:
    text = (key or "").upper()
    if "GR" in text or "GAMMA" in text:
        return "GR"
    if "RHOB" in text or "DENS" in text:
        return "RHOB"
    if "NPHI" in text or "NEUT" in text:
        return "NPHI"
    if "RES" in text or "ILD" in text or "LLD" in text:
        return "RES"
    if "DT" in text or "DTC" in text or "SONIC" in text:
        return "DT"
    return None


def _build_curve_specs(
    digitized_curves: Dict[str, Dict],
    config_curves: Optional[List[Dict]] = None,
    null_value: float = -999.25,
) -> Dict[str, CurveSpec]:
    specs: Dict[str, CurveSpec] = {}
    config_by_key: Dict[str, Dict] = {}

    for cfg in config_curves or []:
        key = str(cfg.get("las_mnemonic") or cfg.get("name") or "").strip()
        if key:
            config_by_key[key.upper()] = cfg

    for raw_key, entry in (digitized_curves or {}).items():
        key = str(raw_key or "").strip()
        if not key or not isinstance(entry, dict):
            continue
        cfg = config_by_key.get(key.upper()) or {}
        curve_type = str(cfg.get("type") or "").strip().upper() or _infer_curve_type(key)
        specs[key.upper()] = CurveSpec(
            key=key,
            curve_type=curve_type or None,
            unit=str(entry.get("unit") or cfg.get("las_unit") or "").strip(),
            values=_sanitize_curve_values(entry.get("values") or [], null_value=null_value),
        )
    return specs


def _pick_curve(specs: Dict[str, CurveSpec], desired_type: str) -> Optional[CurveSpec]:
    desired = str(desired_type or "").strip().upper()
    for spec in specs.values():
        if spec.curve_type == desired:
            return spec
    if desired == "GR":
        for spec in specs.values():
            if _infer_curve_type(spec.key) == "GR":
                return spec
    return None


def _median_step(depth: np.ndarray) -> float:
    if depth.size < 2:
        return 1.0
    diffs = np.diff(depth.astype(np.float64))
    diffs = diffs[np.isfinite(diffs)]
    if diffs.size == 0:
        return 1.0
    step = float(np.nanmedian(np.abs(diffs)))
    return step if step > 0 else 1.0


def _smooth_series(values: np.ndarray, window: int) -> np.ndarray:
    s = pd.Series(values.astype(np.float64))
    s = s.interpolate(limit_direction="both")
    if window > 1:
        s = s.rolling(window=window, center=True, min_periods=1).median()
        s = s.rolling(window=max(3, window // 2), center=True, min_periods=1).mean()
    return s.to_numpy(dtype=np.float32)


def _normalize(arr: np.ndarray) -> np.ndarray:
    arr = np.asarray(arr, dtype=np.float32)
    if arr.size == 0:
        return arr
    lo = float(np.nanmin(arr))
    hi = float(np.nanmax(arr))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return np.zeros_like(arr, dtype=np.float32)
    return (arr - lo) / (hi - lo)


def _segment_gr_curve(depth: np.ndarray, gr_values: np.ndarray) -> Tuple[List[Tuple[int, int]], Dict[str, float]]:
    step = _median_step(depth)
    min_interval_depth = 8.0 if step <= 2.0 else 2.5
    min_samples = max(8, int(round(min_interval_depth / max(step, 1e-6))))
    min_samples = min(min_samples, max(8, depth.size // 3)) if depth.size else min_samples

    smoothed = _smooth_series(gr_values, window=max(5, min(15, min_samples // 2 * 2 + 1)))
    grad = np.abs(np.gradient(smoothed))

    left_mean = pd.Series(smoothed).rolling(window=min_samples, min_periods=1).mean().to_numpy(dtype=np.float32)
    right_mean = pd.Series(smoothed[::-1]).rolling(window=min_samples, min_periods=1).mean().to_numpy(dtype=np.float32)[::-1]
    context_delta = np.abs(right_mean - left_mean)

    grad_norm = _normalize(grad)
    delta_norm = _normalize(context_delta)
    boundary_score = 0.6 * grad_norm + 0.4 * delta_norm

    if boundary_score.size < 3:
        return [(0, int(depth.size))], {
            "min_samples": float(min_samples),
            "boundary_threshold": 1.0,
            "candidate_count": 0.0,
        }

    threshold = float(max(0.34, np.nanquantile(boundary_score, 0.78)))
    local_maxima: List[int] = []
    for idx in range(1, boundary_score.size - 1):
        score = float(boundary_score[idx])
        if score < threshold:
            continue
        if score >= float(boundary_score[idx - 1]) and score >= float(boundary_score[idx + 1]):
            local_maxima.append(idx)

    accepted = [0]
    for idx in local_maxima:
        if idx - accepted[-1] >= min_samples:
            accepted.append(idx)
    if accepted[-1] != boundary_score.size - 1 and (boundary_score.size - 1) - accepted[-1] < min_samples:
        accepted[-1] = boundary_score.size - 1
    else:
        accepted.append(boundary_score.size - 1)

    intervals: List[Tuple[int, int]] = []
    for start_idx, end_idx in zip(accepted[:-1], accepted[1:]):
        start = int(start_idx)
        end = int(end_idx + 1)
        if end - start < 4:
            continue
        if intervals and start - intervals[-1][0] < min_samples // 2:
            prev_start, _prev_end = intervals[-1]
            intervals[-1] = (prev_start, end)
        else:
            intervals.append((start, end))

    if not intervals:
        intervals = [(0, int(depth.size))]

    return intervals, {
        "min_samples": float(min_samples),
        "boundary_threshold": threshold,
        "candidate_count": float(len(local_maxima)),
    }


def _interval_feature_summary(values: np.ndarray) -> Dict[str, Optional[float]]:
    valid = values[np.isfinite(values)]
    total = int(values.size)
    valid_count = int(valid.size)
    pct_missing = float(100.0 * (1.0 - valid_count / total)) if total else 100.0
    if valid.size == 0:
        return {
            "mean": None,
            "std": None,
            "min": None,
            "max": None,
            "pct_missing": pct_missing,
        }
    return {
        "mean": float(np.nanmean(valid)),
        "std": float(np.nanstd(valid)),
        "min": float(np.nanmin(valid)),
        "max": float(np.nanmax(valid)),
        "pct_missing": pct_missing,
    }


def _reason(code: str, label: str, detail: str) -> Dict[str, str]:
    return {"code": code, "label": label, "detail": detail}


def _classify_interval(
    idx: int,
    interval_slice: slice,
    depth: np.ndarray,
    gr: np.ndarray,
    global_stats: Dict[str, float],
    support_global_stats: Dict[str, Dict[str, float]],
    support_curves: Dict[str, Optional[np.ndarray]],
    interval_features: Dict[str, Dict[str, Optional[float]]],
    neighbor_gr_means: Tuple[Optional[float], Optional[float]],
) -> Tuple[str, float, List[Dict[str, str]]]:
    features = interval_features["GR"]
    mean_gr = features["mean"]
    pct_missing = 100.0 if features.get("pct_missing") is None else float(features.get("pct_missing"))
    if mean_gr is None:
        return "uncertain", 0.12, [
            _reason("missing_gr", "low confidence: missing GR", "The gamma ray interval has no usable samples."),
        ]

    span = max(global_stats["p90"] - global_stats["p10"], 1.0)
    high_gr_score = np.clip((mean_gr - global_stats["p60"]) / span, 0.0, 1.0)
    low_gr_score = np.clip((global_stats["p40"] - mean_gr) / span, 0.0, 1.0)

    left_neighbor, right_neighbor = neighbor_gr_means
    neighbor_values = [v for v in (left_neighbor, right_neighbor) if v is not None]
    neighbor_mean = float(np.mean(neighbor_values)) if neighbor_values else None

    reasons: List[Dict[str, str]] = []
    confidence = 0.52
    label = "uncertain"

    rhob = support_curves.get("RHOB")
    nphi = support_curves.get("NPHI")
    rhob_features = interval_features.get("RHOB") or {}
    nphi_features = interval_features.get("NPHI") or {}
    rhob_mean = rhob_features.get("mean")
    nphi_mean = nphi_features.get("mean")

    separation = None
    if rhob_mean is not None and nphi_mean is not None:
        rhob_stats = support_global_stats.get("RHOB") or {}
        nphi_stats = support_global_stats.get("NPHI") or {}
        rhob_span = max(float(rhob_stats.get("p90", 1.0)) - float(rhob_stats.get("p10", 0.0)), 1e-6)
        nphi_span = max(float(nphi_stats.get("p90", 1.0)) - float(nphi_stats.get("p10", 0.0)), 1e-6)
        rhob_pos = (float(rhob_mean) - float(rhob_stats.get("p10", 0.0))) / rhob_span
        nphi_pos = (float(nphi_mean) - float(nphi_stats.get("p10", 0.0))) / nphi_span
        separation = abs(rhob_pos - nphi_pos)

    if pct_missing > 45.0:
        label = "uncertain"
        confidence = 0.18
        reasons.append(
            _reason(
                "gr_missing",
                "low confidence: missing GR",
                "Nearly half of the interval is missing gamma-ray values, so the interval is kept uncertain.",
            )
        )
    elif high_gr_score >= 0.28:
        label = "shale-rich"
        confidence = 0.62 + 0.22 * float(high_gr_score)
        reasons.append(
            _reason(
                "high_gr",
                "high GR",
                "Average gamma ray is in the upper part of the well's GR range, which points toward a shale-rich interval.",
            )
        )
    else:
        low_vs_neighbors = False
        if neighbor_mean is not None and mean_gr < (neighbor_mean - 0.08 * span):
            low_vs_neighbors = True
            reasons.append(
                _reason(
                    "low_gr_neighbors",
                    "low GR vs neighbors",
                    "This interval's average gamma ray is noticeably lower than the adjacent intervals.",
                )
            )

        if separation is not None and low_gr_score > 0.18 and separation >= 0.55:
            label = "possible gas effect"
            confidence = 0.6 + 0.18 * min(1.0, separation)
            reasons.append(
                _reason(
                    "density_neutron_separation",
                    "density-neutron separation",
                    "Density and neutron move apart in this interval, which can indicate gas effect or a strong lithology change.",
                )
            )
        elif low_gr_score >= 0.24 and rhob_mean is not None and nphi_mean is not None and separation is not None and separation <= 0.22:
            label = "carbonate candidate"
            confidence = 0.58 + 0.16 * float(low_gr_score)
            reasons.append(
                _reason(
                    "low_gr",
                    "low GR",
                    "Gamma ray is low relative to the well, which supports a cleaner non-shaly interval.",
                )
            )
        elif low_gr_score >= 0.18 or low_vs_neighbors:
            label = "cleaner clastic candidate"
            confidence = 0.56 + 0.16 * max(float(low_gr_score), 0.15)
            reasons.append(
                _reason(
                    "low_gr",
                    "low GR",
                    "Gamma ray is lower than the shalier parts of the well, which supports a cleaner clastic candidate.",
                )
            )

    if rhob is None:
        reasons.append(
            _reason(
                "missing_rhob",
                "low confidence: missing RHOB",
                "Bulk density is not available for this interval, so lithology confidence is reduced.",
            )
        )
        confidence -= 0.08
    if nphi is None:
        reasons.append(
            _reason(
                "missing_nphi",
                "low confidence: missing NPHI",
                "Neutron porosity is not available for this interval, so cross-checking is limited.",
            )
        )
        confidence -= 0.06

    interval_thickness = float(abs(depth[interval_slice.stop - 1] - depth[interval_slice.start])) if interval_slice.stop - interval_slice.start > 1 else 0.0
    if interval_thickness < (6.0 if global_stats["depth_step"] <= 2.0 else 2.0):
        confidence -= 0.07
        reasons.append(
            _reason(
                "thin_interval",
                "thin interval",
                "Thin intervals are harder to interpret confidently with simple GR-first rules.",
            )
        )

    confidence = float(np.clip(confidence, 0.1, 0.95))
    if label == "uncertain" and not reasons:
        reasons.append(
            _reason(
                "ambiguous_signature",
                "ambiguous log signature",
                "The interval does not clearly separate into one of the simple Stage 2 MVP classes.",
            )
        )
    return label, confidence, reasons


def build_single_well_interpretation(
    depth: List[float],
    digitized_curves: Dict[str, Dict],
    config_curves: Optional[List[Dict]] = None,
    depth_unit: str = "FT",
    null_value: float = -999.25,
    source_key: Optional[str] = None,
    header_metadata: Optional[Dict] = None,
) -> Dict:
    depth_arr = np.asarray(depth, dtype=np.float32)
    specs = _build_curve_specs(digitized_curves, config_curves=config_curves, null_value=null_value)
    gr_spec = _pick_curve(specs, "GR")

    meta = {
        "schema": INTERPRETATION_SCHEMA,
        "algorithm": INTERPRETATION_ALGORITHM,
        "source_key": source_key,
        "created_at_utc": _utc_now_iso(),
        "updated_at_utc": _utc_now_iso(),
        "depth_unit": (depth_unit or "FT").upper(),
        "well_name": str((header_metadata or {}).get("well") or (header_metadata or {}).get("WELL") or "Current Well"),
        "curve_keys": [spec.key for spec in specs.values()],
        "gr_curve_key": gr_spec.key if gr_spec else None,
        "intervals": [],
        "warnings": [],
    }

    if depth_arr.size == 0 or not specs or gr_spec is None:
        meta["warnings"].append("No usable GR curve was found for single-well interpretation.")
        return meta

    gr_values = gr_spec.values
    if gr_values.size != depth_arr.size:
        meta["warnings"].append("GR curve length does not match depth basis; interpretation skipped.")
        return meta

    valid_gr = gr_values[np.isfinite(gr_values)]
    if valid_gr.size == 0:
        meta["warnings"].append("GR curve is entirely null; interpretation skipped.")
        return meta

    intervals, segmentation_debug = _segment_gr_curve(depth_arr, gr_values)
    depth_step = _median_step(depth_arr)
    global_stats = {
        "p10": float(np.nanpercentile(valid_gr, 10)),
        "p40": float(np.nanpercentile(valid_gr, 40)),
        "p60": float(np.nanpercentile(valid_gr, 60)),
        "p90": float(np.nanpercentile(valid_gr, 90)),
        "depth_step": depth_step,
    }

    support = {
        "RHOB": _pick_curve(specs, "RHOB").values if _pick_curve(specs, "RHOB") else None,
        "NPHI": _pick_curve(specs, "NPHI").values if _pick_curve(specs, "NPHI") else None,
        "RES": _pick_curve(specs, "RES").values if _pick_curve(specs, "RES") else None,
    }
    support_global_stats: Dict[str, Dict[str, float]] = {}
    for key, arr in support.items():
        if arr is None:
            continue
        valid = arr[np.isfinite(arr)]
        if valid.size == 0:
            continue
        support_global_stats[key] = {
            "p10": float(np.nanpercentile(valid, 10)),
            "p90": float(np.nanpercentile(valid, 90)),
        }

    meta["segmentation_debug"] = segmentation_debug
    meta["depth_range"] = {
        "top": float(depth_arr[0]),
        "base": float(depth_arr[-1]),
        "sample_count": int(depth_arr.size),
    }

    gr_interval_means: List[Optional[float]] = []
    interval_feature_rows: List[Dict[str, Dict[str, Optional[float]]]] = []
    for start_idx, end_idx in intervals:
        features = {"GR": _interval_feature_summary(gr_values[start_idx:end_idx])}
        for curve_type in ("RHOB", "NPHI", "RES"):
            arr = support.get(curve_type)
            if arr is not None and arr.size == depth_arr.size:
                features[curve_type] = _interval_feature_summary(arr[start_idx:end_idx])
        interval_feature_rows.append(features)
        gr_interval_means.append(features["GR"]["mean"])

    result_intervals: List[Dict] = []
    for idx, (start_idx, end_idx) in enumerate(intervals):
        interval_slice = slice(start_idx, end_idx)
        neighbor_gr = (
            gr_interval_means[idx - 1] if idx > 0 else None,
            gr_interval_means[idx + 1] if idx + 1 < len(gr_interval_means) else None,
        )
        label, confidence, reasons = _classify_interval(
            idx=idx,
            interval_slice=interval_slice,
            depth=depth_arr,
            gr=gr_values,
            global_stats=global_stats,
            support_global_stats=support_global_stats,
            support_curves=support,
            interval_features=interval_feature_rows[idx],
            neighbor_gr_means=neighbor_gr,
        )
        top_depth = float(depth_arr[start_idx])
        base_depth = float(depth_arr[end_idx - 1]) if end_idx - start_idx > 0 else top_depth
        result_intervals.append(
            {
                "id": f"INT-{idx + 1:03d}",
                "top_depth": top_depth,
                "base_depth": base_depth,
                "top_index": int(start_idx),
                "base_index_exclusive": int(end_idx),
                "sample_count": int(end_idx - start_idx),
                "thickness": float(abs(base_depth - top_depth)),
                "label": label,
                "label_color": LABEL_COLORS.get(label, LABEL_COLORS["uncertain"]),
                "confidence": confidence,
                "reasons": reasons,
                "confirmed": False,
                "manual_formation_name": "",
                "features": interval_feature_rows[idx],
            }
        )

    meta["intervals"] = result_intervals
    return meta


def save_interpretation(base_dir: Path, interpretation: Dict) -> Path:
    source_key = str(interpretation.get("source_key") or "").strip()
    if not source_key:
        raise ValueError("Interpretation is missing source_key.")
    out_dir = Path(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = dict(interpretation)
    payload["updated_at_utc"] = _utc_now_iso()
    out_path = out_dir / f"{source_key}.json"
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path


def load_interpretation(base_dir: Path, source_key: str) -> Optional[Dict]:
    key = str(source_key or "").strip()
    if not key:
        return None
    path = Path(base_dir) / f"{key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return data
