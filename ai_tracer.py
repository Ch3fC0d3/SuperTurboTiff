import cv2
import numpy as np
from pathlib import Path

import torch
import torch.nn as nn


COLORED_MODES = {"green", "red", "blue", "auto", "cyan", "magenta", "yellow", "orange", "purple"}


class ConvBlock(nn.Module):
    def __init__(self, in_ch: int, out_ch: int):
        super().__init__()
        self.block = nn.Sequential(
            nn.Conv2d(in_ch, out_ch, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(out_ch, out_ch, 3, padding=1),
            nn.ReLU(inplace=True),
        )

    def forward(self, x):
        return self.block(x)


class CurveSegNet(nn.Module):
    """Lean U-Net style mask predictor for per-pixel curve probability."""

    def __init__(self, in_ch: int = 1, base: int = 16):
        super().__init__()
        self.enc1 = ConvBlock(in_ch, base)
        self.pool1 = nn.MaxPool2d(2)
        self.enc2 = ConvBlock(base, base * 2)
        self.pool2 = nn.MaxPool2d(2)
        self.bottleneck = ConvBlock(base * 2, base * 4)
        self.up2 = nn.ConvTranspose2d(base * 4, base * 2, kernel_size=2, stride=2)
        self.dec2 = ConvBlock(base * 4, base * 2)
        self.up1 = nn.ConvTranspose2d(base * 2, base, kernel_size=2, stride=2)
        self.dec1 = ConvBlock(base * 2, base)
        self.head = nn.Conv2d(base, 1, kernel_size=1)

    def forward(self, x):
        e1 = self.enc1(x)
        e2 = self.enc2(self.pool1(e1))
        b = self.bottleneck(self.pool2(e2))
        d2 = self.up2(b)
        if d2.shape[-2:] != e2.shape[-2:]:
            d2 = nn.functional.interpolate(d2, size=e2.shape[-2:], mode="bilinear", align_corners=False)
        d2 = self.dec2(torch.cat([d2, e2], dim=1))
        d1 = self.up1(d2)
        if d1.shape[-2:] != e1.shape[-2:]:
            d1 = nn.functional.interpolate(d1, size=e1.shape[-2:], mode="bilinear", align_corners=False)
        d1 = self.dec1(torch.cat([d1, e1], dim=1))
        return self.head(d1).squeeze(1)


class LegacyCurveTraceNet(nn.Module):
    """Backward-compatible x-per-row regression model."""

    def __init__(self, in_ch: int = 1, base: int = 16):
        super().__init__()
        self.enc = nn.Sequential(
            nn.Conv2d(in_ch, base, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(base, base, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),
            nn.Conv2d(base, base * 2, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(base * 2, base * 2, 3, padding=1),
            nn.ReLU(inplace=True),
        )
        self.dec = nn.Sequential(
            nn.Upsample(scale_factor=2, mode="bilinear", align_corners=False),
            nn.Conv2d(base * 2, base, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(base, base, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(base, 1, 1),
            nn.Sigmoid(),
        )

    def forward(self, x):
        h = self.enc(x)
        out = self.dec(h)
        return out.squeeze(1)


class AITracer:
    def __init__(self, model_path="curve_trace_model.pt"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = None
        self.model_type = "legacy_regression"
        self.input_h = 256
        self.input_w = 128
        self.meta = {}

        p = Path(model_path)
        if not p.exists():
            print(f"⚠️ AI model not found at {model_path}")
            return

        try:
            ckpt = torch.load(str(p), map_location=self.device, weights_only=False)
            self.input_h = int(ckpt.get("input_h", 256))
            self.input_w = int(ckpt.get("input_w", 128))
            self.model_type = str(ckpt.get("model_type", "legacy_regression"))
            self.meta = {
                "model_type": self.model_type,
                "input_h": self.input_h,
                "input_w": self.input_w,
                "supported_modes": ckpt.get("supported_modes") or ["black"],
                "curve": ckpt.get("curve"),
                "target_width_px": float(ckpt.get("target_width_px", 2.5)),
                "mask_blur_sigma": float(ckpt.get("mask_blur_sigma", 0.8)),
            }

            if self.model_type in {"segmentation_v2", "segmentation"}:
                self.model = CurveSegNet().to(self.device)
            else:
                self.model = LegacyCurveTraceNet().to(self.device)
                self.model_type = "legacy_regression"
                self.meta["model_type"] = self.model_type

            self.model.load_state_dict(ckpt["state_dict"])
            self.model.eval()
            print(f"AI Model loaded from {model_path} (Device: {self.device}, Type: {self.model_type})")
        except Exception as e:
            self.model = None
            print(f"Failed to load AI model: {e}")

    def is_available(self):
        return self.model is not None

    def supports_mode(self, mode: str | None) -> bool:
        if not self.is_available():
            return False
        supported = self.meta.get("supported_modes") or ["black"]
        mode_name = str(mode or "black").strip().lower()
        if "*" in supported:
            return True
        if mode_name in supported:
            return True
        if "black" in supported and mode_name not in COLORED_MODES:
            return True
        return False

    def _preprocess(self, roi_bgr: np.ndarray) -> torch.Tensor:
        roi_gray = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)
        roi_resized = cv2.resize(roi_gray, (self.input_w, self.input_h), interpolation=cv2.INTER_AREA)
        x_tensor = torch.from_numpy(roi_resized).float().unsqueeze(0).unsqueeze(0) / 255.0
        return x_tensor.to(self.device)

    def _legacy_trace_to_prob_map(self, pred_norm: np.ndarray, orig_h: int, orig_w: int) -> np.ndarray:
        pred_x_small = pred_norm.astype(np.float32) * max(0.0, float(orig_w - 1))
        y_small = np.linspace(0.0, max(0.0, float(orig_h - 1)), self.input_h, dtype=np.float32)
        y_orig = np.arange(orig_h, dtype=np.float32)
        pred_x_orig = np.interp(y_orig, y_small, pred_x_small).astype(np.float32)

        sigma_px = max(1.25, min(6.0, float(self.meta.get("target_width_px", 2.5))))
        xs = np.arange(orig_w, dtype=np.float32)[None, :]
        probs = np.exp(-0.5 * ((xs - pred_x_orig[:, None]) / sigma_px) ** 2)
        return probs.astype(np.float32)

    @staticmethod
    def _resize_prob_small(prob_small: np.ndarray, orig_h: int, orig_w: int) -> np.ndarray:
        prob_small = np.asarray(prob_small, dtype=np.float32)
        if prob_small.ndim != 2:
            raise ValueError(f"Expected 2D prob map, got shape={prob_small.shape}")
        if prob_small.size == 0:
            return np.zeros((orig_h, orig_w), dtype=np.float32)
        return cv2.resize(prob_small, (orig_w, orig_h), interpolation=cv2.INTER_LINEAR).astype(np.float32)

    def _legacy_output_to_prob_map(self, pred_out, orig_h: int, orig_w: int) -> np.ndarray:
        pred_arr = np.asarray(pred_out.detach().cpu().numpy(), dtype=np.float32)
        pred_arr = np.squeeze(pred_arr)

        if pred_arr.ndim == 1:
            return self._legacy_trace_to_prob_map(pred_norm=pred_arr, orig_h=orig_h, orig_w=orig_w)

        if pred_arr.ndim == 2:
            prob_small = pred_arr
            if prob_small.shape == (self.input_w, self.input_h):
                prob_small = prob_small.T
            elif prob_small.shape != (self.input_h, self.input_w):
                prob_small = cv2.resize(prob_small, (self.input_w, self.input_h), interpolation=cv2.INTER_LINEAR)

            if float(np.nanmax(prob_small)) > 1.0 or float(np.nanmin(prob_small)) < 0.0:
                prob_small = 1.0 / (1.0 + np.exp(-np.clip(prob_small, -20.0, 20.0)))

            return self._resize_prob_small(prob_small, orig_h=orig_h, orig_w=orig_w)

        raise ValueError(f"Unsupported legacy model output shape: {pred_arr.shape}")

    @torch.no_grad()
    def predict_prob_map(self, roi_bgr: np.ndarray) -> np.ndarray:
        """
        Return an HxW float32 probability map in [0, 1].
        Works with both legacy x-regression checkpoints and new segmentation ones.
        """
        if self.model is None:
            raise RuntimeError("AI model not loaded.")

        orig_h, orig_w = roi_bgr.shape[:2]
        if orig_h <= 0 or orig_w <= 0:
            return np.zeros((0, 0), dtype=np.float32)

        x_tensor = self._preprocess(roi_bgr)
        pred_tensor = self.model(x_tensor)

        if self.model_type == "legacy_regression":
            prob = self._legacy_output_to_prob_map(pred_tensor, orig_h=orig_h, orig_w=orig_w)
        else:
            logits = pred_tensor.squeeze(0)
            prob_small = torch.sigmoid(logits).detach().cpu().numpy().astype(np.float32)
            prob = self._resize_prob_small(prob_small, orig_h=orig_h, orig_w=orig_w)

        blur_sigma = float(self.meta.get("mask_blur_sigma", 0.8))
        if blur_sigma > 1e-3 and prob.size:
            try:
                prob = cv2.GaussianBlur(prob, (0, 0), blur_sigma)
            except Exception:
                pass

        prob = np.clip(prob, 0.0, 1.0).astype(np.float32)
        return prob

    @staticmethod
    def _prob_map_to_trace(prob: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        if prob is None or prob.size == 0:
            return np.array([], dtype=np.float32), np.array([], dtype=np.float32)

        prob = np.asarray(prob, dtype=np.float32)
        h, w = prob.shape[:2]
        xs = np.full(h, np.nan, dtype=np.float32)
        confidence = np.zeros(h, dtype=np.float32)
        x_coords = np.arange(w, dtype=np.float32)

        for y in range(h):
            row = prob[y]
            row_max = float(np.max(row))
            if row_max < 1e-4:
                continue

            thr = max(0.12, row_max * 0.55)
            support = row >= thr
            if not np.any(support):
                continue

            idx = np.flatnonzero(support)
            if idx.size == 0:
                continue

            left = int(idx[0])
            right = int(idx[-1]) + 1
            row_band = row[left:right]
            wsum = float(np.sum(row_band))
            if wsum <= 1e-8:
                continue

            xs[y] = float(np.sum(x_coords[left:right] * row_band) / wsum)
            confidence[y] = row_max

        return xs, confidence

    @torch.no_grad()
    def trace(self, roi_bgr: np.ndarray) -> np.ndarray:
        prob = self.predict_prob_map(roi_bgr)
        xs, _ = self._prob_map_to_trace(prob)
        return xs

    @torch.no_grad()
    def trace_with_confidence(self, roi_bgr: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        prob = self.predict_prob_map(roi_bgr)
        return self._prob_map_to_trace(prob)
