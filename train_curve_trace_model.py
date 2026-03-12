#!/usr/bin/env python3

import argparse
import datetime
import base64
import json
import os
from pathlib import Path

import cv2
import numpy as np
import torch
from torch import nn
from torch.utils.data import Dataset, DataLoader
from torch.utils.data._utils.collate import default_collate

try:
    from PIL import Image
except Exception:
    Image = None


def _decode_data_url_image(data_url: str) -> np.ndarray:
    b64 = data_url.split(',', 1)[1]
    img_bytes = base64.b64decode(b64)
    nparr = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError('Failed to decode image')
    return img


def _load_roi_from_path(
    image_path: str,
    top: int,
    bot: int,
    left: int,
    right: int,
):
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
            roi_img = img.crop((left, top, right, bot)).convert('RGB')
            return np.array(roi_img), (right - left)
    except Exception:
        return None, None


class CurveTraceDataset(Dataset):
    def __init__(
        self,
        configs_path: Path,
        results_path: Path,
        out_h: int = 256,
        out_w: int = 128,
        curve_filter: str | None = None,
    ):
        self.out_h = int(out_h)
        self.out_w = int(out_w)
        self.items = []

        # Helper to iterate items from either JSON array or JSONL
        def _iter_json_items(path: Path):
            use_jsonl = path.suffix.lower() == '.jsonl'
            
            # For JSONL files, stream line by line
            if use_jsonl:
                with path.open('r', encoding='utf-8', buffering=65536) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                yield json.loads(line)
                            except json.JSONDecodeError:
                                continue
            else:
                # For JSON files, use optimized streaming parser
                with path.open('r', encoding='utf-8', buffering=262144) as f:
                    # Read first char to detect format
                    first_char = f.read(1)
                    while first_char and first_char.isspace():
                        first_char = f.read(1)
                    
                    if first_char == '[':
                        # JSON array format - parse objects line by line
                        f.seek(0)
                        for line in f:
                            line = line.strip()
                            if line and line not in ('[', ']', ','):
                                if line.endswith(','):
                                    line = line[:-1]
                                try:
                                    yield json.loads(line)
                                except json.JSONDecodeError:
                                    pass
                    elif first_char == '{':
                        # JSON object with "results" array - skip to results and parse objects
                        f.seek(0)
                        in_results = False
                        for line in f:
                            line = line.strip()
                            
                            # Check if we're entering results array
                            if not in_results and '"results"' in line and '[' in line:
                                in_results = True
                                # Extract any objects on this line after the [
                                rest = line.split('[', 1)[1]
                                if rest and rest not in (']', ','):
                                    if rest.endswith(','):
                                        rest = rest[:-1]
                                    try:
                                        yield json.loads(rest)
                                    except json.JSONDecodeError:
                                        pass
                                continue
                            
                            if in_results:
                                # Stop at end of results array
                                if line in (']', '],', '}'):
                                    break
                                
                                # Parse object lines
                                if line and line not in ('[', ']', ','):
                                    if line.endswith(','):
                                        line = line[:-1]
                                    try:
                                        yield json.loads(line)
                                    except json.JSONDecodeError:
                                        pass
                    else:
                        # Fallback: assume JSONL format
                        f.seek(0)
                        for line in f:
                            line = line.strip()
                            if line:
                                try:
                                    yield json.loads(line)
                                except json.JSONDecodeError:
                                    continue

        # Iterate streams in lock-step
        config_iter = _iter_json_items(configs_path)
        result_iter = _iter_json_items(results_path)
        
        pair_count = 0
        item_count = 0
        skipped_count = 0
        
        for cfg_job, res_item in zip(config_iter, result_iter):
            pair_count += 1
            if pair_count % 100 == 0:
                print(f'[dataset_loading] Processed {pair_count} config-result pairs, created {item_count} items so far')
            
            # Handle JSONL wrapper from pipeline {source:..., result:...}
            # If keys 'source' and 'result' exist, unwrap it.
            res = res_item
            if isinstance(res_item, dict) and 'result' in res_item and 'source' in res_item:
                res = res_item['result']

            if not isinstance(res, dict) or not res.get('success'):
                skipped_count += 1
                continue

            img_data_url = cfg_job.get('image')
            img_path = cfg_job.get('image_path')
            
            if not img_data_url and not img_path:
                skipped_count += 1
                continue

            depth_cfg = (cfg_job.get('config') or {}).get('depth') or {}
            top_px = int(depth_cfg.get('top_px', 0))
            bottom_px = int(depth_cfg.get('bottom_px', 0))
            if bottom_px <= top_px:
                skipped_count += 1
                continue

            curves = (cfg_job.get('config') or {}).get('curves') or []
            curve_traces = res.get('curve_traces') or {}
            global_opts = (cfg_job.get('config') or {}).get('global_options') or {}
            null_val = float(global_opts.get('null', -999.25))

            for c in curves:
                curve_name = c.get('las_mnemonic') or c.get('name')
                if not curve_name:
                    continue
                if curve_filter and curve_name.upper() != curve_filter.upper():
                    continue

                left_px = int(c.get('left_px', 0))
                right_px = int(c.get('right_px', 0))
                if right_px <= left_px:
                    continue

                trace = curve_traces.get(curve_name)
                if trace is None:
                    continue

                self.items.append({
                    'image_data_url': img_data_url,
                    'image_path': img_path,
                    'top_px': top_px,
                    'bottom_px': bottom_px,
                    'left_px': left_px,
                    'right_px': right_px,
                    'trace': trace,
                    'null_val': null_val,
                    'curve_name': curve_name,
                })
                item_count += 1
        
        print(f'[dataset_loading] Complete: processed {pair_count} pairs, created {item_count} items, skipped {skipped_count}')

        if not self.items:
            raise ValueError('No training items found. Make sure configs/results files match and contain traces.')

    def __len__(self):
        return len(self.items)

    def __getitem__(self, idx: int):
        it = self.items[idx]
        top = max(0, int(it['top_px']))
        bot = int(it['bottom_px'])
        left = max(0, int(it['left_px']))
        right = int(it['right_px'])

        roi = None
        orig_w = None

        if it.get('image_path'):
            roi, orig_w = _load_roi_from_path(it['image_path'], top, bot, left, right)
            if roi is None:
                try:
                    img = cv2.imread(it['image_path'], cv2.IMREAD_COLOR)
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
                img = _decode_data_url_image(it['image_data_url'])
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

        roi_gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        roi_resized = cv2.resize(roi_gray, (self.out_w, self.out_h), interpolation=cv2.INTER_AREA)
        x = (roi_resized.astype(np.float32) / 255.0)[None, :, :]

        trace = np.asarray(it['trace'], dtype=np.float32)
        orig_h = trace.shape[0]
        orig_w = max(1, int(orig_w or (right - left)))

        ys = np.linspace(0, max(0, orig_h - 1), self.out_h)
        y0 = np.floor(ys).astype(np.int64)
        y1 = np.clip(y0 + 1, 0, orig_h - 1)
        wa = (ys - y0).astype(np.float32)
        tb = trace[y0] * (1.0 - wa) + trace[y1] * wa

        null_val = float(it['null_val'])
        valid = np.isfinite(tb) & (tb != null_val) & (tb >= 0.0) & (tb <= float(orig_w - 1))

        tb = np.clip(tb, 0.0, float(orig_w - 1))
        if orig_w <= 1:
            t = np.zeros((self.out_h,), dtype=np.float32)
        else:
            t = tb / float(orig_w - 1)

        return {
            'x': torch.from_numpy(x),
            't': torch.from_numpy(t),
            'mask': torch.from_numpy(valid.astype(np.float32)),
            'curve_name': it['curve_name'],
        }


class CurveTraceNet(nn.Module):
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
            nn.Upsample(scale_factor=2, mode='bilinear', align_corners=False),
            nn.Conv2d(base * 2, base, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(base, base, 3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(base, 1, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        feat = self.enc(x)
        logits = self.dec(feat).squeeze(1)  # (N,H,W)
        prob = torch.softmax(logits, dim=-1)
        xs = torch.linspace(0.0, 1.0, logits.shape[-1], device=logits.device)
        pred = (prob * xs).sum(dim=-1)
        return pred


def _masked_mse(pred: torch.Tensor, tgt: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    mask = mask.float()
    denom = mask.sum().clamp(min=1.0)
    return (((pred - tgt) ** 2) * mask).sum() / denom


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--configs', default='training_data.configs.json')
    ap.add_argument('--results', default='training_data.json')
    ap.add_argument('--curve', default=None)
    ap.add_argument('--out', default='curve_trace_model.pt')
    ap.add_argument('--epochs', type=int, default=3)
    ap.add_argument('--batch-size', type=int, default=2)
    ap.add_argument('--lr', type=float, default=1e-3)
    ap.add_argument('--h', type=int, default=256)
    ap.add_argument('--w', type=int, default=128)
    ap.add_argument('--device', default='cpu')
    ap.add_argument('--log-every', type=int, default=25)
    ap.add_argument('--progress-file', default='')
    ap.add_argument('--resume', action='store_true', help='Resume from latest epoch checkpoint if available')
    args = ap.parse_args()

    device = torch.device(args.device)

    progress_path = Path(args.progress_file) if args.progress_file else None

    def _write_progress(payload: dict):
        if progress_path is None:
            return
        payload = dict(payload)
        payload['updated_at'] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        try:
            progress_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
        except Exception:
            pass

    _write_progress({'stage': 'dataset_loading'})

    ds = CurveTraceDataset(
        configs_path=Path(args.configs),
        results_path=Path(args.results),
        out_h=args.h,
        out_w=args.w,
        curve_filter=args.curve,
    )

    def _collate(batch):
        batch = [b for b in batch if b is not None]
        if not batch:
            return None
        return default_collate(batch)

    dl = DataLoader(ds, batch_size=args.batch_size, shuffle=True, num_workers=0, collate_fn=_collate)

    model = CurveTraceNet().to(device)
    opt = torch.optim.Adam(model.parameters(), lr=args.lr)

    out_path = Path(args.out)
    start_epoch = 1
    if args.resume:
        ckpt_path = out_path.with_suffix('.ckpt.pt')
        if ckpt_path.exists():
            ckpt = torch.load(str(ckpt_path), map_location=device)
            model.load_state_dict(ckpt['state_dict'])
            opt.load_state_dict(ckpt['opt_state_dict'])
            start_epoch = ckpt['epoch'] + 1
            print(f'Resumed from checkpoint: epoch {ckpt["epoch"]} (loss={ckpt.get("avg_loss", "?"):.6f})')
        else:
            print('No checkpoint found, starting from scratch.')

    model.train()
    for epoch in range(start_epoch, args.epochs + 1):
        total = 0.0
        steps = 0
        total_steps = len(dl)
        _write_progress({'stage': 'train_epoch_start', 'epoch': epoch, 'total_epochs': args.epochs})
        for batch in dl:
            if batch is None:
                continue
            x = batch['x'].to(device)
            t = batch['t'].to(device)
            m = batch['mask'].to(device)

            pred = model(x)
            loss = _masked_mse(pred, t, m)

            opt.zero_grad(set_to_none=True)
            loss.backward()
            opt.step()

            total += float(loss.item())
            steps += 1

            if args.log_every > 0 and steps % args.log_every == 0:
                print(f'epoch={epoch} step={steps}/{total_steps} loss={loss.item():.6f}')
                _write_progress({
                    'stage': 'train_step',
                    'epoch': epoch,
                    'total_epochs': args.epochs,
                    'step': steps,
                    'total_steps': total_steps,
                    'loss': float(loss.item()),
                })

        avg = total / max(1, steps)
        print(f'epoch={epoch} loss={avg:.6f}')
        _write_progress({
            'stage': 'train_epoch_done',
            'epoch': epoch,
            'total_epochs': args.epochs,
            'avg_loss': float(avg),
            'steps': steps,
            'total_steps': total_steps,
        })
        ckpt_path = out_path.with_suffix('.ckpt.pt')
        ckpt_path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            'epoch': epoch,
            'state_dict': model.state_dict(),
            'opt_state_dict': opt.state_dict(),
            'avg_loss': float(avg),
            'input_h': int(args.h),
            'input_w': int(args.w),
            'curve': args.curve,
        }, str(ckpt_path))
        print(f'checkpoint saved: {ckpt_path}')

    out_path = Path(args.out)
    payload = {
        'state_dict': model.state_dict(),
        'input_h': int(args.h),
        'input_w': int(args.w),
        'curve': args.curve,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(payload, str(out_path))
    print(f'saved: {out_path}')


if __name__ == '__main__':
    main()
