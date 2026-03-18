param(
    [string]$Root = "D:\Users\gabep\Desktop\TestTiflas\log_pairs_out",
    [string]$OutDir = ".\pair_training_examples",
    [string]$Curve = "GR",
    [string]$Sources = "wvgs",
    [string]$InitModel = ".\models\testtiflas_black_seg_v2.pt",
    [string]$OutModel = ".\models\testtiflas_black_seg_v2_pairs.pt",
    [int]$Epochs = 4,
    [int]$BatchSize = 4,
    [int]$MaxPairs = 0,
    [string]$PairId = "",
    [int]$MaxScansPerPair = 0,
    [int]$MaxWindowsPerPanel = 18,
    [int]$ScoreHeight = 1800,
    [int]$WindowHeight = 1400,
    [int]$WindowStride = 1000,
    [double]$MinPanelScore = 0.10,
    [double]$MinWindowScore = 0.08,
    [double]$MinValidFraction = 0.70,
    [string]$Device = "cpu",
    [switch]$AllowFullScans,
    [switch]$InlineImages
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$progress = [System.IO.Path]::ChangeExtension($OutModel, ".progress.json")
$examplesPath = Join-Path $OutDir "examples.jsonl"

if (-not (Test-Path $python)) {
    throw "Python virtualenv not found at $python"
}

$exportArgs = @(
    "export_log_pair_examples.py",
    "--root", $Root,
    "--out-dir", $OutDir,
    "--curve", $Curve,
    "--sources", $Sources,
    "--max-pairs", $MaxPairs,
    "--pair-id", $PairId,
    "--max-scans-per-pair", $MaxScansPerPair,
    "--max-windows-per-panel", $MaxWindowsPerPanel,
    "--score-height", $ScoreHeight,
    "--window-height", $WindowHeight,
    "--window-stride", $WindowStride,
    "--min-panel-score", $MinPanelScore,
    "--min-window-score", $MinWindowScore,
    "--min-valid-fraction", $MinValidFraction
)

if ($AllowFullScans) {
    $exportArgs += "--allow-full-scans"
}

if ($InlineImages) {
    $exportArgs += "--inline-images"
}

$trainArgs = @(
    "train_curve_trace_model.py",
    "--examples", $examplesPath,
    "--mode-filter", "black",
    "--curve", $Curve,
    "--init-model", $InitModel,
    "--out", $OutModel,
    "--epochs", $Epochs,
    "--batch-size", $BatchSize,
    "--device", $Device,
    "--progress-file", $progress
)

Push-Location $repoRoot
try {
    & $python @exportArgs
    if (-not (Test-Path $examplesPath)) {
        throw "Expected examples file not found at $examplesPath"
    }
    $exampleCount = @(Get-Content $examplesPath | Where-Object { $_.Trim() }).Count
    if ($exampleCount -le 0) {
        Write-Warning "Export finished but produced 0 examples. Adjust pair/source filters or lower the export thresholds before training."
        return
    }
    & $python @trainArgs
}
finally {
    Pop-Location
}
