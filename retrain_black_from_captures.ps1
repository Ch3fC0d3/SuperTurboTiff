param(
    [string]$CapturesDir = ".\\training_captures",
    [string]$InitModel = ".\\models\\testtiflas_black_seg_v2.pt",
    [string]$OutModel = ".\\models\\testtiflas_black_seg_v2_captures.pt",
    [string]$Curve = "GR",
    [int]$Epochs = 4,
    [int]$BatchSize = 4,
    [string]$Device = "cpu",
    [switch]$IncludeNeedsReview
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
$progress = [System.IO.Path]::ChangeExtension($OutModel, ".progress.json")

if (-not (Test-Path $python)) {
    throw "Python virtualenv not found at $python"
}

$args = @(
    "train_curve_trace_model.py",
    "--saved-captures-dir", $CapturesDir,
    "--mode-filter", "black",
    "--curve", $Curve,
    "--init-model", $InitModel,
    "--out", $OutModel,
    "--epochs", $Epochs,
    "--batch-size", $BatchSize,
    "--device", $Device,
    "--progress-file", $progress
)

if ($IncludeNeedsReview) {
    $args += "--include-needs-review"
}

Push-Location $repoRoot
try {
    & $python @args
}
finally {
    Pop-Location
}
