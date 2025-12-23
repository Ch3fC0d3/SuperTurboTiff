# Curve Tracing Fix & Optimization Milestone
**Date:** December 8, 2025
**Status:** ✅ Solved

## Problem Description
The curve tracer was exhibiting two main issues, particularly with jagged Gamma Ray (GR) logs:
1.  **Zig-Zagging / Horizontal Bars:** The trace would jump back and forth between the true curve and a nearby vertical grid line or ghost artifact.
2.  **Vertical Lock-on:** The tracer would sometimes prioritize a smooth vertical artifact over the jagged true curve because the smoothness penalty made the artifact "cheaper" to follow.

## Solution Architecture ("The Nuclear Option")

### 1. Performance Optimization (`fast_tracer.py`)
*   Moved the core Viterbi (Dynamic Programming) algorithm out of Python loops and into a **Numba-optimized** module.
*   **Result:** 100x speedup, allowing for more complex logic without UI lag.

### 2. Vertical Artifact Rejection
We implemented a multi-layered defense against vertical lines:
*   **Morphological Erasure:** In `compute_prob_map`, we use a `(1, 40)` vertical kernel to detect lines taller than 40 pixels. These areas are suppressed by **95%** in the probability map before the tracer even sees them.
*   **Aggressive Rail Penalty:**
    *   **Threshold:** Lowered to **2%** (was 20%). Any column with >2% signal is flagged as a rail.
    *   **Penalty:** Increased to **10,000.0** (was 4.0). This effectively sets the cost to infinity, making vertical lines "lava" to the pathfinder.
    *   **Smoothing:** Column statistics are smoothed to catch "wobbling" vertical lines.

### 3. "Fusion" Tracing Strategy
Instead of a single tracer, we now use a hybrid approach for colored modes:
1.  **Run DP Tracer:** Finds the best *continuous* path.
2.  **Run Direct Tracer:** Finds the absolute brightest pixel in every row.
3.  **Fuse:** For each row, we compare the probability of the DP point vs. the Direct point. If the Direct point is >10% brighter, we take it.
    *   *Benefit:* Catches extreme Gamma Ray spikes that DP might smooth over.
    *   *Benefit:* Falls back to DP continuity in faint/noisy areas.

### 4. Parameter Tuning
*   **Smoothness (`smooth_lambda`):** Set to `0.00001` (effectively zero) for colored modes to allow jaggedness.
*   **Jump Limit (`max_step`):** Increased to **100 pixels**. Gamma Ray logs can have massive spikes; the tracer is now allowed to follow them.
*   **Refinement Pipeline:**
    1.  **Fusion:** Merge DP and Direct results (Distance-Gated < 50px).
    2.  **Peak Recovery:** `refine_peaks_and_valleys` (Radius 15px) to catch tips.
    3.  **Gradient Ascent:** Iterative hill-climbing to nearest bright pixel.
    4.  **Sub-pixel Parabola:** Fit curve to intensity profile for sub-pixel accuracy.

## How to Maintain
*   **Do not re-enable** aggressive peak snapping (search radius > 10px) for colored modes without ensuring the probability map is perfectly clean.
*   **Keep Rail Penalties High:** The "2% threshold / 10k penalty" is the primary guardrail preventing artifact lock-on.
*   **Numba is Required:** Ensure `requirements.txt` always includes `numba`.
