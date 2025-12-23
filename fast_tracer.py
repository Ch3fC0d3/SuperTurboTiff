import numpy as np
try:
    from numba import jit
    NUMBA_AVAILABLE = True
except Exception:
    NUMBA_AVAILABLE = False
    def jit(*args, **kwargs):
        def _wrap(fn):
            return fn
        return _wrap

@jit(nopython=True, cache=True)
def run_viterbi(cost, prob, max_step, smooth_lambda, curv_lambda):
    """
    Optimized Viterbi algorithm for curve tracing using Numba.
    
    Args:
        cost: (h, w) float32 array of costs
        prob: (h, w) float32 array of probabilities (0-1)
        max_step: int, maximum horizontal jump
        smooth_lambda: float, penalty for 1st derivative (jumps)
        curv_lambda: float, penalty for 2nd derivative (kinks)
        
    Returns:
        xs: (h,) float32 array of x-coordinates (with NaNs)
        confidence: (h,) float32 array of confidence scores
    """
    h, w = cost.shape
    big = 1e6
    
    # DP tables
    dp = np.full((h, w), big, dtype=np.float32)
    prev = np.full((h, w), -1, dtype=np.int16)
    
    # First row
    dp[0, :] = cost[0, :]
    
    # Forward pass
    for y in range(1, h):
        for x in range(w):
            x0 = max(0, x - max_step)
            x1 = min(w, x + max_step + 1)
            
            best_val = big
            best_xp = -1
            
            for xp in range(x0, x1):
                # 1st derivative penalty
                dx = x - xp
                smooth_penalty = smooth_lambda * (dx * dx)
                
                # 2nd derivative penalty
                if curv_lambda > 0.0 and y >= 2:
                    xpp = prev[y - 1, xp]
                    if xpp >= 0:
                        k = x - 2 * xp + xpp
                        smooth_penalty += curv_lambda * (k * k)
                
                v = dp[y - 1, xp] + cost[y, x] + smooth_penalty
                if v < best_val:
                    best_val = v
                    best_xp = xp
            
            dp[y, x] = best_val
            prev[y, x] = best_xp
            
    # Find best ending point
    bottom_start = max(0, int(h * 0.8))
    best_cost = big
    best_y = h - 1
    best_x = 0
    
    for y in range(bottom_start, h):
        # Find min_x in this row
        min_x = 0
        min_val = big
        for x in range(w):
            if dp[y, x] < min_val:
                min_val = dp[y, x]
                min_x = x
        
        if min_val < best_cost:
            best_cost = min_val
            best_y = y
            best_x = min_x
            
    # Backtrack
    path_x = np.full(h, -1, dtype=np.int32)
    path_x[best_y] = best_x
    
    for y in range(best_y, 0, -1):
        curr_x = path_x[y]
        if curr_x >= 0:
            path_x[y - 1] = prev[y, curr_x]
            
    # Forward fill
    if best_y < h - 1:
        last_x = best_x
        for y in range(best_y + 1, h):
            x0 = max(0, last_x - max_step)
            x1 = min(w, last_x + max_step + 1)
            
            # Find argmax in window
            best_local_idx = -1
            best_local_val = -1.0
            
            # Manual argmax to avoid slicing/allocating
            for xi in range(x0, x1):
                if prob[y, xi] > best_local_val:
                    best_local_val = prob[y, xi]
                    best_local_idx = xi
            
            if best_local_idx != -1:
                path_x[y] = best_local_idx
                last_x = best_local_idx
            else:
                # Fallback if window is somehow empty or all zeros (unlikely)
                path_x[y] = last_x

    # Compute confidence and result
    xs = np.full(h, np.nan, dtype=np.float32)
    confidence = np.zeros(h, dtype=np.float32)
    
    for y in range(h):
        x = path_x[y]
        if x < 0 or x >= w:
            confidence[y] = 0.0
            continue
            
        p_best = prob[y, x]
        
        # Second best in window
        x0 = max(0, x - max_step)
        x1 = min(w, x + max_step + 1)
        
        # We need second best value
        max_val = -1.0
        second_max_val = -1.0
        
        for xi in range(x0, x1):
            val = prob[y, xi]
            if val > max_val:
                second_max_val = max_val
                max_val = val
            elif val > second_max_val:
                second_max_val = val
                
        # Confidence = p_best - p_second
        # If there was only one pixel or max_val was p_best
        # Logic matches: p_second = probs_sorted[1] if size > 1 else 0.0
        
        # If x is the max_val, then p_best is max_val. 
        # We want the *next* best that isn't p_best? 
        # The original code sorts the window. 
        # If p_best is indeed the max in the window (which it should be if it's on the path or close), 
        # then second_max_val is what we want.
        
        conf = 0.0
        if second_max_val >= 0:
            conf = p_best - second_max_val
        else:
            conf = p_best
            
        confidence[y] = conf
        
        # Threshold
        if p_best >= 0.01:
            xs[y] = float(x)
            
    return xs, confidence
