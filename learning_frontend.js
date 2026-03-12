// Phase 1: Frontend integration for user learning
// Add these functions to templates/index.html

// Quality scoring for user adjustments
function calculateQualityScore(userActions) {
    let score = 1.0;
    if (userActions.manualAdjustments > 0) score *= 0.8;
    if (userActions.undoActions > 0) score *= 0.9;
    if (userActions.finalAccept) score *= 1.2;
    return Math.max(0.1, Math.min(1.0, score));
}

// Record user adjustment to learning system
async function recordUserAdjustment(curveType, originalParams, userParams, userActions = {}) {
    const qualityScore = calculateQualityScore(userActions);
    
    try {
        const response = await fetch('/api/learn_from_user', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                curve_type: curveType,
                original_params: originalParams,
                user_params: userParams,
                quality_score: qualityScore,
                image_context: {
                    width: imageWidth,
                    height: imageHeight,
                    timestamp: new Date().toISOString()
                }
            })
        });
        
        const data = await response.json();
        if (data.success) {
            console.log(`Learning: Recorded adjustment for ${curveType}`, data.stats);
        }
    } catch (error) {
        console.warn('Learning: Failed to record adjustment', error);
    }
}

// Get user preference stats
async function getUserPreferences(curveType = null) {
    try {
        const url = curveType ? `/api/user_preferences?curve_type=${curveType}` : '/api/user_preferences';
        const response = await fetch(url);
        return await response.json();
    } catch (error) {
        console.warn('Learning: Failed to get preferences', error);
        return {};
    }
}

// Enhanced curve editing with learning
function enhancedCurveEdit(curveIndex, curveType) {
    const originalParams = {
        left_px: parseInt(document.getElementById(`leftPx${curveIndex}`).value),
        right_px: parseInt(document.getElementById(`rightPx${curveIndex}`).value),
        mode: document.getElementById(`mode${curveIndex}`).value,
        type: document.getElementById(`type${curveIndex}`).value
    };
    
    // Track original parameters before user makes changes
    window.originalCurveParams = originalParams;
    window.currentCurveType = curveType;
    
    // Hook into existing curve editing
    const originalApplyChanges = window.applyCurveChanges || function(){};
    window.applyCurveChanges = function() {
        const userParams = {
            left_px: parseInt(document.getElementById(`leftPx${curveIndex}`).value),
            right_px: parseInt(document.getElementById(`rightPx${curveIndex}`).value),
            mode: document.getElementById(`mode${curveIndex}`).value,
            type: document.getElementById(`type${curveIndex}`).value
        };
        
        // Record the adjustment
        const userActions = {
            manualAdjustments: 1,
            undoActions: 0,
            finalAccept: true
        };
        
        recordUserAdjustment(window.currentCurveType, window.originalCurveParams, userParams, userActions);
        
        // Call original function
        return originalApplyChanges.apply(this, arguments);
    };
}

// Auto-record AI suggestions vs user final choices
function recordAiSuggestionUsage(curveType, aiSuggestion, userFinalChoice) {
    const userActions = {
        manualAdjustments: JSON.stringify(aiSuggestion) !== JSON.stringify(userFinalChoice) ? 1 : 0,
        undoActions: 0,
        finalAccept: true
    };
    
    recordUserAdjustment(curveType, aiSuggestion, userFinalChoice, userActions);
}
