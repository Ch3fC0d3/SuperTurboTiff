// Phase 2: Frontend integration for adaptive parameters
// Add to templates/index.html

// Enhanced parameter suggestions with learning
class AdaptiveParameterEngine {
    constructor() {
        this.learnedCache = new Map();
        this.lastUpdate = null;
    }
    
    async getLearnedParameters(curveType) {
        try {
            const response = await fetch(`/api/learned_parameters/${curveType}`);
            return await response.json();
        } catch (error) {
            console.warn('Failed to get learned parameters:', error);
            return null;
        }
    }
    
    async getParameterSuggestions(curveType) {
        try {
            const response = await fetch(`/api/suggest_parameters/${curveType}`);
            return await response.json();
        } catch (error) {
            console.warn('Failed to get parameter suggestions:', error);
            return null;
        }
    }
    
    async applyLearnedParameters(curveType, curveIndex) {
        const learned = await this.getLearnedParameters(curveType);
        if (!learned || learned.confidence < 0.3) {
            console.log(`Using default parameters for ${curveType} (confidence: ${learned?.confidence || 0})`);
            return false;
        }
        
        const params = learned.parameters;
        
        // Apply learned parameters to UI
        const elements = {
            leftPx: document.getElementById(`leftPx${curveIndex}`),
            rightPx: document.getElementById(`rightPx${curveIndex}`),
            mode: document.getElementById(`mode${curveIndex}`)
        };
        
        if (elements.leftPx && params.left_px !== undefined) {
            elements.leftPx.value = Math.round(params.left_px);
        }
        if (elements.rightPx && params.right_px !== undefined) {
            elements.rightPx.value = Math.round(params.right_px);
        }
        
        // Show confidence indicator
        this.showConfidenceIndicator(curveIndex, learned.confidence, learned.sample_size);
        
        return true;
    }
    
    showConfidenceIndicator(curveIndex, confidence, sampleSize) {
        const indicator = document.createElement('div');
        indicator.className = 'confidence-indicator';
        indicator.style.cssText = `
            position: absolute;
            top: 5px;
            right: 5px;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 10px;
            color: white;
            background: ${confidence > 0.7 ? '#28a745' : confidence > 0.4 ? '#ffc107' : '#6c757d'};
            z-index: 1000;
        `;
        indicator.textContent = `AI: ${Math.round(confidence * 100)}% (${sampleSize})`;
        
        const container = document.getElementById(`curveContainer${curveIndex}`);
        if (container) {
            container.style.position = 'relative';
            container.appendChild(indicator);
        }
    }
    
    async enhanceAiProposals(curves) {
        const enhanced = [];
        
        for (let i = 0; i < curves.length; i++) {
            const curve = curves[i];
            const curveType = curve.mnemonic || 'GR';
            
            const learned = await this.getLearnedParameters(curveType);
            
            if (learned && learned.confidence > 0.3) {
                curve.learned_parameters = learned.parameters;
                curve.confidence = learned.confidence;
                curve.using_learning = true;
            }
            
            enhanced.push(curve);
        }
        
        return enhanced;
    }
}

// Global instance
const adaptiveEngine = new AdaptiveParameterEngine();

// Enhanced AI curve proposals
async function enhancedAiAutoCurves() {
    try {
        const resp = await fetch('/api/enhanced_propose_curves', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                image: uploadedImage,
                region: {
                    left_px: primaryRegion.left_px,
                    right_px: primaryRegion.right_px,
                    top_px: parseInt(document.getElementById('topPx').value),
                    bottom_px: parseInt(document.getElementById('bottomPx').value)
                },
                use_learned: true
            })
        });
        
        const data = await resp.json();
        if (!data.success) {
            showStatus(`Enhanced AI proposal failed: ${data.error || 'unknown error'}`, 'error');
            return;
        }
        
        let curves = data.curves || [];
        if (!curves.length) {
            showStatus('Enhanced AI proposal returned no curves.', 'info');
            return;
        }
        
        // Apply learned parameters to each curve
        for (let i = 0; i < Math.min(6, curves.length); i++) {
            const curve = curves[i];
            const curveType = (curve.mnemonic || '').toUpperCase();
            
            await adaptiveEngine.applyLearnedParameters(curveType, i);
        }
        
        showStatus(`✅ Enhanced AI proposed ${curves.length} curve(s) with learning.`, 'success');
        
    } catch (err) {
        showStatus(`Enhanced AI proposal failed: ${err.message}`, 'error');
    }
}

// Quality-based parameter suggestions
function getQualityBasedSuggestion(adjustments) {
    if (!adjustments || adjustments.length < 3) {
        return { status: 'insufficient_data', message: 'Need at least 3 adjustments' };
    }
    
    const recent = adjustments.slice(-10); // Last 10 adjustments
    const avgQuality = recent.reduce((sum, adj) => sum + adj.quality_score, 0) / recent.length;
    
    return {
        status: 'ready',
        quality_score: avgQuality,
        sample_size: recent.length,
        recommendations: avgQuality > 0.8 ? 'high_confidence' : 'needs_more_data'
    };
}
