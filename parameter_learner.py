import numpy as np
import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

class ParameterLearner:
    """Phase 2: Intelligent parameter learning from user adjustments"""
    
    def __init__(self, tracker):
        self.tracker = tracker
        self.learned_params = defaultdict(dict)
        self.confidence_scores = defaultdict(float)
        self.parameter_weights = {
            'left_px': 1.0,
            'right_px': 1.0,
            'rail_threshold': 2.0,
            'rail_penalty': 1.5,
            'smooth_lambda': 1.5,
            'max_step': 1.0,
            'search_window': 1.0,
            'jump_gate': 2.0
        }
    
    def learn_parameters(self, curve_type: str, min_samples: int = 3) -> Dict[str, float]:
        """Learn optimal parameters from user adjustments"""
        adjustments = self.tracker.get_adjustments(curve_type)
        
        if len(adjustments) < min_samples:
            return self.get_default_params(curve_type)
        
        # Filter recent adjustments (last 30 days)
        recent_cutoff = datetime.now() - timedelta(days=30)
        recent_adjustments = [
            adj for adj in adjustments 
            if datetime.fromisoformat(adj['timestamp']) > recent_cutoff
        ]
        
        if len(recent_adjustments) < min_samples:
            recent_adjustments = adjustments
        
        # Calculate weighted parameters
        weighted_params = self._calculate_weighted_params(recent_adjustments)
        
        # Calculate confidence
        confidence = self._calculate_confidence(recent_adjustments)
        
        # Store learned parameters
        self.learned_params[curve_type] = weighted_params
        self.confidence_scores[curve_type] = confidence
        
        return weighted_params
    
    def _calculate_weighted_params(self, adjustments: List[Dict]) -> Dict[str, float]:
        """Calculate weighted average parameters based on quality and recency"""
        param_deltas = defaultdict(list)
        
        for adj in adjustments:
            original = adj['original_params']
            user = adj['user_params']
            quality = adj['quality_score']
            
            # Calculate time-based weight (decay over 30 days)
            days_old = (datetime.now() - datetime.fromisoformat(adj['timestamp'])).days
            time_weight = max(0.1, 1.0 - (days_old / 30.0))
            
            # Combined weight: quality × time × parameter importance
            total_weight = quality * time_weight
            
            # Track parameter deltas
            for param in self.parameter_weights.keys():
                if param in original and param in user:
                    delta = user[param] - original[param]
                    param_weight = self.parameter_weights[param]
                    weighted_delta = delta * total_weight * param_weight
                    param_deltas[param].append(weighted_delta)
        
        # Calculate final parameters
        learned = {}
        defaults = self.get_default_params('GR')  # Use GR as base
        
        for param, deltas in param_deltas.items():
            if deltas:
                avg_delta = np.mean(deltas)
                learned[param] = max(0.1, defaults[param] + avg_delta)
            else:
                learned[param] = defaults[param]
        
        return learned
    
    def _calculate_confidence(self, adjustments: List[Dict]) -> float:
        """Calculate confidence score for learned parameters"""
        if not adjustments:
            return 0.0
        
        # Factors affecting confidence
        sample_size = len(adjustments)
        avg_quality = np.mean([adj['quality_score'] for adj in adjustments])
        
        # Consistency measure (lower variance = higher confidence)
        param_consistencies = []
        for param in self.parameter_weights.keys():
            deltas = []
            for adj in adjustments:
                if param in adj['original_params'] and param in adj['user_params']:
                    deltas.append(adj['user_params'][param] - adj['original_params'][param])
            if deltas:
                param_consistencies.append(1.0 / (1.0 + np.std(deltas)))
        
        avg_consistency = np.mean(param_consistencies) if param_consistencies else 0.5
        
        # Combined confidence
        confidence = min(1.0, (sample_size / 10.0) * avg_quality * avg_consistency)
        return confidence
    
    def get_learned_params(self, curve_type: str) -> Dict[str, float]:
        """Get learned parameters with confidence"""
        if curve_type not in self.learned_params:
            self.learn_params(curve_type)
        
        return {
            'parameters': self.learned_params[curve_type],
            'confidence': self.confidence_scores[curve_type],
            'sample_size': len(self.tracker.get_adjustments(curve_type))
        }
    
    def get_default_params(self, curve_type: str) -> Dict[str, float]:
        """Get default parameters per curve type"""
        defaults = {
            'GR': {
                'left_px': 0,
                'right_px': 100,
                'rail_threshold': 0.02,
                'rail_penalty': 10000.0,
                'smooth_lambda': 0.00001,
                'max_step': 100,
                'search_window': 100,
                'jump_gate': 0.06
            },
            'RHOB': {
                'left_px': 0,
                'right_px': 100,
                'rail_threshold': 0.02,
                'rail_penalty': 10000.0,
                'smooth_lambda': 0.00001,
                'max_step': 100,
                'search_window': 100,
                'jump_gate': 0.06
            },
            'NPHI': {
                'left_px': 0,
                'right_px': 100,
                'rail_threshold': 0.02,
                'rail_penalty': 10000.0,
                'smooth_lambda': 0.00001,
                'max_step': 100,
                'search_window': 100,
                'jump_gate': 0.06
            }
        }
        return defaults.get(curve_type, defaults['GR'])
    
    def suggest_parameter_adjustments(self, curve_type: str) -> Dict[str, str]:
        """Suggest parameter adjustments based on learning"""
        learned = self.get_learned_params(curve_type)
        
        if learned['confidence'] < 0.3:
            return {'status': 'insufficient_data', 'message': 'Need more user adjustments'}
        
        defaults = self.get_default_params(curve_type)
        suggestions = {}
        
        for param, learned_value in learned['parameters'].items():
            default_value = defaults[param]
            delta = learned_value - default_value
            
            if abs(delta) > 0.1 * abs(default_value):
                direction = 'increase' if delta > 0 else 'decrease'
                suggestions[param] = {
                    'current': default_value,
                    'suggested': learned_value,
                    'direction': direction,
                    'confidence': learned['confidence']
                }
        
        return {
            'status': 'ready',
            'suggestions': suggestions,
            'confidence': learned['confidence'],
            'sample_size': learned['sample_size']
        }

# Global instance - will be created after tracker is imported
# learner = ParameterLearner(tracker)  # Moved to web_app.py
