import json
import os
from datetime import datetime
from collections import defaultdict
import hashlib

class UserPreferenceTracker:
    """Phase 1: Basic tracking of user curve adjustments"""
    
    def __init__(self, storage_path='user_preferences.json'):
        self.storage_path = storage_path
        self.adjustments = defaultdict(list)
        self.load_existing()
    
    def load_existing(self):
        """Load existing preferences if file exists"""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r') as f:
                    data = json.load(f)
                    for curve_type, adjustments in data.items():
                        self.adjustments[curve_type] = adjustments
            except (json.JSONDecodeError, IOError):
                pass  # Start fresh if file is corrupted
    
    def save_preferences(self):
        """Save preferences to disk"""
        try:
            with open(self.storage_path, 'w') as f:
                json.dump(dict(self.adjustments), f, indent=2, default=str)
        except IOError as e:
            print(f"Warning: Could not save preferences: {e}")
    
    def record_adjustment(self, curve_type, original_params, user_params, quality_score=1.0, image_context=None):
        """Record a user adjustment for learning"""
        adjustment = {
            'timestamp': datetime.now().isoformat(),
            'original_params': original_params,
            'user_params': user_params,
            'quality_score': quality_score,
            'image_context': image_context or self._get_image_context(),
            'session_id': self._get_session_id()
        }
        
        self.adjustments[curve_type].append(adjustment)
        
        # Keep only last 100 adjustments per curve type to prevent bloat
        if len(self.adjustments[curve_type]) > 100:
            self.adjustments[curve_type] = self.adjustments[curve_type][-100:]
        
        self.save_preferences()
    
    def get_adjustments(self, curve_type, limit=None):
        """Get recent adjustments for a curve type"""
        adjustments = self.adjustments.get(curve_type, [])
        if limit:
            return adjustments[-limit:]
        return adjustments
    
    def get_all_adjustments(self):
        """Get all adjustments across all curve types"""
        return dict(self.adjustments)
    
    def _get_image_context(self):
        """Generate basic context hash for image features"""
        # Simple context - can be enhanced later
        return {
            'width': None,  # Will be populated when image context is available
            'height': None,
            'curve_color': None,
            'noise_level': None
        }
    
    def _get_session_id(self):
        """Generate a simple session identifier"""
        return f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def get_stats(self, curve_type):
        """Get basic statistics for a curve type"""
        adjustments = self.adjustments.get(curve_type, [])
        if not adjustments:
            return {'count': 0, 'avg_quality': 0.0}
        
        avg_quality = sum(adj['quality_score'] for adj in adjustments) / len(adjustments)
        return {
            'count': len(adjustments),
            'avg_quality': avg_quality,
            'last_adjustment': adjustments[-1]['timestamp'] if adjustments else None
        }

# Global instance for easy access
tracker = UserPreferenceTracker()
