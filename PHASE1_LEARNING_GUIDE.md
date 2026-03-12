# Phase 1 Learning System - Quick Start Guide

## 🎯 What's Implemented
- **User adjustment tracking** - Records every curve parameter change
- **Quality scoring** - Rates adjustments based on user behavior
- **Persistent storage** - Saves preferences to `user_preferences.json`
- **REST API endpoints** - Easy integration with frontend

## 📋 API Endpoints

### Record User Adjustment
```
POST /api/learn_from_user
{
  "curve_type": "GR",
  "original_params": {"left_px": 100, "right_px": 200},
  "user_params": {"left_px": 95, "right_px": 205},
  "quality_score": 1.0
}
```

### Get Preferences
```
GET /api/user_preferences?curve_type=GR
```

### Clear Preferences
```
POST /api/clear_preferences
{"curve_type": "GR"}  // or omit for all curves
```

## 🧪 Testing Commands

```bash
# Test recording an adjustment
curl -X POST http://localhost:5000/api/learn_from_user \
  -H "Content-Type: application/json" \
  -d '{"curve_type": "GR", "original_params": {"left_px": 100}, "user_params": {"left_px": 105}}'

# Check GR preferences
curl http://localhost:5000/api/user_preferences?curve_type=GR

# Check all preferences
curl http://localhost:5000/api/user_preferences
```

## 🚀 Frontend Integration

Add to your HTML:
```html
<script src="learning_frontend.js"></script>
```

Use in curve editing:
```javascript
// Auto-track when user adjusts curves
recordUserAdjustment('GR', originalParams, userParams);
```

## 📊 Data Structure

Each adjustment records:
- **curve_type**: GR, RHOB, NPHI, DT, CALI, SP, or OTHER
- **original_params**: AI-suggested parameters
- **user_params**: User's final parameters
- **quality_score**: 0.1-1.0 based on user behavior
- **timestamp**: When the adjustment was made
- **image_context**: Basic image metadata

## 🔍 Monitoring

Check `user_preferences.json` for stored data:
```json
{
  "GR": [
    {
      "timestamp": "2025-12-29T15:50:09.517715",
      "original_params": {"left_px": 100, "right_px": 200},
      "user_params": {"left_px": 95, "right_px": 205},
      "quality_score": 1.0
    }
  ]
}
```

## ✅ Next Steps

1. **Test with real curve adjustments** - Make changes and watch preferences grow
2. **Monitor quality scores** - See how user behavior affects learning
3. **Prepare for Phase 2** - Parameter analysis and AI integration
