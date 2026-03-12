from flask import request, jsonify
from user_tracker import tracker

@app.route('/api/learn_from_user', methods=['POST'])
def learn_from_user():
    """Record user curve adjustments for learning"""
    data = request.json or {}
    
    required_fields = ['curve_type', 'original_params', 'user_params']
    for field in required_fields:
        if field not in data:
            return jsonify({
                'success': False, 
                'error': f'Missing required field: {field}'
            }), 400
    
    try:
        curve_type = data['curve_type']
        original_params = data['original_params']
        user_params = data['user_params']
        quality_score = data.get('quality_score', 1.0)
        image_context = data.get('image_context')
        
        # Validate curve type
        valid_types = ['GR', 'RHOB', 'NPHI', 'DT', 'CALI', 'SP', 'OTHER']
        if curve_type not in valid_types:
            curve_type = 'OTHER'
        
        # Record the adjustment
        tracker.record_adjustment(
            curve_type=curve_type,
            original_params=original_params,
            user_params=user_params,
            quality_score=quality_score,
            image_context=image_context
        )
        
        # Return stats for feedback
        stats = tracker.get_stats(curve_type)
        
        return jsonify({
            'success': True,
            'stats': stats,
            'message': f'Adjustment recorded for {curve_type}'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/user_preferences', methods=['GET'])
def get_user_preferences():
    """Get user preference statistics"""
    curve_type = request.args.get('curve_type')
    
    if curve_type:
        adjustments = tracker.get_adjustments(curve_type)
        stats = tracker.get_stats(curve_type)
        return jsonify({
            'curve_type': curve_type,
            'adjustments': adjustments,
            'stats': stats
        })
    else:
        all_adjustments = tracker.get_all_adjustments()
        all_stats = {ct: tracker.get_stats(ct) for ct in all_adjustments.keys()}
        return jsonify({
            'all_adjustments': all_adjustments,
            'all_stats': all_stats
        })

@app.route('/api/clear_preferences', methods=['POST'])
def clear_preferences():
    """Clear user preferences (for testing/reset)"""
    curve_type = request.json.get('curve_type')
    
    if curve_type:
        tracker.adjustments[curve_type] = []
    else:
        tracker.adjustments.clear()
    
    tracker.save_preferences()
    
    return jsonify({
        'success': True,
        'message': f'Preferences cleared for {curve_type or "all curves"}'
    })
