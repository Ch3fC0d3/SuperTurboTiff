@app.route('/api/learned_parameters/<curve_type>', methods=['GET'])
def get_learned_parameters(curve_type):
    """Get learned parameters for a specific curve type"""
    try:
        result = learner.get_learned_params(curve_type)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/suggest_parameters/<curve_type>', methods=['GET'])
def suggest_parameters(curve_type):
    """Get parameter suggestions based on learning"""
    try:
        suggestions = learner.suggest_parameter_adjustments(curve_type)
        return jsonify(suggestions)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/enhanced_propose_curves', methods=['POST'])
def enhanced_propose_curves():
    """Enhanced curve proposals using learned parameters"""
    data = request.json or {}
    image_data = data.get('image')
    region = data.get('region') or {}
    use_learned = data.get('use_learned', True)
    
    # Use existing propose_curves logic but enhance with learned parameters
    result = propose_curves()
    
    if use_learned and result[1] == 200:  # Success
        curves = result[0].json.get('curves', [])
        enhanced_curves = []
        
        for curve in curves:
            curve_type = curve.get('mnemonic', 'GR').upper()
            if curve_type in ['GR', 'RHOB', 'NPHI', 'DT', 'CALI', 'SP']:
                learned = learner.get_learned_params(curve_type)
                if learned['confidence'] > 0.3:
                    # Apply learned parameters where appropriate
                    curve['learned_parameters'] = learned['parameters']
                    curve['confidence'] = learned['confidence']
            enhanced_curves.append(curve)
        
        return jsonify({
            'success': True,
            'curves': enhanced_curves,
            'using_learned': True
        })
    
    return result
