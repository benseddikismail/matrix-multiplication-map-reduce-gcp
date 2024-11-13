import os
import json
from google.cloud import storage
import numpy as np
from collections import defaultdict


BUCKET_NAME = os.environ.get('BUCKET_NAME')
    
def reducer(request):

    data = json.loads(request.data)
    
    key = data['aggregated_data']['key']
    values = data['aggregated_data']['values']
    matrix_dims = data['aggregated_data']['matrix_dims']
    
    i, k = map(int, key.split(','))
    
    A_values = {}
    B_values = {}
    for matrix_type, j, value in values:
        if matrix_type == 'A':
            A_values[j] = value
        else:  # matrix_type == 'B'
            B_values[j] = value
    
    # C[i,k] = sum(A[i,j] * B[j,k])
    result = 0
    for j in range(matrix_dims['rows']):
        if j in A_values and j in B_values:
            result += A_values[j] * B_values[j]
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    output_data = {
        'position': [i, k],
        'value': result
    }
    
    blob = bucket.blob(f'reducer-output/result-{i}-{k}.json')
    blob.upload_from_string(json.dumps(output_data))
    
    return {
        'status': 'success',
        'position': [i, k],
        'value': result
    }