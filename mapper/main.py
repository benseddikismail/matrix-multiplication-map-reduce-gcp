import os
import json
from google.cloud import storage

BUCKET_NAME = os.environ.get('BUCKET_NAME')

def mapper(request):

    data = json.loads(request.data)
    
    matrix_name = data.get('matrix_name')
    chunk = data.get('chunk')
    start_idx = data.get('start_idx')
    chunk_size = data.get('chunk_size')
    matrix_dims = data.get('matrix_dims')
    
    rows_A = matrix_dims['rows_A']
    cols_A = matrix_dims['cols_A']
    rows_B = matrix_dims['rows_B']
    cols_B = matrix_dims['cols_B']
    
    output = {}
    
    if matrix_name == 'A':

        for i in range(chunk_size):
            for k in range(cols_B):
                key = f"{start_idx + i},{k}"
                if key not in output:
                    output[key] = []
                for j in range(cols_A):
                    output[key].append(('A', j, chunk[i][j]))
    else:

        for i in range(rows_A):
            for k in range(chunk_size):
                key = f"{i},{start_idx + k}"
                if key not in output:
                    output[key] = []
                for j in range(rows_B):
                    output[key].append(('B', j, chunk[j][k]))
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f'mapper-output/mapper-output-{matrix_name}-{start_idx}.json')
    blob.upload_from_string(json.dumps(output))
    
    return {'status': 'success', 'count': len(output)}


