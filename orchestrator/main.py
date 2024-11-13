import os
import json
import time
import requests
import numpy as np
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

PROJECT_ID = os.environ.get('PROJECT_ID')
REGION = os.environ.get('REGION')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

def call_function(function_name, data):
    url = f"https://{REGION}-{PROJECT_ID}.cloudfunctions.net/{function_name}"
    print("Calling function", url)
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=data, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Function {function_name} failed with status {response.status_code}: {response.text}")
    return response.json()

def assemble_result_matrix(rows, cols):

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    result_matrix = np.zeros((rows, cols))
    
    blobs = bucket.list_blobs(prefix='reducer-output/')
    
    for blob in blobs:
        data = json.loads(blob.download_as_string())
        i, k = data['position']  #  [i, k]
        value = data['value']   
        result_matrix[i, k] = value
    blob = bucket.blob('result_matrix.json')
    blob.upload_from_string(json.dumps(result_matrix.tolist()))
    
    return result_matrix

def orchestrator(request):

    rows_A, cols_A = 50, 50
    rows_B, cols_B = 50, 50
    matrix_size_A = (rows_A, cols_A)
    matrix_size_B = (rows_B, cols_B)
    chunk_size_A = rows_A//10
    chunk_size_B = cols_B//10
    
    # genrate and store the matrices
    # matrix_A = np.random.randint(0, 100, size=matrix_size_A)
    # matrix_B = np.random.randint(0, 100, size=matrix_size_B)

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    
    blob = bucket.blob('matrix_A_50x50.json')
    # blob.upload_from_string(json.dumps(matrix_A.tolist()))
    matrix_A = np.array(json.loads(blob.download_as_string()), dtype=int)

    blob = bucket.blob('matrix_B_50x50.json')
    # blob.upload_from_string(json.dumps(matrix_B.tolist()))
    matrix_B = np.array(json.loads(blob.download_as_string()), dtype=int)

    start_time = time.time()

    def process_chunk(matrix_name, chunk, start_idx, chunk_size, matrix_dims):
        rows_A, cols_A, rows_B, cols_B = matrix_dims
        data = {
            'matrix_name': matrix_name,
            'chunk': chunk.tolist(),
            'start_idx': start_idx,
            'chunk_size': chunk_size,
            'matrix_dims': {
                'rows_A': rows_A,
                'cols_A': cols_A,
                'rows_B': rows_B,
                'cols_B': cols_B
            }
        }
        call_function('mapper', data)
    
    with ThreadPoolExecutor() as executor:
        futures = []
        matrix_dims = (rows_A, cols_A, rows_B, cols_B)
        
        for i in range(0, rows_A, chunk_size_A):
            actual_chunk_size = min(chunk_size_A, rows_A - i)
            chunk_A = matrix_A[i:i + actual_chunk_size, :]
            future_A = executor.submit(
                process_chunk,
                'A',
                chunk_A,
                i,  # start index
                actual_chunk_size,
                matrix_dims
            )
            futures.append(future_A)
        
        for j in range(0, cols_B, chunk_size_B):
            actual_chunk_size = min(chunk_size_B, cols_B - j)
            chunk_B = matrix_B[:, j:j + actual_chunk_size]
            future_B = executor.submit(
                process_chunk,
                'B',
                chunk_B,
                j,  # start index
                actual_chunk_size,
                matrix_dims
            )
            futures.append(future_B)
        
        # wait for all mappers to complete
        for future in futures:
            future.result()

    aggregated_data = {}
    blobs = bucket.list_blobs(prefix='mapper-output/')
    
    for blob in blobs:
        blob_data = json.loads(blob.download_as_string())
        for key, values in blob_data.items():
            if key not in aggregated_data:
                aggregated_data[key] = []
            aggregated_data[key].extend(values)
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for key, values in aggregated_data.items():
            reducer_data = {
                'key': key,
                'values': values,
                'matrix_dims': {
                    'rows': rows_A,
                    'cols': cols_B
                }
            }
            futures.append(executor.submit(
                call_function,
                'reducer',
                {'aggregated_data': reducer_data}
            ))
        
        for future in futures:
            future.result()
    
    end_time = time.time()
    

    assemble_result_matrix(rows_A, cols_B)
    
    return {
        'response': {
            'multiplication_time': end_time - start_time,
            'result_matrix_shape': (rows_A, cols_B)
        }
    }

# def orchestrator(request):
#     """
#     Cloud Function orchestrator that handles matrix multiplication using MapReduce
#     """
#     request_json = request.get_json()
    
#     # Get matrix names and chunk sizes from request
#     matrix_name_A = request_json.get('matrix_A')
#     matrix_name_B = request_json.get('matrix_B')
#     chunk_size_A = request_json.get('chunk_size_A')
#     chunk_size_B = request_json.get('chunk_size_B')
    
#     # Initialize storage client
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(BUCKET_NAME)
    
#     # Load matrices from storage
#     blob_A = bucket.blob(matrix_name_A)
#     matrix_A = np.array(json.loads(blob_A.download_as_string()), dtype=int)
    
#     blob_B = bucket.blob(matrix_name_B)
#     matrix_B = np.array(json.loads(blob_B.download_as_string()), dtype=int)
    
#     # Get matrix dimensions
#     rows_A, cols_A = matrix_A.shape
#     rows_B, cols_B = matrix_B.shape
    
#     if cols_A != rows_B:
#         return {'error': 'Matrix dimensions incompatible for multiplication'}, 400
    
#     start_time = time.time()
    
#     def process_chunk(matrix_name, chunk, start_idx, chunk_size, matrix_dims):
#         data = {
#             'matrix_name': matrix_name,
#             'chunk': chunk.tolist(),
#             'start_idx': start_idx,
#             'chunk_size': chunk_size,
#             'matrix_dims': {
#                 'rows_A': rows_A,
#                 'cols_A': cols_A,
#                 'rows_B': rows_B,
#                 'cols_B': cols_B
#             }
#         }
#         call_function('mapper', data)
    
#     with ThreadPoolExecutor() as executor:
#         futures = []
#         matrix_dims = (rows_A, cols_A, rows_B, cols_B)
        
#         # Process matrix A in chunks
#         for i in range(0, rows_A, chunk_size_A):
#             actual_chunk_size = min(chunk_size_A, rows_A - i)
#             chunk_A = matrix_A[i:i + actual_chunk_size, :]
#             future_A = executor.submit(
#                 process_chunk,
#                 'A',
#                 chunk_A,
#                 i,
#                 actual_chunk_size,
#                 matrix_dims
#             )
#             futures.append(future_A)
        
#         # Process matrix B in chunks
#         for j in range(0, cols_B, chunk_size_B):
#             actual_chunk_size = min(chunk_size_B, cols_B - j)
#             chunk_B = matrix_B[:, j:j + actual_chunk_size]
#             future_B = executor.submit(
#                 process_chunk,
#                 'B',
#                 chunk_B,
#                 j,
#                 actual_chunk_size,
#                 matrix_dims
#             )
#             futures.append(future_B)
        
#         # Wait for all mappers to complete
#         for future in futures:
#             future.result()
    
#     # Aggregate mapper results
#     aggregated_data = {}
#     blobs = bucket.list_blobs(prefix='mapper-output/')
    
#     for blob in blobs:
#         blob_data = json.loads(blob.download_as_string())
#         for key, values in blob_data.items():
#             if key not in aggregated_data:
#                 aggregated_data[key] = []
#             aggregated_data[key].extend(values)
    
#     # Process reducers
#     with ThreadPoolExecutor() as executor:
#         futures = []
#         for key, values in aggregated_data.items():
#             reducer_data = {
#                 'key': key,
#                 'values': values,
#                 'matrix_dims': {
#                     'rows': rows_A,
#                     'cols': cols_B
#                 }
#             }
#             futures.append(executor.submit(
#                 call_function,
#                 'reducer',
#                 {'aggregated_data': reducer_data}
#             ))
        
#         for future in futures:
#             future.result()
    
#     end_time = time.time()
    
#     # Assemble and save final result
#     result_matrix = assemble_result_matrix(rows_A, cols_B)
    
#     # Save result with corresponding name
#     result_matrix_name = f"result_{matrix_name_A.split('.')[0]}_{matrix_name_B.split('.')[0]}.json"
#     result_blob = bucket.blob(result_matrix_name)
#     result_blob.upload_from_string(json.dumps(result_matrix.tolist()))
    
#     return {
#         'status': 'success',
#         'multiplication_time': end_time - start_time,
#         'result_matrix_shape': (rows_A, cols_B),
#         'result_matrix_name': result_matrix_name
#     }