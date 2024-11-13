import numpy as np
import time
import requests
from google.cloud import storage
import json
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor


storage_client = storage.Client()


BUCKET_NAME = 'matrices-bucket'
VM_URL = 'http://34.71.214.58:8080/multiply'
ORCHESTRATOR_URL = 'https://us-central1-ismail-benseddik-fall2024.cloudfunctions.net/orchestrator'

def clear_output_folders(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    n
    def delete_blob(blob):
        blob.delete()
    
    with ThreadPoolExecutor() as executor:

        blobs_mapper = list(bucket.list_blobs(prefix='mapper-output/'))
        executor.map(delete_blob, blobs_mapper)
        
        blobs_reducer = list(bucket.list_blobs(prefix='reducer-output/'))
        executor.map(delete_blob, blobs_reducer)
    
    print("Cleared output folders in the cloud bucket.")

def upload_matrix(matrix, matrix_name, bucket_name=BUCKET_NAME):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(matrix_name)

    matrix_data = json.dumps(matrix.tolist(), separators=(',', ':'))
    blob.upload_from_string(matrix_data)
    return matrix_name

def generate_matrix(rows, cols):
    return np.random.randint(0, 100, size=(rows, cols)).astype(np.float32)

def call_vm_script(matrix_name_A, matrix_name_B):
    data = {
        'matrix_A': matrix_name_A,
        'matrix_B': matrix_name_B,
    }
    try:
        response = requests.post(VM_URL, json=data, timeout=300) 
        response.raise_for_status()
        result = response.json()
        return result['computation_time']
    except requests.exceptions.RequestException as e:
        print(f"Error calling VM script: {e}")
        return None

def call_mapreduce_orchestrator(matrix_name_A, matrix_name_B, chunk_size_A, chunk_size_B):
    data = {
        'matrix_A': matrix_name_A,
        'matrix_B': matrix_name_B,
        'chunk_size_A': chunk_size_A,
        'chunk_size_B': chunk_size_B
    }
    try:
        response = requests.post(ORCHESTRATOR_URL, json=data, timeout=300)
        response.raise_for_status()
        result = response.json()
        return result['multiplication_time']
    except requests.exceptions.RequestException as e:
        print(f"Error calling orchestrator: {e}")
        return None

def run_performance_test():
    matrix_sizes = [
        (4, 4),
        (10, 10),
        (50, 50), 
        (100, 100),  
        (200, 200)
    ]
    
    results = {
        'sizes': [],
        'vm_times': [],
        'mapreduce_times': [],
        'vm_errors': [],
        'mapreduce_errors': []
    }
    
    for rows, cols in matrix_sizes:
        print(f"\nTesting {rows}x{cols} matrices...")
        
        matrix_A = generate_matrix(rows, cols)
        matrix_B = generate_matrix(cols, rows)
        
        matrix_name_A = f'matrix_A_{rows}x{cols}.json'
        matrix_name_B = f'matrix_B_{cols}x{rows}.json'
        
        upload_matrix(matrix_A, matrix_name_A)
        upload_matrix(matrix_B, matrix_name_B)
        
        clear_output_folders(BUCKET_NAME)
        
        vm_time = call_vm_script(matrix_name_A, matrix_name_B)
        
        chunk_size_A = max(rows // 10, 2)
        chunk_size_B = max(cols // 10, 2)


        mapreduce_time = call_mapreduce_orchestrator(
            matrix_name_A, matrix_name_B,
            chunk_size_A, chunk_size_B
        )
        
        results['sizes'].append(f"{rows}x{cols}")
        results['vm_times'].append(vm_time if vm_time is not None else 0)
        results['mapreduce_times'].append(mapreduce_time if mapreduce_time is not None else 0)
        results['vm_errors'].append(vm_time is None)
        results['mapreduce_errors'].append(mapreduce_time is None)
        
        print(f"VM time: {vm_time:.2f}s")
        print(f"MapReduce time: {mapreduce_time:.2f}s")
    
    return results

def plot_results(results):
    plt.figure(figsize=(12, 7))
    
    plt.plot(results['sizes'], results['vm_times'], 
             label='VM Instance (NumPy)', marker='o')
    plt.plot(results['sizes'], results['mapreduce_times'], 
             label='MapReduce', marker='s')
    
    for i, (vm_err, mr_err) in enumerate(zip(results['vm_errors'], 
                                            results['mapreduce_errors'])):
        if vm_err:
            plt.plot(i, 0, 'rx', markersize=10, label='VM Error')
        if mr_err:
            plt.plot(i, 0, 'kx', markersize=10, label='MapReduce Error')
    
    plt.xlabel('Matrix Size')
    plt.ylabel('Time (seconds)')
    plt.title('Matrix Multiplication Performance Comparison')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)

    for i, (vm_time, mr_time) in enumerate(zip(results['vm_times'], 
                                              results['mapreduce_times'])):
        if vm_time > 0:
            plt.text(i, vm_time, f'{vm_time:.1f}s', 
                    horizontalalignment='left', verticalalignment='bottom')
        if mr_time > 0:
            plt.text(i, mr_time, f'{mr_time:.1f}s', 
                    horizontalalignment='right', verticalalignment='top')
    
    plt.tight_layout()
    plt.savefig('performance_comparison.png')
    plt.show()

if __name__ == "__main__":
    results = run_performance_test()
    plot_results(results)