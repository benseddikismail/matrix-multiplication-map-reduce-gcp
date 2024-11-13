from flask import Flask, request, jsonify
import json
import numpy as np
from google.cloud import storage
import time

app = Flask(__name__)
storage_client = storage.Client()

@app.route('/multiply', methods=['POST'])
def handle_multiplication():
    input_data = request.get_json()
    
    matrix_name_A = input_data['matrix_A']
    matrix_name_B = input_data['matrix_B']
    
    matrix_A = fetch_matrix_from_bucket(matrix_name_A)
    matrix_B = fetch_matrix_from_bucket(matrix_name_B)
    
    start_time = time.time()
    result_matrix = multiply_matrices(matrix_A, matrix_B)
    end_time = time.time()
    
    result_matrix_name = f"result_{matrix_name_A.split('.')[0]}_{matrix_name_B.split('.')[0]}.json"
    result_matrix_blob = storage_client.bucket('matrices-bucket').blob(result_matrix_name)
    result_matrix_blob.upload_from_string(json.dumps(result_matrix.tolist()))
    
    return jsonify({
        'computation_time': end_time - start_time,
        'result_matrix_name': result_matrix_name
    })

def fetch_matrix_from_bucket(matrix_name, bucket_name='matrices-bucket'):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(matrix_name)
    matrix_data = json.loads(blob.download_as_text())
    return np.array(matrix_data)

def multiply_matrices(matrix_A, matrix_B):
    return np.dot(matrix_A, matrix_B)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)