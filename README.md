# Matrix Multiplication - MapReduce
Ongoing research focuses on optimizing matrix multiplication algorithms due to their central role
in numerous applications, notably in machine learning. For large matrices, map-reduce is a suitable
paradigm to take on their multiplication. For large matrices, the map-reduce paradigm is an effective
approach for performing their multiplication.
To decompose the problem to fit the map-reduce approach, the initial idea was to split each input
matrix into sub-matrices, then, multiply the sub-matrices, and at end, assemble the resulting submatrices
into one matrix. Though simple, this algorithm does not seem to follow the traditional
map-reduce paradigm. So, the solution maps each row and column elements of the input matrices to
a list of corresponding matrix cell values. Then, values from each matrix corresponding to the same appropriate positions get multiplied and summed up. The algorithm unfolds as follows:

Let $$A$$  and $$B$$ be the matrices to be multiplied.
Let $$i$$ represent the indices of the rows of $$A$$, $$j$$: columns of $$A$$ as well as rows of $$B$$, $$k$$: columns of $$B$$.

1. **Map**
   
For $$A$$, map key ($$i$$, $$k$$) to ($$A$$, $$j$$, $$A_{ij}$$ ).
For $$B$$, map key ($$i$$, $$k$$) to ($$B$$, $$j$$, $$B_{jk}$$).

3. **Group by**
   
Group the output of mappers by key. For example, (0, 0): [($$A$$, 0, 25), ($$B$$, 1, 63),...]

5. **Reduce**
   
For key ($$i$$, $$k$$), multiply values $$A_{ij}$$ and $$B_{jk}$$ sharing the same $$j$$, then sum everything up.
The result is the value at position ($$i$$, $$k$$) of the resulting matrix.

## Implementation
### Orchestrator
To streamline the map-reduce matrix multiplication process, a third orchestrator cloud function is created. It takes care of loading the input matrices and splitting them into chunks to, then, feed them to mappers. Each mapper is fed a chunk of either matrix A or B. Then, after all mappers are done, the orchestrator takes care of aggregating the output of the mappers by key. Each, thereafter, takes in a batch of lists grouped by key. When all reducers are done, the orchestrator assembles the resulting matrix by filling up each of its cell positions. The orchestrator triggers mappers and reducers using http requests.
### Mapper
Each mapper takes in a chunk of either matrix $$A$$ or matrix $$B$$. It iterates over the rows of A and the columns of $$B$$, then, it maps each key ($$i$$, $$k$$) to ($$A$$, $$j$$, $$A_{ij}$$) for $$A$$ and ($$B$$, $$j$$, $$B_{jk}$$) for $$B$$. Each mapper stores the results for each chunk for either matrix $$A$$ or $$B$$ in a cloud bucket.
### Reducer
Based on the data grouped by key, the reducer basically performs matrix multiplication on submatrices.
It multiplies the values $$A_{ij}$$ and $$B_{jk}$$ with the same $$j$$, then, sums the resulting values to obtain the value at position ($$i$$, $$k$$) of the output matrix. Values ($$i$$, $$k$$) of the output matrix are individually stored in the cloud bucket, to be eventually constructed into one resulting matrix by the orchestrator.
## Performance and Testing
### Optimizations
To optimize the map-reduce solution, mappers and reducers are called in parallel using Thread-
PoolExecutor. Chunks of matrices A and B are sent in parallel to each mapper. Similarly, multiple
reducer calls are launched in parallel. This helps reduce the time required for processing by leveraging concurrency. Another effective optimization technique is the use of smaller matrix chunk sizes that can fit into memory. By doing this, the chunks can be directly passed to the mappers and reducers, rather than first storing them in a cloud bucket. This approach reduces the overhead associated with reading and writing from cloud storage, as the mappers and reducers can immediately process the data they receive. As a result, the computation becomes faster and more efficient, especially for large scale matrix multiplications where frequent storage access could otherwise introduce significant latency.
### Performance
To test the performance of the solution, a test script under the test/ directory evaluates the time it takes to multiply two matrices of increasing sizes (from 4 x 4 to 200 x 200). For reference, a script running on a VM instance is used to perform matrix multiplication using numpy on the same matrices. The performance of both solutions can be summarized in the following figure.

<img width="712" alt="detection_ss" src="https://github.com/benseddikismail/matrix-multiplication-map-reduce-gcp/blob/main/test/performance_comparison.png">

The matrix multiplication solution on the VM instance significantly outperforms the MapReduce
approach, highlighting several key factors that contribute to this performance disparity. Primarily, the efficiency of NumPy in matrix multiplication plays a crucial role, leveraging highly optimized algorithms and low-level CPU optimizations for rapid computations.

The MapReduce solution introduces substantial overhead in various aspects of its distributed architecture. This includes computational overhead from data serialization, task distribution, and result aggregation, as well as latency issues stemming from frequent interactions with cloud storage and network communications between components. The distributed nature of MapReduce, while offering parallelism, incurs costs in coordination and data movement that outweigh its benefits for the matrix sizes tested. Additionally, factors such as function cold starts in Cloud Functions and the lack of data locality further impact the MapReduce performance. Despite the parallel execution of mappers and reducers, these combined factors result in longer execution times compared to the streamlined, optimized approach on the VM. This outcome demonstrates that for tasks involving intensive numerical computations on moderately sized data, a well-optimized single-machine solution can surpass a distributed approach, especially when the latter involves high communication and coordination costs. However, it’s important to note that for extremely large matrices or massive datasets exceeding single-machine capabilities, the MapReduce approach could potentially become more advantageous.

One notable observation from the performance analysis is that the relative efficiency gap between
the VM and MapReduce implementations remains fairly consistent as matrix sizes increase. While
both approaches show increased computation time with larger matrices, the overhead costs in the
MapReduce solution don’t proportionally decrease with scale. This highlights one of the fundamental limitations of MapReduce: its distributed computing advantages only become apparent when processing extremely large datasets that justify the inherent overhead costs of distribution, coordination, and data movement. For the tested matrix sizes (up to 200x200), these overhead costs continue to dominate the actual computation time, suggesting that the break-even point where MapReduce becomes more efficient than a single-machine solution lies beyond these dimensions. This characteristic aligns with MapReduce’s original design philosophy of handling massive-scale data processing rather than optimizing smaller, computation-intensive tasks.
## Conclusion and Future Work
The performance evaluation of matrix multiplication implementations revealed significant advantages of the VM-based NumPy solution over the MapReduce approach for the tested matrix sizes. The distributed nature of MapReduce, while powerful for large-scale data processing, introduced considerable overhead through storage access, network communication, and task coordination, ultimately impacting its performance compared to the optimized single-machine solution. Future work could focus on several improvements to enhance the MapReduce implementation’s efficiency. This includes leveraging NumPy’s optimized operations within the mapper and reducer functions, streamlining the data flow between components, and implementing a more balanced distribution of mappers and reducers. Additionally, incorporating a publisher/subscriber pattern could improve the orchestration of the distributed computation by providing real-time notifications of task completion. These enhancements, combined with further optimization of the current implementation, could potentially narrow the performance gap with the VM solution while maintaining the scalability benefits of the distributed approach.





