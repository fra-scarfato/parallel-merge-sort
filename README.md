# Parallel Merge Sort with FastFlow and OpenMPI

## Overview

This project implements a high-performance parallel merge sort algorithm that combines FastFlow's task-parallel programming model with OpenMPI's distributed communication capabilities. The implementation provides scalable sorting across both multi-core and multi-node environments, leveraging intra-process parallelism through FastFlow and inter-process communication through MPI.

## Objectives

The primary goal is to accelerate merge sort performance by:
- **Intra-process parallelism**: Using FastFlow's farm patterns for efficient multi-core sorting
- **Inter-process parallelism**: Distributing data across multiple nodes using MPI
- **Hybrid approach**: Combining both paradigms for maximum scalability
- **Load balancing**: Ensuring efficient work distribution across cores and nodes

## Architecture

### Two-Tier Implementation Strategy

The project consists of two complementary implementations:

1. **FastFlow-only version**: Focuses on single-node multi-core parallelization
2. **Hybrid MPI+FastFlow version**: Extends parallelization across multiple nodes

## FastFlow Implementation

### Strategy 1: Simple Farm Pattern

The Simple Farm Strategy uses a single farm pattern with three components:

```
Input Array → [Emitter] → [Worker 1] → [Collector] → Sorted Array
                      ↘ [Worker 2] ↗
                      ↘ [Worker n] ↗
```

**Components:**
- **Emitter**: Splits input array into n sub-arrays and distributes to workers
- **Workers**: Sort assigned sub-arrays in parallel using sequential merge sort
- **Collector**: Gathers all sorted sub-arrays and performs final sequential merge

**Characteristics:**
- Simple implementation with minimal overhead
- Parallel sorting phase, sequential merging phase
- Suitable for moderate-sized datasets

### Strategy 2: Double Farm Pattern

The Double Farm Strategy introduces a second farm stage for parallel merging:

```
Phase 1: Sorting Farm
Input → [Emitter] → [Worker 1] → [Collector/Emitter] 
                  ↘ [Worker 2] ↗              ↓
                  ↘ [Worker n] ↗              ↓
                                              ↓
Phase 2: Merging Farm                         ↓
        [Emitter] → [Worker 1] → [Collector] → Sorted Array
                  ↘ [Worker 2] ↗
                  ↘ [Worker n] ↗
```

**Pipeline Structure:**
- **First Farm**: Parallel sorting of sub-arrays
- **Second Farm**: Parallel merging of sorted chunks
- **Final Collector**: Sequential merge of remaining sequences (if needed)

**Advantages:**
- Parallelizes both sorting and merging phases
- Better scalability for large datasets
- Reduces bottleneck in the merging phase

## MPI with FastFlow Implementation

### Data Distribution Strategy

The hybrid implementation uses MPI for inter-node communication:

```
Master Node: [Original Array]
     ↓ MPI_Scatterv (balanced distribution)
[Node 1] [Node 2] [Node 3] ... [Node n]
     ↓ FastFlow local sorting
[Sorted 1] [Sorted 2] [Sorted 3] ... [Sorted n]
     ↓ MPI merging strategies
[Final Sorted Array]
```

**Key Features:**
- **MPI_Scatterv**: Ensures balanced load distribution even for non-uniform array sizes
- **Local FastFlow sorting**: Each process uses FastFlow for intra-process parallelism
- **Adaptive merging**: Different strategies based on process count

### Merging Strategies

#### Sequential Merge (Non-Power-of-Two Processes)

When the number of MPI processes is not a power of two:

```
Process 0: [Sorted Chunk 0] ←─ MPI_Gatherv ──┐
Process 1: [Sorted Chunk 1] ──────────────────┤
Process 2: [Sorted Chunk 2] ──────────────────┤
Process 3: [Sorted Chunk 3] ──────────────────┘
                ↓
Process 0: Sequential merge of all chunks
```

**Characteristics:**
- Simple implementation using MPI_Gatherv
- All sorted chunks gathered at root process
- Sequential merge at master node
- Suitable for small number of processes

#### Tree-Based Merge (Power-of-Two Processes)

When the number of processes is a power of two:

```
Level 0: [P0] [P1] [P2] [P3] [P4] [P5] [P6] [P7]
           ↓   ↓   ↓   ↓   ↓   ↓   ↓   ↓
Level 1: [P0] ←─┘ [P2] ←─┘ [P4] ←─┘ [P6] ←─┘
           ↓       ↓       ↓       ↓
Level 2: [P0] ←───┘     [P4] ←───┘
           ↓             ↓
Level 3: [P0] ←─────────┘
```

**Algorithm:**
- **Levels**: log₂(n) merge levels where n is the number of processes
- **Pairing**: Processes grouped into pairs at each level
- **Communication**: One process sends, the other receives and merges
- **Reduction**: Number of active processes halves at each level

**Advantages:**
- Distributed merging reduces communication overhead
- Logarithmic complexity in number of processes
- Maintains load balance throughout merge phases

## Report
The report with implementation details and results can be found [here](merge-sort-report.pdf).
