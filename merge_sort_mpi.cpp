#include "ff/pipeline.hpp"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <ff/ff.hpp>
#include <mpi.h>
#include <utility.hpp>
#include <utility>

using namespace ff;

/**
 * @brief Task structure representing a chunk of data to be sorted
 *
 * Contains information about a segment of the record vector that needs
 * to be processed by a worker thread. Uses RAII principles and provides
 * proper ordering for task scheduling.
 */
struct SortTask
{
  std::vector<Record> *record_ptr;///< Pointer to the main record vector
  size_t start;///< Starting index of the chunk
  size_t size;///< Size of the chunk to process
  uint8_t task_id;///< Unique identifier for ordering

  /**
   * @brief Construct a new Sort Task object
   *
   * @param data Pointer to the record vector
   * @param start_idx Starting index for this chunk
   * @param chunk_size Size of the chunk to process
   * @param id Unique task identifier
   */
  SortTask(std::vector<Record> *data, size_t start_idx, size_t chunk_size, uint8_t id) noexcept
    : record_ptr(data), start(start_idx), size(chunk_size), task_id(id)
  {}

  // Disable copy operations to prevent accidental copying
  SortTask(const SortTask &) = delete;
  SortTask &operator=(const SortTask &) = delete;

  // Enable move operations for efficient transfer
  SortTask(SortTask &&) = default;
  SortTask &operator=(SortTask &&) = default;

  /**
   * @brief Comparison operator for task ordering
   *
   * @param other Task to compare with
   * @return true if this task should be processed before other
   */
  bool operator<(const SortTask &other) const noexcept { return task_id < other.task_id; }
};

/**
 * @brief Source node that creates and distributes sorting tasks
 *
 * Implements the emitter pattern by dividing the input data into
 * manageable chunks and creating tasks for parallel processing.
 */
class Source final : public ff_node_t<SortTask>
{
public:
  /**
   * @brief Construct a new Source object
   *
   * @param data_ptr Pointer to the vector containing records to sort
   * @param num_tasks Number of tasks to create for parallel processing
   */
  explicit Source(std::vector<Record> *data_ptr, size_t num_tasks) : record_ptr_(data_ptr), n_tasks_(num_tasks) {}

  /**
   * @brief Service function that creates and sends sorting tasks
   *
   * Divides the input vector into approximately equal chunks and
   * creates SortTask objects for parallel processing.
   *
   * @param task Unused input parameter
   * @return EOS to signal end of stream
   */
  SortTask *svc(SortTask *)
  {
    // Nothing to sort
    if (record_ptr_->empty()) { return EOS; }

    // Measure execution time
    const auto start_time = std::chrono::steady_clock::now();

    // Calculate base chunk size and remainder for even distribution
    const size_t base_chunk_size = record_ptr_->size() / n_tasks_;
    const size_t rest = record_ptr_->size() % n_tasks_;

    // Track current position to ensure contiguous chunks
    size_t start = 0;

    // Create tasks for each chunk
    for (size_t i = 0; i < n_tasks_; ++i) {
      // Distribute the exceeding elements among the first tasks
      size_t final_size = base_chunk_size + (i < rest ? 1 : 0);

      // Create and send task with current chunk information
      ff_send_out(new SortTask(record_ptr_, start, final_size, static_cast<uint8_t>(i)));

      // Move to next chunk position
      start += final_size;
    }

    // Record time spent in emitter
    const auto end_time = std::chrono::steady_clock::now();
    emitter_duration = std::chrono::duration<double>(end_time - start_time);
    return EOS;
  }

private:
  std::vector<Record> *record_ptr_;///< Pointer to the data to be sorted
  size_t n_tasks_;///< Number of tasks to create
};

/**
 * @brief Worker node that sorts individual chunks of data
 *
 * Processes SortTask objects by sorting the specified range of records
 * using the std::sort algorithm.
 */
class Worker final : public ff_node_t<SortTask>
{
  /**
   * @brief Service function that sorts a chunk of data
   *
   * Sorts the range of records specified by the task using std::sort.
   * The sorting is done in-place on the original vector.
   *
   * @param task Pointer to the task containing chunk information
   * @return The same task pointer to pass to the collector
   */
  SortTask *svc(SortTask *task)
  {
    // Sort the specified range using iterators
    std::sort(task->record_ptr->begin() + task->start, task->record_ptr->begin() + task->start + task->size);

    // Return the same task for further processing
    return task;
  }
};

/**
 * @brief Sink node that collects and merges sorted chunks
 *
 * Receives sorted chunks from workers and performs a final merge
 * to produce a completely sorted array. Uses in-place merging
 * to minimize memory usage.
 */
class Sink final : public ff_node_t<SortTask>
{
public:
  /**
   * @brief Construct a new Sink object
   *
   * @param expected_tasks Number of tasks expected to be received
   */
  explicit Sink(size_t num_task) : n_tasks_(num_task)
  {
    // Pre-allocate for efficiency
    results_.reserve(num_task);
  };

  /**
   * @brief Service function that collects and merges sorted chunks
   *
   * Accumulates sorted chunks and performs final merge when all
   * chunks have been received.
   *
   * @param task Pointer to completed sort task
   * @return GO_ON to continue or EOS when merging is complete
   */
  SortTask *svc(SortTask *task)
  {
    // Add completed task to results collection
    results_.emplace_back(task);

    // Continue collecting until all tasks are received
    if (results_.size() != n_tasks_) { return GO_ON; }

    // Measure execution time for merge phase
    const auto start_time = std::chrono::steady_clock::now();

    // Sort by task_id for correct merge order
    std::sort(results_.begin(), results_.end(), [](const SortTask *a, const SortTask *b) noexcept {
      return a->task_id < b->task_id;
    });

    // Get pointer to the record vector from first task
    std::vector<Record> *record_ptr = results_[0]->record_ptr;

    // Perform sequential in-place merge of all sorted chunks
    for (size_t i = 1; i < results_.size(); ++i) {
      std::inplace_merge(record_ptr->begin() + results_[0]->start,// Start of the sequence
        record_ptr->begin() + results_[i - 1]->start + results_[i - 1]->size,// Start of the second sorted range
        record_ptr->begin() + results_[i]->start + results_[i]->size);// End of the second sorted range
    }

    // Clean up allocated tasks
    for (auto *task : results_) { delete task; }

    // Record merge execution time
    const auto end_time = std::chrono::steady_clock::now();
    collector_duration = std::chrono::duration<float>(end_time - start_time);
    return EOS;
  }

private:
  std::vector<SortTask *> results_;///< Collected completed tasks
  size_t n_tasks_;///< Expected number of tasks
};

/**
 * @brief Creates a simple farm with sort workers and single merge phase
 *
 * Implements a basic parallel sort using a farm pattern where:
 * 1. Source emits sort tasks for chunks
 * 2. Workers sort individual chunks in parallel
 * 3. Sink collects and merges all chunks sequentially
 *
 * @param records Reference to the vector to be sorted
 * @return ff_farm Configured farm ready for execution
 */
ff_farm create_simple_farm(std::vector<Record> &records)
{
  // Create components
  auto emitter = new Source(&records, N_THREADS);
  auto collector = new Sink(N_THREADS);

  // Create workers
  std::vector<ff_node *> workers;
  for (size_t i = 0; i < N_THREADS; i++) { workers.push_back(new Worker()); }

  // Configure farm
  ff_farm farm;
  farm.remove_collector();// Remove default collector
  farm.add_emitter(emitter);// Add custom emitter
  farm.add_collector(collector);// Add custom collector

  // Add workers to the farm
  farm.add_workers(workers);

  // Use on-demand scheduling for better load balancing
  farm.set_scheduling_ondemand();

  return farm;
}

/*------------------------------------------- DOUBLE FARM STRATEGY --------------------------------------------------*/
/**
 * @brief Task structure for merge operations in the second farm
 *
 * Contains information about two contiguous sorted ranges that need
 * to be merged together by a merge worker.
 */
struct MergeTask
{
  std::vector<Record> *record_ptr;///< Pointer to the main record vector
  size_t first_start;///< Start of first sorted range
  size_t first_end;///< End of first sorted range
  size_t second_end;///< End of second sorted range
  uint8_t task_id;///< Unique identifier for ordering

  /**
   * @brief Construct a new Merge Task object
   *
   * @param ptr Pointer to the record vector
   * @param start Start index of first range
   * @param end End index of first range
   * @param last_end End index of second range
   * @param id Unique task identifier
   */
  MergeTask(std::vector<Record> *ptr, size_t start, size_t end, size_t last_end, uint8_t id)
    : record_ptr(ptr), first_start(start), first_end(end), second_end(last_end), task_id(id) {};
};

/**
 * @brief Emitter node that converts SortTasks to MergeTasks
 *
 * Receives sorted chunks and pairs them up for parallel merging.
 * Acts as the collector for the first farm and emitter for the second farm.
 */
class MergeEmitter final : public ff_node_t<SortTask, MergeTask>
{
public:
  /**
   * @brief Construct a new Merge Emitter object
   *
   * @param num_tasks Number of sort tasks expected
   */
  explicit MergeEmitter(size_t num_tasks) : expected_tasks_(num_tasks) { results_.reserve(num_tasks); }

  /**
   * @brief Service function that collects sorted chunks and creates merge tasks
   *
   * Accumulates all sorted chunks, then pairs them for parallel merging.
   *
   * @param task Pointer to completed sort task
   * @return GO_ON to continue collecting, or EOS when all merge tasks sent
   */
  MergeTask *svc(SortTask *task)
  {
    // Collect the completed sort task
    results_.emplace_back(task);

    // Wait until all sort tasks are collected
    if (results_.size() != expected_tasks_) { return GO_ON; }

    // Measure execution time for task creation
    const auto start_time = std::chrono::steady_clock::now();

    // Sort by task_id for correct merge order
    std::sort(results_.begin(), results_.end(), [](const SortTask *a, const SortTask *b) noexcept {
      return a->task_id < b->task_id;
    });

    // Generate merge tasks by pairing adjacent sorted chunks
    uint8_t id{ 0 };
    size_t i{ 0 };

    // Pair up adjacent chunks for parallel merging
    for (; i < results_.size() - 1; i += 2) {
      // Create merge task for two adjacent sorted chunks
      ff_send_out(new MergeTask(results_[i]->record_ptr,
        results_[i]->start,// Start of first chunk
        results_[i]->start + results_[i]->size,// End of first chunk
        results_[i + 1]->start + results_[i + 1]->size,// End of second chunk
        id));
      id++;
    }

    // Handle unpaired chunk (if odd number of chunks)
    if (i < results_.size()) {
      // Create a no-op merge task (first_end == second_end means no actual merge)
      ff_send_out(new MergeTask(results_[i]->record_ptr,
        results_[i]->start,// Start of unpaired chunk
        results_[i]->start + results_[i]->size,// End of unpaired chunk
        results_[i]->start + results_[i]->size,// Same as end (no merge needed)
        id));
    }

    // Clean up allocated sort tasks
    for (auto *task : results_) { delete task; }

    // Record time spent creating merge tasks
    const auto end_time = std::chrono::steady_clock::now();
    merge_emitter_duration = std::chrono::duration<float>(end_time - start_time);
    return EOS;
  }

private:
  std::vector<SortTask *> results_;///< Collected sort tasks
  size_t expected_tasks_;///< Expected number of sort tasks
};

/**
 * @brief Worker node that performs parallel merge operations
 *
 * Processes MergeTask objects by merging two contiguous sorted ranges
 * using std::inplace_merge algorithm.
 */
class MergeWorker final : public ff_node_t<MergeTask>
{
  /**
   * @brief Service function that merges two sorted ranges
   *
   * Merges the two sorted ranges specified in the task using std::inplace_merge.
   * The merge is done in-place on the original vector.
   *
   * @param task Pointer to the merge task containing range information
   * @return The same task pointer to pass to the collector
   */
  MergeTask *svc(MergeTask *task)
  {
    // Perform in-place merge of two contiguous sorted ranges
    std::inplace_merge(task->record_ptr->begin() + task->first_start,// Start of first range
      task->record_ptr->begin() + task->first_end,// Start of second range
      task->record_ptr->begin() + task->second_end);// End of second range

    // Return the same task for further processing
    return task;
  }
};

/**
 * @brief Collector node that performs final sequential merge
 *
 * Receives completed merge tasks and performs a final sequential merge
 * to produce the completely sorted array. Handles the remaining merges
 * that couldn't be done in parallel.
 */
class MergeCollector final : public ff_node_t<MergeTask>
{
public:
  /**
   * @brief Construct a new Merge Collector object
   *
   * @param expected_merges Number of merge tasks expected
   */
  explicit MergeCollector(size_t expected_merges) : expected_merges_(expected_merges)
  {
    // Pre-allocate for efficiency
    completed_merges_.reserve(expected_merges);
  }

  /**
   * @brief Service function that collects merge tasks and performs final merge
   *
   * Accumulates completed merge tasks and performs final sequential merge
   * when all parallel merges are complete.
   *
   * @param task Pointer to completed merge task
   * @return GO_ON to continue or EOS when final merge is complete
   */
  MergeTask *svc(MergeTask *task)
  {
    // Collect the completed merge task
    completed_merges_.push_back(task);

    // Wait until all merge tasks are collected
    if (completed_merges_.size() != expected_merges_) { return GO_ON; }

    // Measure execution time for final merge
    const auto start_time = std::chrono::steady_clock::now();

    // Sort completed merges by task_id for correct merge order
    std::sort(completed_merges_.begin(), completed_merges_.end(), [](const MergeTask *a, const MergeTask *b) noexcept {
      return a->task_id < b->task_id;
    });

    // Get pointer to the record vector from first task
    std::vector<Record> *record_ptr = completed_merges_[0]->record_ptr;

    // Perform sequential merge of all the pair-merged chunks
    for (size_t i = 1; i < completed_merges_.size(); ++i) {
      std::inplace_merge(record_ptr->begin() + completed_merges_[0]->first_start,// Start of entire sequence
        record_ptr->begin() + completed_merges_[i - 1]->second_end,// End of previous merged section
        record_ptr->begin() + completed_merges_[i]->second_end);// End of current section
    }

    // Clean up allocated merge tasks
    for (auto *task : completed_merges_) { delete task; }

    // Record final merge execution time
    const auto end_time = std::chrono::steady_clock::now();
    collector_duration = std::chrono::duration<double>(end_time - start_time);
    return EOS;
  }

private:
  std::vector<MergeTask *> completed_merges_;///< Collected completed merge tasks
  size_t expected_merges_;///< Expected number of merge tasks
};

/**
 * @brief Creates a double farm pipeline with parallel sort and merge phases
 *
 * Implements a more sophisticated parallel sort using two farms in pipeline:
 * 1. First farm: Source -> Sort Workers -> MergeEmitter
 * 2. Second farm: MergeWorkers -> MergeCollector
 * This allows both sorting and merging to happen in parallel.
 *
 * @param records Reference to the vector to be sorted
 * @return ff_pipeline Configured pipeline ready for execution
 */
ff_pipeline create_double_farm(std::vector<Record> &records)
{
  // Create emitter for first farm (generates sort tasks)
  auto emitter = new Source(&records, N_THREADS);

  // Create collector/emitter bridge between farms
  auto merge_dispatcher = new MergeEmitter(N_THREADS);

  // Calculate expected number of merge tasks (pairs of chunks + potential odd one)
  uint8_t expected_merges = N_THREADS / 2 + (N_THREADS % 2);
  auto merge_collector = new MergeCollector(expected_merges);

  // Create sort workers for first farm
  std::vector<ff_node *> sort_workers;
  for (size_t i = 0; i < N_THREADS; i++) { sort_workers.push_back(new Worker()); }

  // Create merge workers for second farm (only need as many as merge tasks)
  std::vector<ff_node *> merge_workers;
  for (size_t i = 0; i < expected_merges; i++) { merge_workers.push_back(new MergeWorker()); }

  // Configure first farm (sorting phase)
  ff_farm sort_farm;
  sort_farm.remove_collector();// Remove default collector
  sort_farm.add_emitter(emitter);// Add sort task emitter
  sort_farm.add_workers(sort_workers);// Add sort workers
  sort_farm.add_collector(merge_dispatcher);// Add merge task creator
  sort_farm.set_scheduling_ondemand();// Enable on-demand scheduling

  // Configure second farm (merging phase)
  ff_farm merge_farm;
  merge_farm.remove_collector();// Remove default collector
  merge_farm.add_workers(merge_workers);// Add merge workers
  merge_farm.add_collector(merge_collector);// Add final merge collector
  merge_farm.set_scheduling_ondemand();// Enable on-demand scheduling

  // Create pipeline connecting the two farms
  ff_pipeline pipeline;
  pipeline.add_stage(sort_farm);// First stage: parallel sorting
  pipeline.add_stage(merge_farm);// Second stage: parallel merging

  return pipeline;
}

/*----------------------------------- MPI UTILITY ----------------------------------------*/
/**
 * @brief Calculate data distribution parameters for MPI_Scatterv
 * @param n_proc Number of processes
 * @param array_size Total size of data array
 * @return Pair of vectors containing counts and displacements for each process
 */
std::pair<std::vector<int>, std::vector<int>> calculate_distribution(int n_proc)
{
  std::vector<int> counts(n_proc);
  std::vector<int> displs(n_proc);

  const int base_chunk_size = ARRAY_SIZE / n_proc;
  const int remainder = ARRAY_SIZE % n_proc;
  int start_offset = 0;

  // Distribute data as evenly as possible
  // First 'remainder' processes get one extra element
  for (int i = 0; i < n_proc; ++i) {
    counts[i] = base_chunk_size + (i < remainder ? 1 : 0);
    displs[i] = start_offset;
    start_offset += counts[i];
  }

  return { std::move(counts), std::move(displs) };
}

/**
 * @brief Perform simple gather and merge strategy
 * @param local_vec Local sorted data
 * @param counts Number of elements from each process
 * @param displs Starting positions for each process
 * @param record_t MPI datatype for Record
 * @param rank Current process rank
 * @param n_proc Total number of processes
 */
void simple_gather_merge(std::vector<Record> &local_vec,
  const std::vector<int> &counts,
  const std::vector<int> &displs,
  MPI_Datatype record_t,
  int rank,
  int n_proc)
{
  std::vector<Record> gathered;

  // Only root process needs to allocate space for gathered data
  if (rank == 0) { gathered.resize(ARRAY_SIZE); }

  // Gather all sorted chunks to root process
  MPI_Gatherv(local_vec.data(),
    static_cast<int>(local_vec.size()),
    record_t,
    rank == 0 ? gathered.data() : nullptr,
    counts.data(),
    displs.data(),
    record_t,
    0,
    MPI_COMM_WORLD);

  // Root process performs final merge
  if (rank == 0) {
    // Sequentially merge sorted chunks using inplace_merge
    size_t current_end = counts[0];

    for (int src = 1; src < n_proc; ++src) {
      size_t next_end = current_end + counts[src];

      // Merge already-merged prefix with next sorted chunk
      std::inplace_merge(gathered.begin(), gathered.begin() + current_end, gathered.begin() + next_end);

      current_end = next_end;
    }

    // Update local_vec with final result for root process
    local_vec = std::move(gathered);
  }
}

/**
 * @brief Perform tree-based reduction merge strategy
 * @param local_vec Local sorted data (modified in-place)
 * @param record_t MPI datatype for Record
 * @param rank Current process rank
 * @param levels Number of tree levels (log2 of process count)
 */
void tree_reduction_merge(std::vector<Record> &local_vec, MPI_Datatype record_t, int rank, int levels)
{
  MPI_Status status;

  // Tree reduction
  for (int level = 0; level < levels; ++level) {
    const int step = 1 << level;// 2^level
    const int next_step = 1 << (level + 1);// 2^(level+1)

    // Check which node partecipates to the communication
    if (rank % step == 0) {
      if (rank % next_step != 0) {
        // This process should send its data to the previous node that partecipates to the communication
        const int receiver = rank - step;
        const int size = static_cast<int>(local_vec.size());

        // Send size first, then data
        MPI_Send(&size, 1, MPI_INT, receiver, 99, MPI_COMM_WORLD);
        MPI_Send(local_vec.data(), size, record_t, receiver, 99, MPI_COMM_WORLD);
        break;// This process is done
      } else {
        // This process should receive data from the next node that partecipates to the communication
        const int sender = rank + step;
        int incoming_size;

        // Receive size first
        MPI_Recv(&incoming_size, 1, MPI_INT, sender, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        // Expand local vector to receive incoming data
        const size_t prev_size = local_vec.size();
        local_vec.resize(prev_size + incoming_size);

        // Receive the actual data
        MPI_Recv(local_vec.data() + prev_size, incoming_size, record_t, sender, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        // Merge the two sorted sequences in-place
        std::inplace_merge(local_vec.begin(), local_vec.begin() + prev_size, local_vec.end());
      }
    }
  }
}

/**
 * @brief Main function - entry point of the program
 *
 * Parses command line arguments, generates test data, creates the appropriate
 * parallel sort pipeline, executes it, and prints results.
 *
 * @param argc Number of command line arguments
 * @param argv Array of command line argument strings
 * @return int Exit status
 */
int main(int argc, char *argv[])
{

  int provided, rank, n_proc;
  double elapsed_time;

  // Initialize MPI environment with thread supports
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &n_proc);

  // Initialize the data type to send/recv
  MPI_Datatype record_t;
  MPI_Type_contiguous(sizeof(Record), MPI_BYTE, &record_t);
  MPI_Type_commit(&record_t);

  std::vector<Record> records;

  // Parse command line options (defined in utility.hpp)
  parse_options(argc, argv);

  if (rank == 0) {

    // Generate test data according to parsed options
    generate_random_data(records);
    // Print sample data if requested
    if (PRINT) print_sample(records);
  }

  int tree_levels = static_cast<int>(log2(n_proc));
  // Apply tree startegy only if n_proc is a power of two
  const bool tree_strategy = (tree_levels != log2(n_proc)) ? false : true;

  auto [counts, displs] = calculate_distribution(n_proc);

  // Start timing
  const double t0 = MPI_Wtime();
  std::vector<Record> local_vec(counts[rank]);

  // Distribute the vector among the processors
  MPI_Scatterv(records.data(),
    counts.data(),
    displs.data(),
    record_t,
    local_vec.data(),
    counts[rank],
    record_t,
    0,
    MPI_COMM_WORLD);

// Conditional compilation: choose between simple farm or double farm
#if DOUBLE_FARM
  // Create pipeline with parallel sort and merge
  auto my_pipeline = create_double_farm(local_vec);
#else
  // Create simple farm with sequential final merge
  auto my_pipeline = create_simple_farm(local_vec);
#endif

  // Execute the pipeline and wait for completion
  my_pipeline.run_and_wait_end();

  if (tree_strategy) {
    tree_reduction_merge(local_vec, record_t, rank, tree_levels);
  } else {
    simple_gather_merge(local_vec, counts, displs, record_t, rank, n_proc);
  }


  const double t1 = MPI_Wtime();
  // Single processor time
  const double delta_t = t1 - t0;
  MPI_Barrier(MPI_COMM_WORLD);
  // Retrieve the slower processor
  MPI_Reduce(&delta_t, &elapsed_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);


  if (rank == 0) {
    // Print results and performance metrics
    if (PRINT) print_sample(local_vec);
    std::cout << "elapsed time: " << elapsed_time << "\n";
  }

  MPI_Type_free(&record_t);
  MPI_Finalize();
}
