#include "ff/pipeline.hpp"
#include <algorithm>
#include <cstdint>
#include <ff/ff.hpp>
#include <utility.hpp>

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
  // Parse command line options (defined in utility.hpp)
  parse_options(argc, argv);

  // Generate test data according to parsed options
  std::vector<Record> records;
  generate_random_data(records);

  // Print sample data if requested
  if (PRINT) print_sample(records);

// Conditional compilation: choose between simple farm or double farm
#if DOUBLE_FARM
  // Create pipeline with parallel sort and merge
  auto my_pipeline = create_double_farm(records);
#else
  // Create simple farm with sequential final merge
  auto my_pipeline = create_simple_farm(records);
#endif

  // Measure total execution time
  const auto start_time = std::chrono::steady_clock::now();

  // Execute the pipeline and wait for completion
  my_pipeline.run_and_wait_end();

  // Record total execution time
  const auto end_time = std::chrono::steady_clock::now();
  total_duration = std::chrono::duration<double>(end_time - start_time);

  // Print results and performance metrics
  print_results(records);
}
