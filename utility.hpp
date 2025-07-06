#if !defined(UTILITY_HPP)
#define UTILITY_HPP

// Conditional compilation flag for double farm configuration
#if !defined(DOUBLE_FARM)
#define DOUBLE_FARM 0
#endif

// Standard library includes for various utilities
#include <algorithm>// For std::sort, std::shuffle, std::min, std::is_sorted
#include <chrono>// For timing measurements
#include <cstddef>// For size_t
#include <cstdint>// For uint16_t fixed-width integers
#include <cstdlib>// For std::atoi, exit
#include <getopt.h>// For command-line argument parsing
#include <iostream>// For console I/O
#include <random>// For random data generation
#include <unordered_map>// For payload storage
#include <vector>// For dynamic arrays

// Global configuration variables with default values
static size_t ARRAY_SIZE{ 100 };// Number of records to sort
static size_t PAYLOAD_SIZE{ 8 };// Size of payload string for each record
static size_t N_THREADS{ 1 };// Number of FastFlow threads to use
static bool PRINT{ false };// Flag to print sample records
static bool MEASURE{ false };// Flag to print timing measurements

// Constants
const size_t N_SORT_TASK = N_THREADS;
const size_t N_MERGE_TASK = N_THREADS / 2 + (N_THREADS % 2);

// Global timing variables for performance measurement
std::chrono::duration<double> total_duration;// Total execution time
std::chrono::duration<double> emitter_duration;// Time spent in emitter component
std::chrono::duration<double> collector_duration;// Time spent in collector component
std::chrono::duration<double> merge_emitter_duration;// Time for merge emitter (double farm mode)

/**
 * Record structure representing a sortable data element
 * Contains a key for sorting and an index to reference associated payload data
 */
struct Record
{
  uint64_t key;// Sorting key (64-bit unsigned integer)
  uint64_t index;// Index to reference payload in global map

  Record() = default;
  // Constructor to initialize record with key and index
  Record(uint64_t k, uint64_t idx) : key(k), index(idx) {};

  // Comparison operator for sorting - records are sorted by key in ascending order
  bool operator<(const Record &other) const { return key < other.key; }
};

// Global map to store payload strings, indexed by record index
// This simulates having additional data associated with each record
std::unordered_map<uint64_t, std::string> payload_map;

/**
 * Print usage information for the program
 * @param program_name The name of the executable
 */
static inline void print_usage(const char *program_name)
{
  std::cout << "Usage: " << program_name << " -s <array_size> -r <payload_size> -t <num_threads>\n";
  std::cout << "Options:\n";
  std::cout << "  -s <size>     Size of the array to sort\n";
  std::cout << "  -r <size>     Size of the payload (in characters) for each record\n";
  std::cout << "  -t <threads>  Number of FastFlow threads to use\n";
  std::cout << "  -p            Print samples of the records\n";
  std::cout << "  -m            Print measurements of all the components\n";
  std::cout << "  -h            Help\n";
}

/**
 * Print a sample of records for debugging/verification purposes
 * @param records Vector of records to sample from
 * @param count Number of records to print (default: 10)
 */
void print_sample(const std::vector<Record> &records, int count = 10)
{
  std::cout << "Sample records (first " << count << "):\n";
  for (int i = 0; i < std::min(count, (int)records.size()); i++) {
    std::cout << "Record " << i << ": key=" << records[i].key << ", index=" << records[i].index << ", payload=\""
              << payload_map[records[i].index].substr(0, 20) << (payload_map[records[i].index].size() > 20 ? "..." : "")
              << "\"\n";
  }
}

/**
 * Parse command-line options and set global configuration variables
 * @param argc Argument count
 * @param argv Argument vector
 */
void parse_options(int argc, char *argv[])
{
  long opt = 1;
  // Process command-line options using getopt
  while ((opt = getopt(argc, argv, "s:r:t:pmh")) != -1) {
    switch (opt) {
    case 's':// Array size option
      ARRAY_SIZE = std::atoi(optarg);
      break;
    case 'r':// Payload size option
      PAYLOAD_SIZE = std::atoi(optarg);
      break;
    case 't':// Thread count option
      N_THREADS = std::atoi(optarg);
      break;
    case 'p':// Print samples flag
      PRINT = true;
      break;
    case 'm':// Measure performance flag
      MEASURE = true;
      break;
    case 'h':// Help option
    default:// Invalid option
      print_usage(argv[0]);
      exit(0);
    }
  }
}

/**
 * Generate random test data for sorting benchmarks
 * Creates records with unique shuffled keys and random payload strings
 * @param records Vector to populate with generated records
 */
void generate_random_data(std::vector<Record> &records)
{
  // Initialize random number generator
  std::random_device rd;
  std::mt19937 gen(rd());

  // Prepare containers
  records.reserve(ARRAY_SIZE);
  payload_map.clear();

  // Generate sequential unique keys (1 to ARRAY_SIZE)
  std::vector<uint64_t> unique_keys;
  unique_keys.reserve(ARRAY_SIZE);
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    unique_keys.push_back(i + 1);// Keys from 1 to ARRAY_SIZE
  }

  // Shuffle the keys to create random ordering
  std::shuffle(unique_keys.begin(), unique_keys.end(), gen);

  // Character set for generating random payload strings
  std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<> char_dist(0, chars.size() - 1);

  // Create records with shuffled keys and random payloads
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    // Create record with shuffled key and sequential index
    records.emplace_back(unique_keys[i], i);

    // Generate random payload string for this record
    std::string payload;
    payload.reserve(PAYLOAD_SIZE);
    for (size_t j = 0; j < PAYLOAD_SIZE; ++j) { payload += chars[char_dist(gen)]; }
    // Store payload in global map using record index as key
    payload_map[i] = payload;
  }
}

/**
 * Print results and performance measurements after sorting
 * @param records The sorted vector of records
 */
void print_results(const std::vector<Record> &records)
{
  // Print sample records if requested
  if (PRINT) print_sample(records);

  // Print detailed timing measurements if requested
  if (MEASURE) {
    std::cout << "emitter time: " << emitter_duration.count() << "\n";
#if DOUBLE_FARM
    // Additional timing for double farm configuration
    std::cout << "merge emitter time: " << merge_emitter_duration.count() << "\n";
#endif
    std::cout << "collector time: " << collector_duration.count() << "\n";
  }

  // Always print total elapsed time
  std::cout << "elapsed time: " << total_duration.count() << "\n";

  // Verify sorting correctness if printing is enabled
  if (PRINT && !std::is_sorted(records.begin(), records.end())) { std::cout << "Error: not sorted" << "\n"; }
}

#endif
