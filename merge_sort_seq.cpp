#include <chrono>
#include <utility.hpp>

int main(int argc, char *argv[])
{
  ARRAY_SIZE = std::atoi(argv[1]);
  PAYLOAD_SIZE = std::atoi(argv[2]);
  std::vector<Record> records;
  generate_random_data(records);
  if (PRINT) print_sample(records);

  std::chrono::time_point<std::chrono::system_clock> begin = std::chrono::system_clock::now();
  std::sort(records.begin(), records.end());
  std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();

  if (PRINT) print_sample(records);
  std::chrono::duration<double> time = end - begin;

  std::cout << "elapsed time: " << time.count() << "\n";
}
