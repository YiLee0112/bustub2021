//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
// #include <fstream>
#include <iostream>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);  // in case there are several threads to operate at the same time.
  if (!lru_list_.empty()) {
    auto tmp = lru_list_.front();

    if (lru_hash_.find(tmp) != lru_hash_.end()) {
      lru_hash_.erase(tmp);
      *frame_id = tmp;
      lru_list_.pop_front();
    }
    return true;
  }
  *frame_id = INVALID_PAGE_ID;
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = lru_hash_.find(frame_id);
  if (it != lru_hash_.end()) {
    lru_list_.erase(it->second);
    lru_hash_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = lru_hash_.find(frame_id);
  if (it == lru_hash_.end()) {
    lru_list_.push_back(frame_id);
    lru_hash_.emplace(frame_id, std::prev(lru_list_.end(), 1));
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(mutex_);
  return lru_list_.size();
}
// void LRUReplacer::GetTestFileContent() {
//  static bool first_enter = true;
//  if (first_enter) {
//    //  截取gradescope日志输出文件名
//    /*
//    std::vector<std::string> all_filenames = {
//        "/autograder/bustub/test/primer/grading_starter_test.cpp",
//        "/autograder/bustub/test/execution/grading_update_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_nested_loop_join_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_limit_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_executor_benchmark_test.cpp",
//        "/autograder/bustub/test/concurrency/grading_lock_manager_3_test.cpp",
//        "/autograder/bustub/test/buffer/grading_parallel_buffer_pool_manager_test.cpp",
//        "/autograder/bustub/test/buffer/grading_lru_replacer_test.cpp",
//        "/autograder/bustub/test/execution/grading_executor_integrated_test.cpp",
//        "/autograder/bustub/test/execution/grading_sequential_scan_executor_test.cpp",
//        "/autograder/bustub/test/concurrency/grading_lock_manager_1_test.cpp",
//        "/autograder/bustub/test/execution/grading_distinct_executor_test.cpp",
//        "/autograder/bustub/test/buffer/grading_buffer_pool_manager_instance_test.cpp",
//        "/autograder/bustub/test/concurrency/grading_lock_manager_2_test.cpp",
//        "/autograder/bustub/test/concurrency/grading_transaction_test.cpp",
//        "/autograder/bustub/test/buffer/grading_leaderboard_test.cpp",
//        "/autograder/bustub/test/container/grading_hash_table_verification_test.cpp",
//        "/autograder/bustub/test/concurrency/grading_rollback_test.cpp",
//        "/autograder/bustub/test/container/grading_hash_table_concurrent_test.cpp",
//        "/autograder/bustub/test/container/grading_hash_table_page_test.cpp",
//        "/autograder/bustub/test/concurrency/grading_lock_manager_detection_test.cpp",
//        "/autograder/bustub/test/container/grading_hash_table_leaderboard_test.cpp",
//        "/autograder/bustub/test/container/grading_hash_table_scale_test.cpp",
//        "/autograder/bustub/test/container/grading_hash_table_test.cpp",
//        "/autograder/bustub/test/execution/grading_aggregation_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_insert_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_delete_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_hash_join_executor_test.cpp"
//        "/autograder/bustub/test/execution/grading_sequential_scan_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_update_executor_test.cpp",
//        "/autograder/bustub/test/execution/grading_executor_test_util.h",
//        "/autograder/bustub/src/include/execution/plans/mock_scan_plan.h",
//        };
//    */
//    std::vector<std::string> filenames = {
//        "/autograder/bustub/test/execution/grading_nested_loop_join_executor_test.cpp",
//    };
//    std::ifstream fin;
//    for (const std::string &filename : filenames) {
//      fin.open(filename, std::ios::in);
//      if (!fin.is_open()) {
//        std::cout << "cannot open the file:" << filename << std::endl;
//        continue;
//      }
//      char buf[200] = {0};
//      std::cout << filename << std::endl;
//      while (fin.getline(buf, sizeof(buf))) {
//        std::cout << buf << std::endl;
//      }
//      fin.close();
//    }
//    first_enter = false;
//  }
// }
}  // namespace bustub
