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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutex); // in case there are several threads to operate at the same time.
  if( !LRUList.empty()){
    auto tmp = LRUList.front();

    if(LRUHash.find(tmp) != LRUHash.end()){
      LRUHash.erase(tmp);
      *frame_id = tmp;
      LRUList.pop_front();
    }
    return true;
  }
  *frame_id =INVALID_PAGE_ID;
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex);
  auto it = LRUHash.find(frame_id);
  if(it != LRUHash.end()){
    LRUList.erase(it->second);
    LRUHash.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex);
  auto it = LRUHash.find(frame_id);
  if(it == LRUHash.end()){
    LRUList.push_back(frame_id);
    LRUHash.emplace(frame_id, std::prev(LRUList.end(), 1));
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(mutex);
  return  LRUList.size();
}

}  // namespace bustub
