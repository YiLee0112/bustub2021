//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <string>
#include <utility>
#include <vector>
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // init directory page
  auto dir_page = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page_data = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  // dir_page_data->IncrGlobalDepth();// set global depth to 1
  dir_page_data->SetPageId(directory_page_id_);

  // set local depth to 1, init 2 bucket page
  page_id_t bucketpageid0;
  buffer_pool_manager_->NewPage(&bucketpageid0);
  dir_page_data->SetBucketPageId(0, bucketpageid0);
  // dir_page_data->SetLocalDepth(0, 1);

  // Unpin these pages
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucketpageid0, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> page_id_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  auto dir_page_data = FetchDirectoryPage();
  table_latch_.RLock();
  // table lock
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  HASH_TABLE_BUCKET_TYPE *bucket_page_data = FetchBucketPage(bucket_page_id);
  Page *bucket_page = reinterpret_cast<Page *>(bucket_page_data);
  // page latch
  bucket_page->RLatch();
  auto res = bucket_page_data->GetValue(key, comparator_, result);
  // Unpin pages
  bucket_page->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);  // TODO(liyi) :?
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto dir_page_data = FetchDirectoryPage();
  table_latch_.RLock();
  // table lock
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  HASH_TABLE_BUCKET_TYPE *bucket_page_data = FetchBucketPage(bucket_page_id);
  Page *bucket_page = reinterpret_cast<Page *>(bucket_page_data);
  // page latch
  bucket_page->WLatch();
  // if needing splite
  if (bucket_page_data->IsFull()) {
    bucket_page->WUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return SplitInsert(transaction, key, value);
  }
  // otherwise
  bool res = bucket_page_data->Insert(key, value, comparator_);
  bucket_page->WUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(bucket_page_id, res);  // is_dirty is desided
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  /*
   * 1. increase the gloabl depth if needed
   * 2. create a new bucket page
   * 3. increase the local depths for both
   * 4. redirect the dir pointer to the new bucket
   * 5. traverse all the existing pairs and try to split the bucket
   * 6.1 if the bucket is still full and the insertion fails, try to split once again
   * 6.2 if the split succeeds, then do the insertion accordingly
   */
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  while (true) {
    page_id_t bucket_page_id = KeyToPageId(key, dir_page);
    uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
    HASH_TABLE_BUCKET_TYPE *bucket_page_data = FetchBucketPage(bucket_page_id);
    if (bucket_page_data->IsFull()) {
      // if bucket is full, it needs to split
      uint32_t global_depth = dir_page->GetGlobalDepth();
      uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
      // new page
      page_id_t new_bucket_id = 0;
      HASH_TABLE_BUCKET_TYPE *new_bucket =
          reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_id));
      // set globle depth++ if needed
      if (global_depth == local_depth) {
        // bucket splits and dir_page extands
        // if i == ij, extand the bucket_page_data dir, and split the bucket_page_data
        uint32_t bucket_num = 1 << global_depth;
        // update dir_page's idx
        for (uint32_t i = 0; i < bucket_num; i++) {
          dir_page->SetBucketPageId(i + bucket_num, dir_page->GetBucketPageId(i));
          dir_page->SetLocalDepth(i + bucket_num, dir_page->GetLocalDepth(i));
        }
        dir_page->IncrGlobalDepth();
        dir_page->SetBucketPageId(bucket_idx + bucket_num, new_bucket_id);
        dir_page->IncrLocalDepth(bucket_idx);
        dir_page->IncrLocalDepth(bucket_idx + bucket_num);
        // global_depth++;
      } else {
        // if i > ij, split the bucket_page_data
        // more than one records point to the bucket_page_data
        // the records' low ij bits are same
        // and the high (i - ij) bits are index of the records point to the same bucket_page_data
        /*
        uint32_t mask = (1 << local_depth) - 1;
        uint32_t base_idx = mask & bucket_idx;
        uint32_t records_num = 1 << (global_depth - local_depth - 1);// + high bits
        uint32_t step = (1 << local_depth);
        uint32_t idx = base_idx;
        // divide base_idex which has the same key into two parts

        for (uint32_t i = 0; i < records_num; i++) {
          dir_page->IncrLocalDepth(idx);
          idx += step * 2;
        }
        idx = base_idx + step;
        for (uint32_t i = 0; i < records_num; i++) {
          dir_page->SetBucketPageId(idx, new_bucket_id);
          dir_page->IncrLocalDepth(idx);
          idx += step * 2;
        }*/
        uint32_t mask = (1 << (local_depth + 1)) - 1;  // merge mask
        uint32_t new_idx = mask & bucket_idx;
        uint32_t high_bite = dir_page->GetLocalHighBit(bucket_idx);
        for (uint32_t i = 0; i < dir_page->Size(); i++) {
          if ((i & mask) == new_idx) {
            dir_page->IncrLocalDepth(i);
          } else if ((i & mask) == (high_bite ^ new_idx)) {
            dir_page->SetBucketPageId(i, new_bucket_id);
            dir_page->IncrLocalDepth(i);
          }
        }
      }
      // dir_page->PrintDirectory();
      // because dir page idx changed before, so move buckets with idx
      // rehash all records in bucket_page_data j
      for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
        KeyType j_key = bucket_page_data->KeyAt(i);
        ValueType j_value = bucket_page_data->ValueAt(i);
        bucket_page_data->RemoveAt(i);
        if (KeyToPageId(j_key, dir_page) == bucket_page_id) {
          // in the same bucket
          bucket_page_data->Insert(j_key, j_value, comparator_);
        } else {
          // move to new bucket
          new_bucket->Insert(j_key, j_value, comparator_);
        }
      }
      // std::cout<<"original bucket_page_data size = "<<bucket_page_data->NumReadable()<<std::endl;
      // std::cout<<"new bucket_page_data size = "<<new_bucket->NumReadable()<<std::endl;
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      buffer_pool_manager_->UnpinPage(new_bucket_id, true);
    } else {
      bool ret = bucket_page_data->Insert(key, value, comparator_);
      table_latch_.WUnlock();
      // std::cout<<"find the unfull bucket_page_data"<<std::endl;
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      return ret;
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto dir_page_data = FetchDirectoryPage();
  table_latch_.RLock();
  // table lock
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page_data);
  HASH_TABLE_BUCKET_TYPE *bucket_page_data = FetchBucketPage(bucket_page_id);
  Page *bucket_page = reinterpret_cast<Page *>(bucket_page_data);
  // page latch
  bucket_page->WLatch();
  // remove first
  bool res = bucket_page_data->Remove(key, value, comparator_);
  bucket_page->WUnlatch();
  // if it turned into empty, merge is needed
  if (bucket_page_data->IsEmpty() && dir_page_data->GetLocalDepth(bucket_idx) != 0) {
    table_latch_.RUnlock();
    Merge(transaction, key, value);
  } else {
    // if it doesn't need to merge
    table_latch_.RUnlock();
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);  // is_dirty is decided
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  // bool is_glowing = false;
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
    // uint32_t global_depth = dir_page->GetGlobalDepth();
    // How to find the bucket to Merge?
    // Answer: After Merge, the records, which pointed to the Merged Bucket,
    // have low (local_depth - 1) bits same
    // therefore, reverse the low local_depth can get the idx point to the bucket to Merge
    uint32_t merged_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));
    page_id_t merged_page_id = dir_page->GetBucketPageId(merged_bucket_idx);
    // HASH_TABLE_BUCKET_TYPE *merged_bucket = FetchBucketPage(merged_page_id);
    if (dir_page->GetLocalDepth(merged_bucket_idx) == local_depth) {
      // check merge page's merge page is empty or not.
      // if it is empty, let merge page merge in loop , then merge with this page last
      // because the first merge page must be not empty
      dir_page->DecrLocalDepth(merged_bucket_idx);
      uint32_t merged_depth = dir_page->GetLocalDepth(merged_bucket_idx);
      uint32_t merge2_bucket_idx = merged_bucket_idx ^ (0x1 << merged_depth);
      while (dir_page->GetLocalDepth(merged_bucket_idx) == dir_page->GetLocalDepth(merge2_bucket_idx) &&
             dir_page->GetLocalDepth(merged_bucket_idx) > 0) {
        page_id_t merge2_bucket_id = dir_page->GetBucketPageId(merge2_bucket_idx);
        HASH_TABLE_BUCKET_TYPE *merge_bucket = FetchBucketPage(merge2_bucket_id);
        if (merge_bucket->IsEmpty()) {
          dir_page->DecrLocalDepth(merged_bucket_idx);
          merge2_bucket_idx = merge2_bucket_idx ^ (0x1 << (dir_page->GetLocalDepth(merged_bucket_idx)));
          buffer_pool_manager_->DeletePage(merge2_bucket_id);
        } else {
          buffer_pool_manager_->UnpinPage(merge2_bucket_id, false);
        }
      }

      // if(merged_bucket->IsEmpty()) is_glowing =true;
      /*
      local_depth--;
      uint32_t mask = (1 << local_depth) - 1;
      uint32_t idx = mask & bucket_idx;
      uint32_t records_num = 1 << (global_depth - local_depth);
      uint32_t step = (1 << local_depth);
      // basic_index lead to same merge_page
      for (uint32_t i = 0; i < records_num; i++) {
        dir_page->SetBucketPageId(idx, merged_page_id);
        dir_page->DecrLocalDepth(idx);
        idx += step;
      }
       */
      local_depth = dir_page->GetLocalDepth(merged_bucket_idx);
      uint32_t mask = (1 << (local_depth)) - 1;  // merge mask
      uint32_t old_idx = mask & bucket_idx;
      // uint32_t high_bite = dir_page->GetLocalHighBit(bucket_idx);
      for (uint32_t i = 0; i < dir_page->Size(); i++) {
        if ((i & mask) == old_idx) {
          dir_page->SetBucketPageId(i, merged_page_id);
          dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(merged_bucket_idx));
        }
      }
      buffer_pool_manager_->DeletePage(bucket_page_id);
      buffer_pool_manager_->UnpinPage(merged_page_id, true);
    }
    if (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(merged_page_id, true);
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  // buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  // if(is_glowing) Merge(transaction, key, value);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
