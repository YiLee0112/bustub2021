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

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  //init directory page
  auto dir_page = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page_data = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  dir_page_data->IncrGlobalDepth();// set global depth to 1
  dir_page_data->SetPageId(directory_page_id_);


  //set local depth to 1, init 2 bucket page
  page_id_t bucketpageid0;
  page_id_t bucketpageid1;
  buffer_pool_manager_->NewPage(&bucketpageid0);
  buffer_pool_manager_->NewPage(&bucketpageid1);
  dir_page_data->SetBucketPageId(0,bucketpageid0);
  dir_page_data->SetLocalDepth(0,1);
  dir_page_data->SetBucketPageId(0,bucketpageid1);
  dir_page_data->SetLocalDepth(1,1);

  //Unpin these pages
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucketpageid0, false);
  buffer_pool_manager_->UnpinPage(bucketpageid1, false);


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
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<Page *, HASH_TABLE_BUCKET_TYPE *> HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  return std::pair<Page *, HASH_TABLE_BUCKET_TYPE *>(bucket_page, bucket_page_data);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  //table加锁
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);
  //page加锁
  bucket_page->RLatch();
  auto res = bucket_page_data->GetValue(key, comparator_, result);
  //Unpin pages
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);//TODO:?

  bucket_page->RUnlatch();
  table_latch_.RUnlock();
  return res;

}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  //table加锁
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);
  //page加锁
  bucket_page->RLatch();
  //如果需要分裂
  if(bucket_page_data->IsFull()){
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->RUnlatch();
    table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }else{
    //不需要分裂
    bool res = bucket_page_data->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(bucket_page_id, res);//is_dirty根据插入情况而定
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->RUnlatch();
    table_latch_.RUnlock();
    return res;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  //table加锁
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);
  //page加锁
  bucket_page->RLatch();
  //先进行remove
  bool res = bucket_page_data->Remove(key, value, comparator_);
  //如果remove后为空则需要分裂
  if(res && bucket_page_data->IsEmpty()){
    buffer_pool_manager_->UnpinPage(bucket_page_id, res);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->RUnlatch();
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return res;
  }else{
    //不需要分裂
    buffer_pool_manager_->UnpinPage(bucket_page_id, res);//is_dirty根据插入情况而定
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    bucket_page->RUnlatch();
    table_latch_.RUnlock();
    return res;
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

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
