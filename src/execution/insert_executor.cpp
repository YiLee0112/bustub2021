//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(oid);
  from_insert_ = plan->IsRawInsert();
  if (from_insert_) {
    size_ = plan->RawValues().size();
  }
  indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (!from_insert_) {
    child_executor_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple tuple_insert;
  RID rid_insert;
  Transaction *txn = exec_ctx_->GetTransaction();
  if (from_insert_) {
    for (uint32_t idx = 0; idx < size_; idx++) {
      std::vector<Value> value = plan_->RawValuesAt(idx);
      tuple_insert = Tuple(value, &table_info_->schema_);
      if (table_info_->table_->InsertTuple(tuple_insert, &rid_insert, txn)) {
        for (auto index : indexes_) {
          index->index_->InsertEntry(
              tuple_insert.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
              rid_insert, txn);
        }
      }
    }
    return false;
  }
  while (child_executor_->Next(&tuple_insert, &rid_insert)) {
    if (table_info_->table_->InsertTuple(tuple_insert, &rid_insert, txn)) {
      for (auto index : indexes_) {
        index->index_->InsertEntry(tuple_insert.KeyFromTuple(*child_executor_->GetOutputSchema(), index->key_schema_,
                                                             index->index_->GetKeyAttrs()),
                                   rid_insert, txn);
      }
    }
  }
  return false;
}

}  // namespace bustub
