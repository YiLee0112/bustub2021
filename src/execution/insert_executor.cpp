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

#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  if (plan_->IsRawInsert()) {
    total_size_ = plan_->RawValues().size();
    cur_size_ = 0;
  } else {
    child_executor_->Init();
    assert(table_info_->schema_.GetColumnCount() == child_executor_->GetOutputSchema()->GetColumnCount());
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  const Schema *tuple_schema;

  if (plan_->IsRawInsert()) {
    do {
      if (cur_size_ == total_size_) {
        return false;
      }
      *tuple = Tuple(plan_->RawValuesAt(cur_size_++), &table_info_->schema_);
    } while (!table_info_->table_->InsertTuple(*tuple, rid, txn));

    tuple_schema = &table_info_->schema_;
  } else {
    /* select insert */
    if (child_executor_->Next(tuple, rid)) {
      if (!table_info_->table_->InsertTuple(*tuple, rid, txn)) {
        return false;
      }
    } else {
      return false;
    }

    tuple_schema = child_executor_->GetOutputSchema();
  }

  /* insert successfully, update index */
  for (auto &index_info : indexes_) {
   index_info->index_->InsertEntry(
        tuple->KeyFromTuple(*tuple_schema, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid, txn);
  }

  return true;
}

}  // namespace bustub
