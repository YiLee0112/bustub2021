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
  /** 如果插入来源是子节点则初始化子节点 */
  if (!from_insert_) {
    child_executor_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple tuple_;
  RID rid_;
  Transaction *txn_ = exec_ctx_->GetTransaction();
  // 如果插入来源是输入
  if (from_insert_) {
    /** 逐行插入 */
    for (uint32_t idx = 0; idx < size_; idx++) {
      std::vector<Value> value_ = plan_->RawValuesAt(idx);
      tuple_ = Tuple(value_, &table_info_->schema_);
      // 利用当前表的schema进行插入tuple的构造
      if (table_info_->table_->InsertTuple(tuple_, &rid_, txn_)) {
        // 若插入表成功，则插入索引
        for (auto index : indexes_) {
          // 使用InsertEntry更新表中的所有索引，InsertEntry的参数应由KeyFromTuple方法构造
          index->index_->InsertEntry(
              tuple_.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
                                         rid_, txn_);

        }
      }
    }
    // Insert节点不应向外输出任何元组，所以其总是返回假
    return false;
  }
  // 如果插入的来源是子节点（扫描节点）
  while (child_executor_->Next(&tuple_, &rid_)) {
    /** 子节点每次返回一个tuple，所以每次将返回的结果插入 */
    if (table_info_->table_->InsertTuple(tuple_, &rid_, txn_)) {
      // 若插入表成功，则插入索引
      for (auto index : indexes_) {
        // 使用InsertEntry更新表中的所有索引，InsertEntry的参数应由KeyFromTuple方法构造
        index->index_->InsertEntry(
            /** 注意:构造时使用的schema是子节点的outputschema */
            tuple_.KeyFromTuple(*child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
            rid_, txn_);

      }
    }
  }
  return false;
}

}  // namespace bustub
