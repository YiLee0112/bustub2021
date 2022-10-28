//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_oid_t oid = plan->TableOid();
  auto catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // 旧的tuple
  Tuple tuple_old_;
  auto *txn_ = GetExecutorContext()->GetTransaction();
  while (child_executor_->Next(&tuple_old_, rid)) {
    /** 子节点每次返回一个tuple，所以每次将返回的结果更新 */
    *tuple = GenerateUpdatedTuple(tuple_old_);
    if (table_info_->table_->UpdateTuple(*tuple, *rid, txn_)) {
      // 若插入表成功，则插入索引
      for (auto index : indexes_) {
        /** 使用DeleteEntry删除表中的旧索引 */
        index->index_->DeleteEntry(
            /** 注意:构造时使用的schema是子节点的outputschema */
            tuple_old_.KeyFromTuple(*child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
            *rid, txn_);
        /** 使用InsertEntry插入表中的新索引 */
        index->index_->InsertEntry(
            tuple->KeyFromTuple(*child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs()),
            *rid, txn_);

      }
    }
  }
  return false;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
