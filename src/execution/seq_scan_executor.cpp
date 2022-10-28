//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {
  table_oid_t oid = plan->GetTableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(oid);
  iterator_ = table_info_->table_->Begin(exec_ctx->GetTransaction());
  end_ = table_info_->table_->End();
}

void SeqScanExecutor::Init() {
  iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const Schema *out_schema = GetOutputSchema();
  // 输出schema
  Schema table_schema_ = table_info_->schema_;
  // 当前table的schema
  while (iterator_ != end_) {
  Tuple table_tuple_ = *iterator_;
  std::vector<Value> values_;
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    values_.emplace_back(col.GetExpr()->Evaluate(&table_tuple_, &table_schema_));
  }
  *tuple = Tuple(values_, out_schema);
  auto predict = plan_->GetPredicate();
  if (predict == nullptr || predict->Evaluate(tuple, out_schema).GetAs<bool>()) {
    iterator_++;
    return true;
  }
  iterator_++;
  }
  return false;
}

}  // namespace bustub
