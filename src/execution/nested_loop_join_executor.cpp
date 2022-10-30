//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      is_dummy_left_(true) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  is_dummy_left_ = true;

  column_cnt_ = plan_->OutputSchema()->GetColumnCount();
  for (uint32_t i = 0; i < column_cnt_; i++) {
    left_or_right_.push_back(
        static_cast<const ColumnValueExpression *>(plan_->OutputSchema()->GetColumn(i).GetExpr())->GetTupleIdx());
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;

  /* enter upon first Next() invocation */
  if (is_dummy_left_) {
    if (!left_executor_->Next(&cur_left_, rid)) {
      return false;
    }
    is_dummy_left_ = false;
  }

  while (true) {
    if (!right_executor_->Next(&right_tuple, rid)) {
      if (!left_executor_->Next(&cur_left_, rid)) {
        return false;
      }
      right_executor_->Init();
    } else {
      if ((plan_->Predicate() == nullptr) || plan_->Predicate()
                                                 ->EvaluateJoin(&cur_left_, left_executor_->GetOutputSchema(),
                                                                &right_tuple, right_executor_->GetOutputSchema())
                                                 .GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < column_cnt_; i++) {
          values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(
              left_or_right_[i] == 0 ? &cur_left_ : &right_tuple,
              left_or_right_[i] == 0 ? plan_->GetLeftPlan()->OutputSchema() : plan_->GetRightPlan()->OutputSchema()));
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        return true;
      }
    }
  }
}

}  // namespace bustub
