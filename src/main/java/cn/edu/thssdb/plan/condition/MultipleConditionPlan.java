package cn.edu.thssdb.plan.condition;

import cn.edu.thssdb.exception.IndexExceedLimitException;
import cn.edu.thssdb.exception.InvalidComparatorException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;

public class MultipleConditionPlan extends LogicalPlan {
  public String op;
  public MultipleConditionPlan multipleConditionPlan1;
  public MultipleConditionPlan multipleConditionPlan2;
  public SingleConditionPlan singleConditionPlan;
  public Boolean hasChild = false;

  public MultipleConditionPlan(SingleConditionPlan singleConditionPlan) {
    super(LogicalPlanType.MULTIPLE_CONDITION);
    this.hasChild = false;
    this.singleConditionPlan = singleConditionPlan;
  }

  public MultipleConditionPlan(MultipleConditionPlan m1, MultipleConditionPlan m2, String op) {
    super(LogicalPlanType.MULTIPLE_CONDITION);
    this.multipleConditionPlan1 = m1;
    this.multipleConditionPlan2 = m2;
    this.op = op;
    this.hasChild = true;
  }

  public Boolean hasChild() {
    return this.hasChild;
  }

  public MultipleConditionPlan getChild(int i) {
    if (i == 0) {
      return this.multipleConditionPlan1;
    } else if (i == 1) {
      return this.multipleConditionPlan2;
    } else {
      throw new IndexExceedLimitException();
    }
  }

  public Boolean ConditionVerify(Row row, ArrayList<String> columnName) {
    if (!hasChild) {
      return singleConditionPlan.ConditionVerify(row, columnName);
    } else {
      Boolean cond1 = multipleConditionPlan1.ConditionVerify(row, columnName);
      Boolean cond2 = multipleConditionPlan2.ConditionVerify(row, columnName);
      if (op.equals("&&")) {
        return (cond1 && cond2);
      } else if (op.equals("||")) {
        return (cond1 || cond2);
      } else {
        throw new InvalidComparatorException(op);
      }
    }
  }
}
