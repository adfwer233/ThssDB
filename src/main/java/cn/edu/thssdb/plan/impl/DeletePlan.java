package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;

public class DeletePlan extends LogicalPlan {

  private final String tableName;
  private final MultipleConditionPlan whereCond;

  public DeletePlan(String tableName, MultipleConditionPlan whereCond) {
    super(LogicalPlanType.DELETE);
    this.whereCond = whereCond;
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public MultipleConditionPlan getWhereCond() {
    return whereCond;
  }
}
