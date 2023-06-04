package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class ShowTablePlan extends LogicalPlan {
  private final String tableName;

  public String getTableName() {
    return tableName;
  }

  public ShowTablePlan(String tableName) {
    super(LogicalPlan.LogicalPlanType.SHOW_TABLE);
    this.tableName = tableName;
  }
}
