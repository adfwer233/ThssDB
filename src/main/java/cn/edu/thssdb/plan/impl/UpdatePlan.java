package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.condition.ComparerPlan;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;

public class UpdatePlan extends LogicalPlan {

  private String tableName;

  private String columnName;

  private MultipleConditionPlan whereCond;

  private ComparerPlan expr;

  public UpdatePlan(
      String tableName, MultipleConditionPlan whereCond, String columnName, ComparerPlan expr) {
    super(LogicalPlanType.UPDATE);
    this.whereCond = whereCond;
    this.tableName = tableName;
    this.columnName = columnName;
    this.expr = expr;
  }

  public String getTableName() {
    return tableName;
  }

  public MultipleConditionPlan getWhereCond() {
    return whereCond;
  }

  public String getColumnName() {
    return columnName;
  }

  public ComparerPlan getExpr() {
    return expr;
  }
}
