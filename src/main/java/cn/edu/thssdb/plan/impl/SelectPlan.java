package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.condition.MultipleConditionPlan;

import java.util.List;

public class SelectPlan extends LogicalPlan {

  private List<String> attributeList;
  private List<String> tableNameList;
  private MultipleConditionPlan onConditionPlan;
  private MultipleConditionPlan whereConditionPlan;

  public SelectPlan(
      List<String> attributeList,
      List<String> tableNameList,
      MultipleConditionPlan onConditionPlan,
      MultipleConditionPlan whereConditionPlan) {
    super(LogicalPlanType.SELECT);
    this.attributeList = attributeList;
    this.tableNameList = tableNameList;
    this.onConditionPlan = onConditionPlan;
    this.whereConditionPlan = whereConditionPlan;
  }

  public List<String> getAttributeList() {
    return attributeList;
  }

  public List<String> getTableNameList() {
    return tableNameList;
  }

  public MultipleConditionPlan getOnConditionPlan() {
    return onConditionPlan;
  }

  public MultipleConditionPlan getWhereConditionPlan() {
    return whereConditionPlan;
  }
}
