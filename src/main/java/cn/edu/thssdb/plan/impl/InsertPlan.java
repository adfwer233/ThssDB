package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

import java.util.List;

public class InsertPlan extends LogicalPlan {
  private String tableName;
  private List<String> attributeNameList;
  private List<List<String>> entryValueList;

  public InsertPlan(
      String tableName, List<String> attributeNameList, List<List<String>> entryValueList) {
    super(LogicalPlanType.INSERT);
    this.tableName = tableName;
    this.attributeNameList = attributeNameList;
    this.entryValueList = entryValueList;
  }

  public String getTableName() {
    return tableName;
  }

  public List<List<String>> getEntryValueList() {
    return entryValueList;
  }

  public List<String> getAttributeNameList() {
    return attributeNameList;
  }
}
