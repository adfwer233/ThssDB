package cn.edu.thssdb.plan.condition;

import cn.edu.thssdb.exception.IndexExceedLimitException;
import cn.edu.thssdb.exception.InvalidComparatorException;
import cn.edu.thssdb.exception.TypeNotMatchException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;

public class SingleConditionPlan extends LogicalPlan {
  public String comparator; // {>, >=, <, <=, ==, <>}
  public ComparerPlan expr1;
  public ComparerPlan expr2;

  public SingleConditionPlan(ComparerPlan expr1, ComparerPlan expr2, String comparator) {
    super(LogicalPlanType.CONDITION);
    this.comparator = comparator;
    this.expr1 = expr1;
    this.expr2 = expr2;
  }

  public ComparerPlan getChild(int i) {
    if (i == 0) {
      return this.expr1;
    } else if (i == 1) {
      return this.expr2;
    } else {
      throw new IndexExceedLimitException();
    }
  }

  public Boolean ConditionVerify(Row row, ArrayList<String> columnName) {
    try {
      int result = 0;
      System.out.println("compare here");
      System.out.println(columnName);
      System.out.println(row);
      System.out.println(expr1.tableName);
      System.out.println(expr1.columnName);
      expr1.ComparerRes(row, columnName);
      Object value1 = expr1.getValue(row, columnName);

      expr2.ComparerRes(row, columnName);
      Object value2 = expr2.getValue(row, columnName);


      if (value1 == null || value2 == null) {
        if (comparator.equals("=")) {
          return value1 == value2;
        } else if (comparator.equals("<>")) {
          return value1 != value2;
        } else {
          return false;
        }
      }
      if ((value1 instanceof String && !(value2 instanceof String))
          || !(value1 instanceof String) && value2 instanceof String) {
        throw new TypeNotMatchException(ComparerType.NUMBER, ComparerType.STRING);
      }

      if (value1 instanceof Integer || value1 instanceof Double) {
        Double doubleValue1 = Double.valueOf(value1.toString());
        Double doubleValue2 = Double.valueOf(value2.toString());
        result = doubleValue1.compareTo(doubleValue2);
      } else {
        String stringValue1 = value1.toString();
        String stringValue2 = value2.toString();
        result = stringValue1.compareTo(stringValue2);
      }

      boolean conditionResult = false;
      switch (comparator) {
        case ">":
          conditionResult = result > 0;
          break;
        case ">=":
          conditionResult = result >= 0;
          break;
        case "<":
          conditionResult = result < 0;
          break;
        case "<=":
          conditionResult = result <= 0;
          break;
        case "=":
          conditionResult = result == 0;
          break;
        case "<>":
          conditionResult = result != 0;
          break;
        default:
          throw new InvalidComparatorException(comparator);
      }

      return conditionResult;
    } catch (Exception e) {
      System.out.println("Get Error in ConditionItem.evaluate()\n" + e.getMessage() + ' ' + e.getClass());
      return null;
    }
  }
}
