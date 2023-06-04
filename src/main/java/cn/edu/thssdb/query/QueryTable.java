package cn.edu.thssdb.query;

import cn.edu.thssdb.plan.condition.MultipleConditionPlan;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryTable implements Iterator<Row> {

  public List<Row> rows;
  public List<Column> columns;

  public QueryTable(Table table, Boolean fullTableName) {
    this.rows = new ArrayList<>();
    for (Row row : table) {
      rows.add(row);
    }
    this.columns = new ArrayList<>();
    for (Column col : table.getColumns()) {
      Column tmp = new Column(col.getName());
      if (fullTableName) tmp.setName(table.tableName + "." + col.getName());
      columns.add(tmp);
    }
  }

  public QueryTable(Table table1, Table table2, ArrayList<Row> rows) {
    this.rows = new ArrayList<>();
    this.columns = new ArrayList<>();
    for (Column col : table1.getColumns()) {
      columns.add(new Column(table1.tableName + "." + col.getName()));
    }
    for (Column col : table2.getColumns()) {
      columns.add(new Column(table1.tableName + "." + col.getName()));
    }
    this.rows.addAll(rows);
  }

  public QueryTable(Table table1, ArrayList<Row> rows) {
    this.rows = new ArrayList<>();
    this.columns = new ArrayList<>();
    for (Column col : table1.getColumns()) {
      columns.add(new Column(col.getName()));
    }
    this.rows.addAll(rows);
  }

  @Override
  public boolean hasNext() {
    return rows.iterator().hasNext();
  }

  @Override
  public Row next() {
    return rows.iterator().next();
  }

  public void joinTableWithCondition(Table rightTable, MultipleConditionPlan conditon) {}

  public void joinWithTable(Table rightTable) {
    List<Column> newColumns = new ArrayList<>();

    newColumns.addAll(this.columns);

    String rightTableName = rightTable.tableName;
    for (Column col : rightTable.getColumns()) {
      Column newCol = new Column(col.getName());
      newCol.setName(rightTableName + "." + col.getName());
      newColumns.add(newCol);
    }

    // TODO: add blocking or multi-thread here to speed up join
    List<Row> newRows = new ArrayList<>();

    for (Row rowLeft : this.rows) {
      for (Row rowRight : rightTable) {
        Row tmp = new Row(rowLeft);
        tmp.appendEntries(rowRight.getEntries());
        newRows.add(tmp);
      }
    }
    this.rows = newRows;
    this.columns = newColumns;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer("Query Result\n");
    for (Column col : columns) {
      buffer.append(col.getName() + '\t');
    }
    buffer.append('\n');
    for (Row row : rows) {
      for (Entry entry : row.getEntries()) {
        buffer.append(entry);
        buffer.append('\t');
      }
      buffer.append('\n');
    }
    return buffer.toString();
  }

  public List<List<String>> getRowList() {
    List<List<String>> res = new ArrayList<>();
    for (Row row : rows) {
      ArrayList<String> rowStringList = new ArrayList<>();
      for (Entry entry : row.getEntries()) {
        if (entry != null) {
          rowStringList.add(entry.toString());
        } else {
          rowStringList.add("null");
        }
      }
      res.add(rowStringList);
    }
    return res;
  }
}
