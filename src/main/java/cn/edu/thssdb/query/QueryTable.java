package cn.edu.thssdb.query;

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

  public QueryTable(Table table) {
    this.rows = new ArrayList<>();
    for (Row row : table) {
      rows.add(row);
    }
    this.columns = new ArrayList<>();
    for (Column col : table.getColumns()) {
      Column tmp = new Column(col.getName());
      tmp.setName(table.tableName + "." + col.getName());
      columns.add(tmp);
    }
  }

  @Override
  public boolean hasNext() {
    return rows.iterator().hasNext();
  }

  @Override
  public Row next() {
    return rows.iterator().next();
  }

  public void joinWithTable(Table rightTable) {
    List<Column> newColumns = new ArrayList<>();

    newColumns.addAll(this.columns);

    String rightTableName = rightTable.tableName;
    for (Column col : rightTable.getColumns()) {
      col.setName(rightTableName + "." + col.getName());
      newColumns.add(col);
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
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer("Query Result\n");
    for (Column col: columns) {
      buffer.append(col.getName() + '\t');
    }
    buffer.append('\n');
    for(Row row : rows) {
      for (Entry entry: row.getEntries()) {
        buffer.append(entry);
        buffer.append('\t');
      }
      buffer.append('\n');
    }
    return buffer.toString();
  }
}
