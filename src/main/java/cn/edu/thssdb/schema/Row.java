package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  public Row(Row other) {
    this.entries = new ArrayList<>();
    this.entries.addAll(other.getEntries());
  }

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(ArrayList<Entry> entries) {
    this.entries = entries;
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null) return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries) sj.add(e.toString());
    return sj.toString();
  }
}
