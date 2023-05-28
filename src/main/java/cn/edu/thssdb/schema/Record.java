package cn.edu.thssdb.schema;

public class Record {
  private Row content;

  public Record() {
    this.content = null;
  }

  public Record(Row content) {
    this.content = content;
  }

  public void removeContent() {
    content = null;
  }
  public Boolean hasContent() {
    return content != null;
  }

  public Row getContent() {
    return content;
  }
}
