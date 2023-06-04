package cn.edu.thssdb.exception.InsertException;

public class AttributeNameNotExistException extends Exception {
  private final String attrName;

  public AttributeNameNotExistException(String attrName) {
    super();
    this.attrName = attrName;
  }

  @Override
  public String getMessage() {
    return "Attribute Name" + attrName + "Not Exist";
  }
}
