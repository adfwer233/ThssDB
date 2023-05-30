package cn.edu.thssdb.index;

import java.io.Serializable;
import java.util.ArrayList;

public class PageCounter implements Serializable {
  public ArrayList<Integer> indexList = new ArrayList<>();
  private static final long serialVersionUID = -5809782578123943999L;

  public int getMaxIndex() {
    return indexList.get(indexList.size() - 1);
  }

  public int allocNewIndex() {
    if (indexList.isEmpty()) {
      indexList.add(1);
      return 1;
    }
    for (int i = 1; i < indexList.size(); i++) {
      if (indexList.get(i) > indexList.get(i - 1) + 1) {
        indexList.add(i, indexList.get(i - 1) + 1);
        System.out.println("[ALLOC NEW INDEX] " + (indexList.get(i - 1) + 1));
        return indexList.get(i - 1) + 1;
      }
    }
    Integer last = indexList.get(indexList.size() - 1) + 1;
    indexList.add(last);
    System.out.println("[ALLOC NEW INDEX] " + last);
    return last;
  }

  public void removeIndex(int index) {
    indexList.remove(indexList.indexOf(index));
  }
}
