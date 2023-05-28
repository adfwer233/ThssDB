package cn.edu.thssdb.index;

import java.util.ArrayList;

public class PageCounter {
  public ArrayList<Integer> indexList = new ArrayList<Integer>();
  public int getMaxIndex() {
    return indexList.get(indexList.size() - 1);
  }

  public int allocNewIndex() {
    for (int i = 1; i < indexList.size(); i++) {
      if (indexList.get(i) > indexList.get(i - 1)) {
        indexList.add(i, indexList.get(i - 1) + 1);
        return i;
      }
    }
    Integer last = indexList.get(indexList.size() - 1);
    indexList.add(last);
    return last;
  }

  public void removeIndex(int index) {
    indexList.remove(indexList.indexOf(index));
  }
}
