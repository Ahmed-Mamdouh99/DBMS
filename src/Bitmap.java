import java.io.Serializable;
import java.util.ArrayList;

public class Bitmap<T extends Comparable<T>> implements Comparable<Bitmap<T>>, Serializable {
  T value;
  ArrayList<ArrayList<Boolean>> bitmap;

  Bitmap(T value, int pageIndex, T[] tablePage, int[] sizes) {
    this.value = value;
    bitmap = new ArrayList<>();
    for (int i = 0; i < sizes.length; i++) {
      int size = sizes[i];
      ArrayList<Boolean> tmp = new ArrayList<>();
      if(i != pageIndex) {
        for (int j = 0; j < size; j++) {
          tmp.add(false);
        }
      } else {
        if(size != tablePage.length){
          for(int j=0; j<size; j++) {
            tmp.add(tablePage[j].equals(value));
          }
        }
      }
      bitmap.add(tmp);
    }
  }

  boolean[][] toMap() {
    boolean[][] result = new boolean[bitmap.size()][];
    for (int i = 0; i < result.length; i++) {
      result[i] = new boolean[bitmap.get(i).size()];
      for (int j = 0; j < result[i].length; j++) {
        result[i][j] = bitmap.get(i).get(j);
      }
    }
    return result;
  }

  @Override
  public int compareTo(Bitmap<T> o) {
    return value.compareTo(o.value);
  }

  boolean isEmpty() {
    for (ArrayList<Boolean> map : bitmap) {
      for (Boolean bit : map) {
        if (bit) {
          return false;
        }
      }
    }
    return true;
  }
}