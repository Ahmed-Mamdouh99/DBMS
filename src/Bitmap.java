import java.io.Serializable;
import java.util.ArrayList;

public class Bitmap<T extends Comparable<T>> implements Comparable<Bitmap<T>>, Serializable {
  T value;
  ArrayList<ArrayList<Boolean>> bitmap;

  Bitmap(T value, int pageIndex, T[] tablePage, int[] sizes) {
    this.value = value;
    bitmap = new ArrayList<>();
    for (int size : sizes) {
      ArrayList<Boolean> tmp = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        tmp.add(false);
      }
      bitmap.add(tmp);
    }
    ArrayList<Boolean> page = bitmap.get(pageIndex);
    for (int i = 0; i < page.size(); i++) {
      page.remove(i);
      page.add(i, tablePage[i].equals(value));
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
    for(ArrayList<Boolean> map : bitmap) {
      for(Boolean bit : map) {
        if(bit){
          return false;
        }
      }
    }
    return true;
  }
}