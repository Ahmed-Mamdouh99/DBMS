import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;

public class Index<T extends Comparable<T>> implements Serializable {
  private String colName;
  private ArrayList<String> pages;
  private String path;
  private ArrayList<Integer> sizes;

  Index(String colName, String path) {
    this.colName = colName;
    this.path = path;
    pages = new ArrayList<>();
    sizes = new ArrayList<>();
  }

  void insertPage(T[] values, int pageNumber) throws DBAppException {
    // Update page sizes
    sizes.add(pageNumber, values.length);
    // Save values into a set
    HashSet<T> set = new HashSet<>(Arrays.asList(values));
    // Add
    for (String pageName : pages) {
      IndexPage<T> loadedPage = IndexPage.loadPage(new File(pageName));
      loadedPage.addPage(values);
      for (T obj : loadedPage.getValues()) {
        set.remove(obj);
      }
    }
    for (T obj : set) {
      insertNewValue(obj, values, pageNumber);
    }
  }

  private void insertNewValue(T obj, T[] values, int pageNumber) throws DBAppException {
    // Create new map based on the current page value
    int[] sizesArr = new int[sizes.size()];
    for(int i=0; i<sizes.size(); i++) {
      sizesArr[i] = sizes.get(i);
    }
    Bitmap<T> newBitmap = new Bitmap<>(obj, pageNumber, values, sizesArr);
    // Loop through the pages and add the new map
    for (String fileName : pages) {
      IndexPage<T> loadedPage = IndexPage.loadPage(new File(fileName));
      newBitmap = loadedPage.insertValue(newBitmap);
      if (newBitmap == null) {
        break;
      }
    }
    if (newBitmap != null) {
      // Create a new index page to add the value
      String fileName = path+new Date().getTime();
      IndexPage<T> newPage = IndexPage.createPage(new File(fileName));
      pages.add(fileName);
      newPage.insertValue(newBitmap);
    }
  }

  void deletePage(int pageNumber) throws DBAppException {
    // Update page sizes
    sizes.remove(pageNumber);
    for (int i = 0; i < pages.size(); i++) {
      String pageName = pages.get(i);
      IndexPage loadedPage = IndexPage.loadPage(new File(pageName));
      loadedPage.deletePage(pageNumber);
      if (loadedPage.isEmpty()) {
        pages.remove(i--);
      }
    }
  }

  void updatePage(int pageNumber, T[] values) throws DBAppException {
    sizes.remove(pageNumber);
    sizes.add(pageNumber, values.length);
    HashSet<T> uniqueValues = new HashSet<>(Arrays.asList(values));
    for (int i=0; i<pages.size(); i++) {
      String pageName = pages.get(i);
      IndexPage<T> loadedPage = IndexPage.loadPage(new File(pageName));
      loadedPage.updatePage(pageNumber, values);
      for (T obj : values) {
        if (loadedPage.contains(obj)) {
          uniqueValues.remove(obj);
        }
      }
      if(loadedPage.isEmpty()) {
        pages.remove(i--);
      }
    }
    // Add new values
    for (T newValue : uniqueValues) {
      insertNewValue(newValue, values, pageNumber);
    }
  }

  boolean[][] query(SQLTerm term) throws DBAppException {
    if(pages.size() == 0){
      return new boolean[0][0];
    }
    String operator = term._strOperator;
    switch (operator) {
      case ("="):
        return selectEqual(term);
      case ("!="):
        return selectNotEqual(term);
      case ("<"):
        return selectSmallerThan(term);
      case ("<="):
        return selectSmallerThanOrEqual(term);
      case (">"):
        return biggerThan(term);
      case (">="):
        return biggerThanOrEqual(term);
    }
    throw new DBAppException("Unknown operator");
  }

  private boolean[][] biggerThanOrEqual(SQLTerm term) throws DBAppException {
    return invert(selectSmallerThan(term));
  }

  private boolean[][] biggerThan(SQLTerm term) throws DBAppException {
    return invert(selectSmallerThanOrEqual(term));
  }

  private boolean[][] selectSmallerThanOrEqual(SQLTerm term) throws DBAppException {
    // Get the shape of the bitmap
    IndexPage<T> loadedPage = IndexPage.loadPage(new File(pages.get(0)));
    boolean[][] result = loadedPage.getEmptyMap();
    // loop over pages
    for (String page : pages) {
      // load page
      loadedPage = IndexPage.loadPage(new File(page));
      // Check if the value is less than the minimum of the page
      if (loadedPage.peek().compareTo((T) term._objValue) > 0) {
        return result;
      }
      // Check if the current page contains the value
      boolean[][] tmp = loadedPage.getLessThanOrEqual((T) term._objValue);
      result = intersect(result, tmp);
    }
    // Making sure all pages were initialized properly
    return result;
  }


  private boolean[][] selectSmallerThan(SQLTerm term) throws DBAppException {
    // Get the shape of the bitmap
    IndexPage<T> loadedPage = IndexPage.loadPage(new File(pages.get(0)));
    boolean[][] result = loadedPage.getEmptyMap();
    // loop over pages
    for (String page : pages) {
      // load page
      loadedPage = IndexPage.loadPage(new File(page));
      // Check if the value is less than the minimum of the page
      if (loadedPage.peek().compareTo((T) term._objValue) > 0) {
        return result;
      }
      // Check if the current page contains the value
      boolean[][] tmp = loadedPage.getLessThan((T) term._objValue);
      result = intersect(result, tmp);
    }
    // Making sure all pages were initialized properly
    return result;
  }

  private boolean[][] selectNotEqual(SQLTerm term) throws DBAppException {
    // Reverse the result from select equal
    boolean[][] result = selectEqual(term);
    for (int i = 0; i < result.length; i++) {
      for (int j = 0; j < result[i].length; j++) {
        result[i][j] = !result[i][j];
      }
    }
    return result;
  }

  private boolean[][] selectEqual(SQLTerm term) throws DBAppException {
    // Get the shape of the bitmap
    IndexPage<T> loadedPage = IndexPage.loadPage(new File(pages.get(0)));
    boolean[][] result = loadedPage.getEmptyMap();
    // loop over pages
    for (String page : pages) {
      // load page
      loadedPage = IndexPage.loadPage(new File(page));
      // Check if the value is less than the minimum of the page
      if (loadedPage.peek().compareTo((T) term._objValue) > 0) {
        return result;
      }
      // Check if the current page contains the value
      if (loadedPage.contains((T) term._objValue)) {
        return loadedPage.get((T) term._objValue);
      }
    }
    // Making sure all pages were initialized properly
    return result;
  }

  private boolean[][] intersect(boolean[][] arr1, boolean[][] arr2) {
    boolean[][] result = new boolean[arr1.length][];
    for (int i = 0; i < arr1.length; i++) {
      result[i] = new boolean[arr1[i].length];
      for (int j = 0; j < result[i].length; j++) {
        result[i][j] = arr1[i][j] | arr2[i][j];
      }
    }
    return result;
  }

  private boolean[][] invert(boolean[][] arr1) {
    boolean[][] result = new boolean[arr1.length][];
    for (int i = 0; i < arr1.length; i++) {
      result[i] = new boolean[arr1[i].length];
      for (int j = 0; j < result[i].length; j++) {
        result[i][j] = !arr1[i][j];
      }
    }
    return result;
  }

  public String getColName() {
    return colName;
  }
}
