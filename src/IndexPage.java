import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

class IndexPage<T extends Comparable<T>> implements Serializable {
  private ArrayList<Bitmap<T>> bitmaps;
  transient private File pageFile;
  private int pageSize;


  // Static methods
  static IndexPage createPage(File pageFile) throws DBAppException {
    IndexPage newPage = new IndexPage();
    newPage.pageFile = pageFile;
    newPage.pageSize = getPageSize();
    newPage.bitmaps = new ArrayList();
    return newPage;
  }

  static IndexPage loadPage(File pageFile) throws DBAppException {
    File temp = null;
    try {
      // Create temp file for decompression
      temp = new File(pageFile.getAbsolutePath() + ".tmp");
      // Unzip the compressed file
      ZipInputStream zis = new ZipInputStream(new BufferedInputStream(
          new FileInputStream(pageFile)));
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(temp));
      // Get the entry from the zip
      zis.getNextEntry();
      // extract the entry into the temp file
      while (zis.available() != 0) {
        bos.write(zis.read());
      }
      bos.flush();
      bos.close();
      BufferedInputStream bis = new BufferedInputStream(new FileInputStream(temp));
      // Read the object from the temp file
      ObjectInputStream objectInputStream = new ObjectInputStream(bis);
      IndexPage loadedPage = (IndexPage) objectInputStream.readObject();
      loadedPage.pageFile = pageFile;
      bis.close();
      objectInputStream.close();
      zis.close();
      if (!temp.delete()) {
        throw new DBAppException("Could not delete temp file");
      }
      return loadedPage;
    } catch (IOException | ClassNotFoundException e) {
      if (!temp.delete()) {
        System.err.println("First");
        throw new DBAppException("Could not delete temp file");
      }

      e.printStackTrace();
      throw new DBAppException("Could not read file from disk: " + e.getMessage());
    }
  }

  void updatePage(int pageNumber, T[] toBitMap) throws DBAppException {
    for (int i = 0; i < bitmaps.size(); i++) {
      Bitmap<T> bitmap = bitmaps.get(i);
      ArrayList<Boolean> map = new ArrayList<>();
      for (T obj : toBitMap) {
        map.add(bitmap.value.compareTo(obj) == 0);
      }
      bitmap.bitmap.remove(pageNumber);
      bitmap.bitmap.add(pageNumber, map);
      if (bitmap.isEmpty()) {
        bitmaps.remove(i);
        i = Math.max(i - 1, 0);
      }
    }
    writeToDisk();
  }

  private void writeToDisk() throws DBAppException {
    if (isEmpty()) {
      if (!pageFile.delete()) {
        throw new DBAppException("Could not delete index page file");
      }
      return;
    }
    try {
      ZipOutputStream zipStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(pageFile)));
      zipStream.putNextEntry(new ZipEntry("ENTRY"));
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(zipStream);
      objectOutputStream.writeObject(this);
      objectOutputStream.flush();
      objectOutputStream.close();
    } catch (IOException e) {
      throw new DBAppException("Could not write index file: " + e.getMessage());
    }
  }

  Bitmap<T> insertValue(Bitmap<T> bitmap) throws DBAppException {
    int index = 0;
    for (; index < bitmaps.size(); index++) {
      Bitmap<T> map = bitmaps.get(index);
      if (map.compareTo(bitmap) >= 0) {
        // If the current value is bigger than the value trying to insert
        bitmaps.add(index, bitmap);
        if (bitmaps.size() <= pageSize) {
          writeToDisk();
          return null;
        } else {
          Bitmap<T> output = bitmaps.remove(pageSize - 1);
          writeToDisk();
          return output;
        }
      }
    }
    if (bitmaps.size() < pageSize) {
      bitmaps.add(bitmap);
      writeToDisk();
      return null;
    }
    return bitmap;
  }

  private static int getPageSize() throws DBAppException {
    return MetaData.getIndexPageSize();
  }

  T peek() {
    if (bitmaps.isEmpty()) {
      return null;
    }
    return bitmaps.get(0).value;
  }

  boolean contains(T value) {
    for (Bitmap<T> bitmap : bitmaps) {
      if (bitmap.value.compareTo(value) == 0) {
        return true;
      }
    }
    return false;
  }

  boolean[][] get(T objValue) throws DBAppException {
    for (Bitmap<T> bitmap : bitmaps) {
      if (bitmap.value.compareTo(objValue) == 0) {
        return bitmap.toMap();
      }
    }
    throw new DBAppException("Value not found");
  }

  boolean[][] getEmptyMap() throws DBAppException {
    boolean[][] tmp = get(peek());
    boolean[][] result = new boolean[tmp.length][];
    for (int i = 0; i < result.length; i++) {
      result[i] = new boolean[tmp[i].length];
    }
    return result;
  }

  int[] getShape() throws DBAppException {
    boolean[][] tmp = get(peek());
    int[] result = new int[tmp.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = tmp[i].length;
    }
    return result;
  }

  boolean[][] getLessThan(T objValue) throws DBAppException {
    boolean[][] resultSet = getEmptyMap();
    for (Bitmap<T> bitmap : bitmaps) {
      if (bitmap.value.compareTo(objValue) >= 0) {
        return resultSet;
      }
      intersect(resultSet, bitmap.toMap());
    }
    return resultSet;
  }

  boolean[][] getLessThanOrEqual(T objValue) throws DBAppException {
    boolean[][] resultSet = getEmptyMap();
    for (Bitmap<T> bitmap : bitmaps) {
      if (bitmap.value.compareTo(objValue) > 0) {
        return resultSet;
      }
      intersect(resultSet, bitmap.toMap());
    }
    return resultSet;
  }

  private void intersect(boolean[][] b1, boolean[][] b2) {
    for (int i = 0; i < b1.length; i++) {
      for (int j = 0; j < b1[i].length; j++) {
        b1[i][j] = b1[i][j] | b2[i][j];
      }
    }
  }

  void deletePage(int pageNumber) throws DBAppException {
    for (int i = 0; i < bitmaps.size(); i++) {
      Bitmap<T> bitmap = bitmaps.get(i);
      bitmap.bitmap.remove(pageNumber);
      if (bitmap.isEmpty()) {
        bitmaps.remove(i--);
      }
    }
    writeToDisk();
  }

  void addPage(T[] toBitMap) throws DBAppException {
    for (Bitmap<T> bitmap : bitmaps) {
      ArrayList<Boolean> map = new ArrayList<>();
      for (T obj : toBitMap) {
        map.add(bitmap.value.compareTo(obj) == 0);
      }
      bitmap.bitmap.add(map);
    }
    writeToDisk();
  }

  Iterable<T> getValues() {
    HashSet<T> values = new HashSet<>();
    for (Bitmap<T> bmap : bitmaps) {
      values.add(bmap.value);
    }
    return values;
  }

  boolean isEmpty() {
    return bitmaps.isEmpty();
  }
}
