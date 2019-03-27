import java.io.*;
import java.util.*;

class TablePage implements Serializable {
  private Vector<Hashtable<String, Comparable>> records;
  private int maxSize;
  transient private File pageFile;
  transient private boolean changed;

  // static methods
  static TablePage loadPage(File pageFile) throws DBAppException {
    try {
      ObjectInputStream reader = new ObjectInputStream(new FileInputStream(pageFile));
      TablePage loadedPage = (TablePage) reader.readObject();
      reader.close();
      loadedPage.pageFile = pageFile;
      loadedPage.changed = false;
      return loadedPage;
    } catch (IOException | ClassNotFoundException e) {
      throw new DBAppException("Could not load page: " + e.getMessage());
    }
  }

  static TablePage createPage(File pageFile) throws DBAppException {
    TablePage newPage = new TablePage();
    newPage.maxSize = getSizeFromProperties();
    newPage.records = new Vector<>(newPage.maxSize);
    newPage.pageFile = pageFile;
    newPage.changed = false;
    return newPage;
  }

  // Instance methods
  Hashtable<String, Comparable> peek() {
    return records.get(0);
  }

  void writeToDisk() throws DBAppException {
    // Check if the page is empty
    if (records.isEmpty()) {
      // Delete page
      if (!pageFile.delete()) {
        throw new DBAppException("Could not delete page.");
      }
    } else {
      try {
        ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(pageFile));
        writer.writeObject(this);
        writer.flush();
        writer.close();
      } catch (IOException e) {
        throw new DBAppException("Could not write page to disk: " + e.getMessage());
      }
    }
    changed = false;
  }

  Hashtable<String, Comparable> insert(Hashtable<String, Comparable> newRecord, String key) {
    int index = 0;
    Comparable keyValue = newRecord.get(key);
    changed = true;
    // Looping over records
    for (; index < records.size(); index++) {
      // Get the record at the index and compare it to the new record
      Comparable currentValue = records.get(index).get(key);
      if (keyValue.compareTo(currentValue) >= 0) {
        if (records.size() == maxSize) {
          // Switch the current record with the record at the index
          records.add(index, newRecord);
          return records.remove(records.size() - 1);
        } else {
          records.add(index, newRecord);
          return null;
        }
      }
    }
    // Check if the page isn't full
    if (records.size() != maxSize) {
      // add the record and return null
      records.add(newRecord);
      return null;
    }
    changed = false;
    // Return overflow record
    return newRecord;
  }

  void delete(Hashtable<String, Comparable> mask) {
    for (int i = 0; i < records.size(); i++) {
      Hashtable<String, Comparable> currentRecord = records.get(i);
      // Check if the record matches the mask
      boolean match = true;
      for (String colName : mask.keySet()) {
        Comparable value = mask.get(colName);
        if (!(currentRecord.get(colName).compareTo(value) == 0)) {
          match = false;
          break;
        }
      }
      if (match) {
        records.remove(i--);
        changed = true;
      }
    }
  }

  boolean isEmpty() {
    return records.isEmpty();
  }

  // Bitmap methods
  Comparable[] getValues(String colName) {
    Comparable[] map = new Comparable[records.size()];
    for (int i = 0; i < map.length; i++) {
      map[i] = records.get(i).get(colName);
    }
    return map;
  }

  Hashtable<String, Comparable> get(int index) {
    return records.get(index);
  }

  // Private methods
  private static int getSizeFromProperties() throws DBAppException {
    return MetaData.getTablePageSize();
  }

  HashSet<Hashtable<String, Comparable>> get(String strColumnName, Object objValue) {
    HashSet<Hashtable<String, Comparable>> bag = new HashSet<>();
    for (Hashtable<String, Comparable> record : records) {
      if (record.get(strColumnName).compareTo(objValue) == 0) {
        bag.add(record);
      }
    }
    return bag;
  }

  boolean contains(String strColumnName, Object objValue) {
    for (Hashtable<String, Comparable> record : records) {
      if (((Comparable) objValue).compareTo(record.get(strColumnName)) == 0) {
        return true;
      }
    }
    return false;
  }

  HashSet<Hashtable<String, Comparable>> getAll() {
    return new HashSet<>(records);
  }

  HashSet<Hashtable<String, Comparable>> getAll(boolean[] map) {
    HashSet<Hashtable<String, Comparable>> output = new HashSet<>();
    for (int i = 0; i < map.length; i++) {
      if(map[i]) {
        output.add(records.get(i));
      }
    }
    return output;
  }

  List<Hashtable<String, Object>> update(Hashtable<String, Comparable> mask, String keyCol, String tableKeyColumn) {
    List<Hashtable<String, Object>> output = new ArrayList<>();
    // Check if updated records should be re-inserted for sorting
    boolean removeMatches = !keyCol.equals(tableKeyColumn);
    // Loop over records
    for (int i = 0; i < records.size(); i++) {
      Hashtable<String, Comparable> row = records.get(i);
      // Check if the record matches the mask
      if (row.get(keyCol).compareTo(mask.get(keyCol)) == 0) {
        // Updating record
        changed = true;
        for (String colName : mask.keySet()) {
          row.put(colName, mask.get(colName));
        }
        // Add the record to the output and remove from the page
        if (removeMatches) {
          output.add(new Hashtable<>(row));
          records.remove(i--);
        }
      }
    }
    return output;
  }

  public boolean isChanged() {
    return changed;
  }
}
