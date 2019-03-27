import java.io.*;
import java.util.*;

public class Table implements Serializable {
  transient private Hashtable<String, Column> columns;
  private Hashtable<String, Index> indices;
  private String name;
  transient private String path;
  private ArrayList<String> pages;
  private String keyColumn;


  Iterator select(SQLTerm[] terms, String[] operators) throws DBAppException {
    // Checking if the query is a select all query
    if (terms.length == 1 && operators.length == 0 && terms[0]._strColumnName == null) {
      HashSet<Hashtable<String, Comparable>> bag = new HashSet<>();
      for (String pageName : pages) {
        TablePage loadedPage = TablePage.loadPage(new File(pageName));
        bag.addAll(loadedPage.getAll());
      }
      return bag.iterator();
    }
    // Validating terms
    if (SQLTerm.validateQuery(terms, operators)) {
      throw new DBAppException("Failed to validate query");
    }
    // Initialize result sets
    HashSet<Hashtable<String, Comparable>> bag0 = null;
    HashSet<Hashtable<String, Comparable>> bag1 = executeQuery(terms[0]);
    for (int index = 1; index < terms.length; index++) {
      // Get the second bag
      HashSet<Hashtable<String, Comparable>> tmp = executeQuery(terms[index]);
      // Merge bags into bag2
      String operator = operators[index - 1];
      switch (operator) {
        case ("OR"):
          bag1 = orBags(bag1, tmp);
          break;
        case ("XOR"):
          bag1 = xorBags(bag1, tmp);
          break;
        case ("AND"):
          if (bag0 == null) {
            bag0 = bag1;
            break;
          }
          bag0 = andBags(bag0, bag1);
          bag1 = tmp;
          break;
      }
    }
    if (bag0 == null) {
      return bag1.iterator();
    }
    return andBags(bag0, bag1).iterator();
  }

  void createBitmapIndex(String colName) throws DBAppException {
    // Check if an index already exists
    if (indices.containsKey(colName)) {
      throw new DBAppException("Column already indexed.");
    }
    // Create index directory
    String indexDirectoryPath = path + "indices/" + colName + "/";
    {
      File directory = new File(indexDirectoryPath);
      if (!directory.mkdirs()) {
        throw new DBAppException("Could not create directory for index: " + name + "." + colName + ".");
      }
    }
    // Get the column type
    String dataType = columns.get(colName).getType();
    Index newIndex;
    switch (dataType) {
      case ("java.lang.String"):
        newIndex = new Index<String>(colName, indexDirectoryPath);
        break;
      case ("java.lang.Integer"):
        newIndex = new Index<Integer>(colName, indexDirectoryPath);
        break;
      case ("java.lang.Double"):
        newIndex = new Index<Double>(colName, indexDirectoryPath);
        break;
      case ("java.lang.Boolean"):
        newIndex = new Index<Boolean>(colName, indexDirectoryPath);
        break;
      case ("java.util.Date"):
        newIndex = new Index<Date>(colName, indexDirectoryPath);
        break;
      default:
        throw new DBAppException("Invalid data type in term " + dataType);
    }
    for (int pageNumber = 0; pageNumber < pages.size(); pageNumber++) {
      String fileName = pages.get(pageNumber);
      TablePage loadedPage = TablePage.loadPage(new File(fileName));
      Comparable[] bitmap = loadedPage.toBitMap(colName);
      newIndex.insertPage(bitmap, pageNumber);
    }
    indices.put(colName, newIndex);
    // Mark column as indexed
    columns.get(colName).setIndexed(true);
    writeToDisk();
  }

  Table(String tableName, String keyColumn, Hashtable<String, String> colNameType) throws DBAppException {
    name = tableName;
    path = "data/" + tableName + "/";
    this.keyColumn = keyColumn;
    pages = new ArrayList<>();
    columns = new Hashtable<>();
    for (String colName : colNameType.keySet()) {
      Column newColumn = new Column(name, colName, colNameType.get(colName), colName.equals(keyColumn), false);
      columns.put(colName, newColumn);
    }
    // Adding touch date column
    columns.put("TouchDate", new Column(name, "TouchDate", "java.util.Date", false, false));
    // Initializing the indices table
    indices = new Hashtable<>();
    // Creating the root directory for the table
    File tableDirectory = new File(path);
    if (!tableDirectory.mkdir()) {
      throw new DBAppException("Could not create table directory");
    }
    // Creating the pages directory for the table
    File pagesDirectory = new File(path + "pages/");
    if (!pagesDirectory.mkdir()) {
      throw new DBAppException("Could not create pages directory");
    }
    // Creating the indices directory for the table
    File indicesDirectory = new File(path + "indices");
    if (!indicesDirectory.mkdir()) {
      throw new DBAppException("Could not create indices directory");
    }
    writeToDisk();
  }

  static Table loadTable(String tableName, String path, HashSet<Column> cols) throws DBAppException {
    try {
      // Read the table from disk
      ObjectInputStream reader = new ObjectInputStream(new FileInputStream(path + tableName));
      Table loadedTable = (Table) reader.readObject();
      reader.close();
      // Add columns
      loadedTable.columns = new Hashtable<>();
      for (Column c : cols) {
        loadedTable.columns.put(c.getName(), c);
      }
      // Add path
      loadedTable.path = path;
      // Return loaded table
      return loadedTable;
    } catch (IOException | ClassNotFoundException e) {
      throw new DBAppException("Could not load table data: " + e.getMessage());
    }
  }

  void insert(Hashtable<String, Object> newRecord) throws DBAppException {
    // Copy the record
    Hashtable<String, Comparable> record = copyHashtable(newRecord);
    // Adding touch date
    record.put("TouchDate", new Date());
    // Loop over the pages
    int pageNum = 0;
    for (; pageNum < pages.size(); pageNum++) {
      File pageFile = new File(pages.get(pageNum));
      // Load the page
      TablePage loadedPage = TablePage.loadPage(pageFile);
      // Check the first value in the page
      if (loadedPage.peek().get(keyColumn).compareTo(record.get(keyColumn)) >= 0) {
        break;
      }
    }
    pageNum = Math.max(0, pageNum - 1);
    // Loop over the pages and keep pushing records until the record fits
    for (; pageNum < pages.size(); pageNum++) {
      File pageFile = new File(pages.get(pageNum));
      TablePage page = TablePage.loadPage(pageFile);
      record = page.insert(record, keyColumn);
      if(page.isChanged()) {
        // Update indices and write page to disk
        updateIndices(pageNum, page);
        page.writeToDisk();
      }
      if (record == null) {
        break;
      }
    }
    // If there is a record left from pushing records then create a new page for it
    if (record != null) {
      // Create new page
      String fileName = path + "pages/" + new Date().getTime();
      File newPageFile = new File(fileName);
      TablePage newPage = TablePage.createPage(newPageFile);
      newPage.insert(record, keyColumn);
      // Add the new fileName to the pages list
      pages.add(fileName);
      newPage.writeToDisk();
      // Adding page to index
      insertToIndices(newPage, pages.size() - 1);
    }
    writeToDisk();
  }

  void update(String keyCol, Hashtable<String, Object> mask) throws DBAppException {
    Hashtable<String, Comparable> record = copyHashtable(mask);
    HashSet<Hashtable<String, Object>> overflow = new HashSet<>();
    // Loop over pages
    for (int i = 0; i < pages.size(); i++) {
      File pageFile = new File(pages.get(i));
      TablePage loadedPage = TablePage.loadPage(pageFile);
      // Update records in page and get overflow unsorted records
      overflow.addAll(loadedPage.update(record, keyCol, this.keyColumn));
      // Check if the page changed
      i = writePageToDisk(i, loadedPage);
    }
    // Add overflow records
    for (Hashtable<String, Object> row : overflow) {
      insert(row);
    }
    writeToDisk();
  }

  void delete(Hashtable<String, Object> mask) throws DBAppException {
    // Copy the record
    Hashtable<String, Comparable> record = copyHashtable(mask);
    // Looping through the pages
    for (int pageNum = 0; pageNum < pages.size(); pageNum++) {
      File pageFile = new File(pages.get(pageNum));
      TablePage page = TablePage.loadPage(pageFile);
      page.delete(record);
      pageNum = writePageToDisk(pageNum, page);
    }
    writeToDisk();
  }

  private int writePageToDisk(int pageNum, TablePage page) throws DBAppException {
    if(page.isChanged()) {
      // Check if the page is empty
      page.writeToDisk();
      if (page.isEmpty()) {
        // Remove the page and move back the pointer
        pages.remove(pageNum);
        removeFromIndices(pageNum--);
        return pageNum;
      }
      updateIndices(pageNum, page);
    }
    return pageNum;
  }

  boolean containsColumn(String colName) {
    return columns.containsKey(colName);
  }

  Column getColumn(String colName) {
    return columns.get(colName);
  }


  // private instance methods
  private void updateIndices(int pageNumber, TablePage page) throws DBAppException {
    for (Column column : columns.values()) {
      if (column.isIndexed()) {
        // Get the index for that specific column
        Index index = indices.get(column.getName());
        index.updatePage(pageNumber, page.toBitMap(column.getName()));
      }
    }
  }

  private void removeFromIndices(int pageNumber) throws DBAppException {
    for (Column column : columns.values()) {
      if (column.isIndexed()) {
        // Get the index for that specific column
        Index index = indices.get(column.getName());
        index.deletePage(pageNumber);
      }
    }
  }

  private void insertToIndices(TablePage newPage, int pageNumber) throws DBAppException {
    for (Column column : columns.values()) {
      if (column.isIndexed()) {
        // Get the index for that specific column
        Index index = indices.get(column.getName());
        index.insertPage(newPage.toBitMap(column.getName()), pageNumber);
      }
    }
  }

  private Hashtable<String, Comparable> copyHashtable(Hashtable<String, Object> table) {
    Hashtable<String, Comparable> copy = new Hashtable<>(table.size());
    for (String key : table.keySet()) {
      copy.put(key, (Comparable) table.get(key));
    }
    return copy;
  }

  private void writeToDisk() throws DBAppException {
    try {
      ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(path + name));
      writer.writeObject(this);
      writer.close();
    } catch (IOException e) {
      throw new DBAppException("Could not save table to memory: " + e.getMessage());
    }
  }

  private HashSet<Hashtable<String, Comparable>> xorBags(HashSet<Hashtable<String, Comparable>> bag1, HashSet<Hashtable<String, Comparable>> bag2) {
    HashSet<Hashtable<String, Comparable>> intersection = orBags(bag1, bag2);
    HashSet<Hashtable<String, Comparable>> symDifference = andBags(bag1, bag2);
    for (Object obj : intersection) {
      symDifference.remove(obj);
    }
    return symDifference;
  }

  private HashSet<Hashtable<String, Comparable>> andBags(HashSet<Hashtable<String, Comparable>> bag1, HashSet<Hashtable<String, Comparable>> bag2) {
    HashSet<Hashtable<String, Comparable>> interSection = new HashSet<>();
    for (Hashtable<String, Comparable> obj : bag2) {
      if (bag1.contains(obj)) {
        interSection.add(obj);
      }
    }
    return interSection;
  }

  private HashSet<Hashtable<String, Comparable>> orBags(HashSet<Hashtable<String, Comparable>> bag1, HashSet<Hashtable<String, Comparable>> bag2) {
    HashSet<Hashtable<String, Comparable>> union = new HashSet<>(bag2);
    union.addAll(bag1);
    return union;
  }

  private HashSet<Hashtable<String, Comparable>> executeQuery(SQLTerm term) throws DBAppException {
    // Check if an index exists
    HashSet<Hashtable<String, Comparable>> bag = new HashSet<>();
    if (indices.containsKey(term._strColumnName)) {
      // Get the bitmap from the index
      boolean[][] bitmap = indices.get(term._strColumnName).query(term);
      // Loop over the table pages and get the values from the result set
      for (int pageNumber = 0; pageNumber < bitmap.length; pageNumber++) {
        boolean[] oneDMap = bitmap[pageNumber];
        // Check if there are any results in the current page
        boolean isEmpty = true;
        for (boolean bit : oneDMap) {
          if (bit) {
            isEmpty = false;
            break;
          }
        }
        if (isEmpty) {
          // Skip page if empty
          continue;
        }
        // Load page
        File pageFile = new File(pages.get(pageNumber));
        TablePage loadedPage = TablePage.loadPage(pageFile);
        // Iterate over values in the map and add matches to the bag
        for (int index = 0; index < oneDMap.length; index++) {
          if (oneDMap[index]) {
            bag.add(loadedPage.get(index));
          }
        }
      }
      return bag;
    }
    // Linear probing
    return probe(term);
  }

  private HashSet<Hashtable<String, Comparable>> probe(SQLTerm term) throws DBAppException {
    // Loop over pages
    HashSet<Hashtable<String, Comparable>> output = new HashSet<>();
    for (String fileName : pages) {
      TablePage loadedPage = TablePage.loadPage(new File(fileName));
      HashSet<Hashtable<String, Comparable>> set = loadedPage.getAll();
      for (Hashtable<String, Comparable> record : set) {
        switch (term._strOperator) {
          case "=":
            if ((record.get(term._strColumnName)).compareTo(term._objValue) == 0) {
              output.add(record);
            }
            break;
          case "!=":
            if ((record.get(term._strColumnName)).compareTo(term._objValue) != 0) {
              output.add(record);
            }
            break;
          case "<":
            if ((record.get(term._strColumnName)).compareTo(term._objValue) < 0) {
              output.add(record);
            }
            break;
          case "<=":
            if ((record.get(term._strColumnName)).compareTo(term._objValue) <= 0) {
              output.add(record);
            }
            break;
          case ">":
            if ((record.get(term._strColumnName)).compareTo(term._objValue) > 0) {
              output.add(record);
            }
            break;
          case ">=":
            if ((record.get(term._strColumnName)).compareTo(term._objValue) >= 0) {
              output.add(record);
            }
            break;
          default:
            throw new DBAppException("Invalid term operator: " + term._strOperator);
        }
      }
    }
    return output;
  }

  @Override
  public String toString() {
    return name + "," + path;
  }

  HashSet<Column> getColSet() {
    return new HashSet<>(columns.values());
  }
}
