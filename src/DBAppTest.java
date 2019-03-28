import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class DBAppTest {

  private static final String[] types = {"java.lang.String", "java.lang.Integer", "java.lang.Double",
      "java.lang.Boolean", "java.util.Date"};
  private static final PrintWriter pw = new PrintWriter(System.out);
  private static Random random = new Random();

  public static void main(String[] args) {
    long seed = new Date().getTime();
    random.setSeed(seed);
      try {
        DBApp app;
        try {
          app = new DBApp();
        } catch (DBAppException e) {
          e.printStackTrace();
          return;
        }
        test(app, true);
        // flush writer
        pw.flush();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.printf("SEED: %d", seed);
      }
    pw.close();
  }

  private static void test(DBApp app, boolean b) throws DBAppException {
    int tableNumber;
    if (b) {
      // Clear data directory
      File dataFile = new File("data/");
      if (dataFile.exists() && !deleteDirectories(dataFile)) {
        System.err.println("Could not clear data directory");
        return;
      }
      tableNumber = testTableCreation(app, 1);
    } else {
      tableNumber = 0;
    }
    // Test table creation
    if (tableNumber == -1) {
      // end testing
      return;
    }
    String tableName = "Table" + tableNumber;
    // Test insertion without indices
    if (b) {
      testInsertion(app, tableName, tableNumber, 100);
    }
    // Test index creation
    if (b) {
      testIndexCreation(app, tableName, tableNumber);
    }
    // Test selection
    testSelection(app, tableName, tableNumber, false);
    // Test update
    testUpdate(app, tableName, tableNumber, true);
    // Test deletion
    testDeletion(app, tableName, tableNumber, false);

  }

  private static void testUpdate(DBApp app, String tableName, int tableNumber, boolean value) throws DBAppException {
    Hashtable<String, Object> mask = new Hashtable<>();
    mask.put(types[3] + tableNumber, value);
    mask.put(types[1] + tableNumber, -1);
    app.updateTable(tableName, types[3] + tableNumber, mask);
    pw.println("Updated records successfully.");
    testSelection(app, tableName, tableNumber, value);
  }

  private static int testTableCreation(DBApp app, int numberOfTables) throws DBAppException {
    Hashtable<String, String> columns = new Hashtable<>();
    int i = 0;
    for (; i < numberOfTables; i++) {
      String tableName = "Table" + i;
      columns.clear();
      for (String type : types) {
        columns.put(type + i, type);
      }
      String keyCol = types[0] + i;
      app.createTable(tableName, keyCol, columns);
    }
    pw.printf("Created %d/%d tables.\n", i, numberOfTables);
    return i - 1;
  }

  private static void testInsertion(DBApp app, String tableName, int tableNumber, int numberOfRecords) throws DBAppException {
    Hashtable<String, Object> record = new Hashtable<>();
    int i = 0;
    for (; i < numberOfRecords; i++) {
      // Clearing the hash table
      record.clear();
      // Generating random data
      Comparable<?>[] values = genRecord();
      for (int j = 0; j < types.length; j++) {
        record.put(types[j] + tableNumber, values[j]);
      }
      app.insertIntoTable(tableName, record);
    }
    pw.printf("Inserted %d/%d rows.\n", i, numberOfRecords);
  }

  private static void testIndexCreation(DBApp app, String tableName, int tableNumber) throws DBAppException {
    int i = 0;
    for (; i < types.length; i++) {
      String type = types[i];
      String colName = type + tableNumber;
      app.createBitmapIndex(tableName, colName);
    }
    pw.printf("Created %d/%d indices.\n", i, types.length);
  }

  private static void testSelection(DBApp app, String tableName, int tableNumber, boolean value) throws DBAppException {
    SQLTerm term = new SQLTerm();
    term._objValue = value;
    term._strOperator = "=";
    term._strColumnName = types[3] + tableNumber;
    term._strTableName = tableName;
    Iterator<Hashtable<String, Object>> result;
    result = app.selectFromTable(new SQLTerm[]{term}, new String[0]);
    int resultSize = printResults(result);
    pw.printf("Selection succeeded with %d matches.\n", resultSize);
  }

  private static void testDeletion(DBApp app, String tableName, int tableNumber, boolean value) throws DBAppException {
    // Deleting all true values from the table
    Hashtable<String, Object> record = new Hashtable<>();
    record.put(types[3] + tableNumber, value);
    app.deleteFromTable(tableName, record);
    pw.println("Deleted records successfully.");
    testSelection(app, tableName, tableNumber, value);
  }

  private static Comparable<?>[] genRecord() {
    StringBuilder string = new StringBuilder();
    for (int i = 0; i < 4; i++) {
      char c = (char) (random.nextInt(91 - 65) + 65);
      string.append(c);
    }
    int integer = random.nextInt();
    double floatingPoint = random.nextDouble();
    boolean bool = random.nextBoolean();
    Date date = new Date();
    return new Comparable<?>[]{string.toString(), integer, floatingPoint, bool, date};
  }

  private static boolean deleteDirectories(File root) {
    for (File subFile : Objects.requireNonNull(root.listFiles())) {
      if (subFile.isFile()) {
        if (!subFile.delete()) {
          return false;
        }
        continue;
      }
      if (!deleteDirectories(subFile)) {
        return false;
      }
      if (!subFile.delete()) {
        return false;
      }
    }
    return true;
  }

  private static int printResults(Iterator<Hashtable<String, Object>> result) {
    pw.println("Result set:");
    int resultSize = 0;
    while (result.hasNext()) {
      resultSize++;
      Hashtable<String, Object> record = result.next();
      StringBuilder recString = new StringBuilder();
      for (Object value : record.values()) {
        recString.append(" ").append(value);
      }
      pw.println(recString);
    }
    return resultSize;
  }
}
