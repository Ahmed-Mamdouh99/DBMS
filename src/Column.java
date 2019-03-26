import java.io.Serializable;

public class Column implements Serializable {
  private String tableName;
  private String name;
  private String type;
  private boolean isKey;
  private boolean indexed;

  Column(String tableName, String name, String type, boolean isKey, boolean indexed) {
    this.tableName = tableName;
    this.name = name;
    this.type = type;
    this.isKey = isKey;
    this.indexed = indexed;
  }

  Column(String csv) {
    String[] fields = csv.split(",");
    tableName = fields[0];
    name = fields[1];
    type = fields[2];
    isKey = Boolean.parseBoolean(fields[3]);
    indexed = Boolean.parseBoolean(fields[4]);
  }

  boolean isIndexed() {
    return indexed;
  }

  void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }

  String getName() {
    return name;
  }

  String getType() {
    return type;
  }

  @Override
  public String toString() {
    return (tableName + "," + name + "," + type + "," + isKey + "," + indexed);
  }

  String getTableName() {
    return tableName;
  }
}