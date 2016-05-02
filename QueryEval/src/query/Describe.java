package query;

import parser.AST_Describe;
import relop.Schema;
import global.AttrType;
import global.Minibase;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {
private String tableName;
private Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {
      tableName = tree.getFileName();
      schema = QueryCheck.tableExists(tableName);
      
  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
     int i = 0;
     System.out.println("=====================================");
      for(i=0;i<schema.getCount();i++)
      {
          System.out.println("field-name: "+getSchema().fieldName(i)+" is of type: "+AttrType.toString(getSchema().fieldType(i)));
      }
      System.out.println("======================================");
    // print the output message
    System.out.println("MiniSQL: Finished Execution!");

  } // public void execute()

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

} // class Describe implements Plan
