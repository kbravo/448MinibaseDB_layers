package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_Update;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Selection;
import relop.Tuple;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {
    
    String fileName;
    Predicate[][] predicate_list;
    Schema schema;
    Object[] values;
    int[] field_numbers;
    String[] column_names;
    
  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if invalid column names, values, or predicates
   */
    
  public Update(AST_Update tree) throws QueryException {
      fileName = tree.getFileName();
      schema = Minibase.SystemCatalog.getSchema(fileName);
      column_names = tree.getColumns();
      values = tree.getValues();
      predicate_list = tree.getPredicates();
      
      //Query Checks
      field_numbers = QueryCheck.updateFields(schema, column_names);
      QueryCheck.updateValues(schema, field_numbers, values);
      QueryCheck.predicates(schema, predicate_list);
      
      if(column_names.length > 0) {
          for(int i = 0; i < column_names.length; i++) {
              QueryCheck.columnExists(schema, column_names[i]);
          }
      }
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
      int row_updated_count = 0;
      IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(fileName, schema, field_numbers);
      HeapFile file = new HeapFile(fileName);
      FileScan scan = new FileScan(schema, file);
      
      if(predicate_list.length <= 0) {
          System.out.println("The query does not have any predicates.");
          
          while(scan.hasNext()) {
            row_updated_count++;
            Tuple t = scan.getNext();
            RID rid = scan.getLastRID();
            
            for(IndexDesc i : indexes) {
                HashIndex index = new HashIndex(i.indexName);
                SearchKey key = new SearchKey(t.getField(i.columnName));
                index.deleteEntry(key, rid);
            }
            
            if(column_names.length > 0){
                for(int i = 0; i < column_names.length; i++){
                  t.setField(field_numbers[i],values[i]);
                }
            }

            file.updateRecord(rid, t.getData());
            
            for(IndexDesc i : indexes) {
                HashIndex index = new HashIndex(i.indexName);
                SearchKey key = new SearchKey(t.getField(i.columnName));
                index.insertEntry(key, rid);
            }
        }
        scan.close();
        System.out.println("All "+ row_updated_count + " rows affected.");
        return;
      }
      
      Selection selection_tuples = new Selection(scan, predicate_list[0][0]);
      for(int a = 0; a < predicate_list.length; a++) {
          for(int b = 0; b < predicate_list[a].length; b++) {
              selection_tuples = new Selection(selection_tuples, predicate_list[a][b]);
          }
      }
      
      
      while(selection_tuples.hasNext()) {
        row_updated_count++;
        
        RID rid = scan.getLastRID();
        Tuple t = new Tuple(schema,file.selectRecord(rid));
        for(IndexDesc i : indexes) {
            HashIndex index = new HashIndex(i.indexName);
            SearchKey key = new SearchKey(t.getField(i.columnName));
            index.deleteEntry(key, rid);
        }
          
        if(column_names.length > 0){
              for(int i = 0; i < column_names.length; i++){
                      t.setField(field_numbers[i],values[i]);
              }
        }
        file.updateRecord(rid, t.getData());
        
        for(IndexDesc i : indexes) {
            HashIndex index = new HashIndex(i.indexName);
            SearchKey key = new SearchKey(t.getField(i.columnName));
            index.insertEntry(key, rid);
        }
      }
      scan.close();
      
      
    // print the output message
    System.out.println(row_updated_count + " rows affected.");

  } // public void execute()

} // class Update implements Plan
