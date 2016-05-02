package query;

import global.AttrType;
import global.Minibase;
import global.RID;
import global.SortKey;
import heap.HeapFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
    Iterator iter;
    Predicate[][] predicate_list;
    String[] tables;
    SortKey[] sort_order;
    String[] table_columns;
    static Schema finalSchema;
    List<Integer> types = Arrays.asList(AttrType.INTEGER,AttrType.FLOAT,AttrType.STRING);
    static HashMap<String, Integer> mockRelCatalog;
    static HashMap<String, String> mockAttrCatalog;
    
    static private class extras {
        HashMap<String, Set<Integer>> map; 
        boolean[] preds;
        
        public extras(int size) {
            this.preds = new boolean[size];
            this.map = new HashMap<String, Set<Integer>>();
        }
        
    }
    
    private boolean checkAllArugments(Schema new_schema) throws QueryException {
        
        QueryCheck.predicates(new_schema, predicate_list);
        
        for(int i = 0; i < tables.length; i++){
            QueryCheck.tableExists(tables[i]); 
        }
        
        for(int i = 0; i < table_columns.length; i++){
            QueryCheck.columnExists(new_schema, table_columns[i]);
        }
        return true;
    }
    
    private void constructMockRelCatalog() {
        for(int i = 0; i < tables.length; i++) {
            RID rid = Minibase.SystemCatalog.getFileRID(tables[i], true);
            byte[] data = Minibase.SystemCatalog.f_rel.selectRecord(rid);
            Tuple t = new Tuple(Minibase.SystemCatalog.s_rel, data);
            int recCount = t.getIntFld(1);
            
            mockRelCatalog.put(tables[i], recCount);
        }
    }
    
    private void constructMockAttrCatalog() {
        for(int i=0;i < tables.length; i++){
            Schema schema =  Minibase.SystemCatalog.getSchema(tables[i]);
            for(int j=0; j < schema.getCount(); j++){
                    mockAttrCatalog.put(schema.fieldName(j), tables[i]);
            }
        }
    }
    
    /**
     * Optimizes the plan, given the parsed query.
     * 
     * @throws QueryException if validation fails
     */
    public Select(AST_Select tree) throws QueryException {
      table_columns = tree.getColumns();
      sort_order = tree.getOrders();
      predicate_list = tree.getPredicates();
      tables = tree.getTables();

      for(int i = 0; i < tables.length; i++) {
        if(i == 0) {
          finalSchema = Minibase.SystemCatalog.getSchema(tables[i]);
          continue;
        }
        Schema schema = Minibase.SystemCatalog.getSchema(tables[i]);
        finalSchema = Schema.join(finalSchema, schema);
      }

      boolean validity = checkAllArugments(finalSchema);

      if(validity == false) {
          throw new QueryException("Not valid. Sorry. Please go away.");
      }
      
      
      mockRelCatalog = new HashMap<String, Integer>();
      mockAttrCatalog = new HashMap<String, String>();
      constructMockRelCatalog();
      constructMockAttrCatalog();
      
      sort();
      
      extras valids = buildPredValids();
    
      for(int i=0; i < tables.length; i++) {
        Schema schema = Minibase.SystemCatalog.getSchema(tables[i]);
        HeapFile file = new HeapFile(tables[i]);
        Iterator fscan = new FileScan(schema, file);
        if(valids.map.containsKey(tables[i])) {
            Set<Integer> india = valids.map.get(tables[i]);
            for(Integer z : india){
                fscan = new Selection(fscan, predicate_list[z]);
            }
        }
        
        if(i==0) {
                iter = fscan;
        } else {
                Predicate[] x = new Predicate[0];
                iter = new SimpleJoin(iter, fscan, x);
        }
      }
      
      for(int i=0; i < predicate_list.length; i++){
        if(valids.preds[i] == false) {
            iter = new Selection(iter, predicate_list[i]);
        }
      }

      if(table_columns.length != 0)
      {		
        Integer[] fields = new Integer[table_columns.length];
        for(int i = 0; i < table_columns.length; i++) {
            Schema schema = iter.getSchema();
            fields[i] = schema.fieldNumber(table_columns[i]);
        }
        iter = new Projection(iter, fields);
      }
      
    } // public Select(AST_Select tree) throws QueryException

    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {
      iter.execute();
      iter.close();
      System.out.println("MiniSQL : Finished Execution!");
    } // public void execute()
  

    void sort() {
        int j;
        boolean flag = true;
        String temp = "";

        while ( flag )
        {
            flag = false;
            for ( j = 0;  j < tables.length - 1;  j++ )
            {
                if ( mockRelCatalog.get(tables[ j ]) > mockRelCatalog.get(tables[ j+1 ]))
                {                                             // ascending sort
                    temp = tables [ j ];
                    tables [ j ] = tables [ j+1 ];
                    tables [ j+1 ] = temp;
                    flag = true;
                }
            }
        }
    }
    
    private extras buildPredValids() {
        extras e = new extras(predicate_list.length);
        
        for(int i = 0; i < predicate_list.length; i++) {
            int j=0;
            String table_name = null;
           
            for(j = 0; j < predicate_list[i].length; j++) {
                Integer right_side_type = predicate_list[i][j].getRtype();
                String left_field = predicate_list[i][j].getLeft().toString();
                
                if(table_name == null) {
                    table_name = mockAttrCatalog.get(left_field).toString();
                }
                
                if(types.contains(right_side_type) == false){
                    break;
                } else if(table_name.equals(mockAttrCatalog.get(left_field).toString()) == false) {
                    break;
                }
            }
            
            if(j == predicate_list[i].length) {
                if(e.map.get(table_name) == null) {
                    e.map.put(table_name, new HashSet<Integer>());
                }
                
                e.preds[i] = true;
                e.map.get(table_name).add(i);
            }
        }
        return e;
    }

} // class Select implements Plan
