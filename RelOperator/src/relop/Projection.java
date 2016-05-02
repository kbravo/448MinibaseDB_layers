package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
    private boolean isOpen;

    private Iterator iterator;
    private Integer[] fields;
    private Schema schema;
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
      this.fields = fields;

      Schema old = iter.getSchema();
     schema = new Schema(this.fields.length);

      int count = 0;

        for (int i = 0; i < fields.length; i++) {
            int k = fields[i].intValue();
            schema.initField(i, old.fieldType(k), old.fieldLength(k), old.fieldName(k));
        }

        this.setSchema(schema);
        this.iterator = iter;
        this.iterator.restart();
        this.isOpen = true;
  }

    /**
     * Gives a one-line explaination of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
      indent(depth);
      System.out.println("Projection Plan \n");
      iterator.explain(depth + 1);
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {

      this.isOpen = true;
      this.iterator.restart();

    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
      return isOpen;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
    if(isOpen == false)
      {
        throw new IllegalStateException("The Projection is already closed");
      }

      this.isOpen = false;
      this.iterator.close();
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        if(isOpen()) {
            return iterator.hasNext();
        } else {
            throw new IllegalStateException("file failed to open");
        }    
    }

    /**
     * Gets the next tuple in the iteration.
     * 
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        Tuple oldTup = iterator.getNext();

        Tuple tup = new Tuple(getSchema());
        for(int i = 0; i < fields.length; i++)
                tup.setField(i, oldTup.getField(fields[i]));

         if(tup == null)
        {
          throw new IllegalStateException("No Qualifying Tuple Found");
        }       
        return tup;
    }

    public boolean isIsOpen() {
        return isOpen;
    }

    public void setIsOpen(boolean isOpen) {
        this.isOpen = isOpen;
    }

    public Iterator getIterator() {
        return iterator;
    }

    public void setIterator(Iterator iterator) {
        this.iterator = iterator;
    }

    public Integer[] getFields() {
        return fields;
    }

    public void setFields(Integer[] fields) {
        this.fields = fields;
    }


  
} // public class Projection extends Iterator
