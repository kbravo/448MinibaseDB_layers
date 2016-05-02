package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
    /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
    private Tuple TupIter;
    private Iterator iterator;
    private boolean isOpen;
    private Predicate[] predicate;
    
    public Selection(Iterator iter, Predicate... preds) {
      this.predicate = preds; // array
      this.setSchema(iter.getSchema());
      this.iterator = iter;
      this.isOpen = true;
      this.iterator.restart();
    }

    /**
     * Gives a one-line explaination of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
      indent(depth);
      System.out.println("Selection Plan \n");
      iterator.explain(depth + 1);
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        if(this.isOpen())
            close();
        this.iterator.restart();
        this.isOpen = true;
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
      return this.isOpen;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        if(this.isOpen())
            iterator.close();
        else
            throw new IllegalStateException("Random error : Scan already in closed state");
        this.isOpen = false;
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        while (iterator.hasNext()) {
            Tuple tuple = iterator.getNext();
            if(validatePreds(tuple)) {
                TupIter = tuple;
                return true;
            }
        }
        TupIter = null;
        return false;
    }
  
    public boolean hasNextRaw() {
            return iterator.hasNext();
    }

    /**
     * Gets the next tuple in the iteration.
     * 
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        if(isOpen()) {
            if(TupIter != null)
                return TupIter;
            else
                throw new IllegalStateException("No more tuples");
        } else {
            throw new IllegalStateException("No more tuples");
        }
    }
  
    public boolean validatePreds(Tuple tuple) {
        for(int i = 0; i < predicate.length; i++) {
            if(predicate[i].evaluate(tuple) == true)
                return true;
        }
        return false;
    }

    public Tuple getTupIter() {
        return TupIter;
    }

    public void setTupIter(Tuple TupIter) {
        this.TupIter = TupIter;
    }

    public Iterator getIterator() {
        return iterator;
    }

    public void setIterator(Iterator iterator) {
        this.iterator = iterator;
    }

    public boolean isIsOpen() {
        return isOpen;
    }

    public void setIsOpen(boolean isOpen) {
        this.isOpen = isOpen;
    }

    public Predicate[] getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate[] predicate) {
        this.predicate = predicate;
    }
    
} // public class Selection extends Iterator
