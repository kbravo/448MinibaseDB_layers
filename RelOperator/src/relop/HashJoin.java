package relop;

import java.util.ArrayList;
import java.util.Random;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

public class HashJoin extends Iterator {

/**
 * Constructs a hash join, given the left and right iterators and which
 * columns to match (relative to their individual schemas).
 */
    
  private Tuple currentRightIndex;
  private int currentIndex;
  
  private static char[] arr;
  private static final String CHAR_SET = 
          "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
  

  private Iterator leftSideIter;
  private Iterator rightSideIter;
  private Integer leftCol;
  private Integer rightCol;

  private IndexScan leftBuckets;
  private IndexScan rightBuckets;

  private Tuple tupleIter;
  ArrayList<String> tupleList;

  //INNER JOIN CONSTRUCTION
  IndexScan innerIndexScan;
  IndexScan outerIndexScan;

  private HashTableWithDups hashTDups;
   
  /* Change from Iterator to FileScan */
  
  private int getRandom() {
      int myint = 0;
      Random randomGenerator = new Random();
      myint = randomGenerator.nextInt(arr.length);
      
      if (myint - 1 < 0) 
        return myint;
      else
        return myint - 1;
  }
  
  public FileScan convertToFileScan(Iterator iterator){
      StringBuffer Str = new StringBuffer();
      
      for(int i = 0; i < 16; i++) {
        int number = getRandom();
        char ch = arr[number];
        Str.append(ch);
      }
      
      FileScan f = null;
      if(iterator instanceof IndexScan) {
        f = new FileScan(iterator.getSchema(), ((IndexScan) iterator).getFile());
      } else {
        HeapFile file = new HeapFile(Str.toString());
        
        while(iterator.hasNext()) {
            file.insertRecord(iterator.getNext().data);
        }
        
        f = new FileScan(iterator.getSchema(), file);
      }
      
      return f;
  }
	
  public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
    this.arr = CHAR_SET.toCharArray();
    this.leftSideIter = left;
    this.rightSideIter = right;

    this.leftCol = lcol;
    this.rightCol = rcol;

    this.setSchema(Schema.join(left.getSchema(), right.getSchema()));
    this.hashTDups = new HashTableWithDups();
    this.currentIndex = -1;
    this.currentRightIndex = null;

    constructInnerIndexScan(right);
    constructOutsideIndexScan(left);

    this.tupleList = new ArrayList<String>();

    init();
    outerIndexScan.restart();
    innerIndexScan.restart();
  }
  
  public void constructInnerIndexScan(Iterator right) {
    HashIndex rightHash = new HashIndex(null);
    FileScan rightfScan = convertToFileScan(right);
    while(rightfScan.hasNext()) {
            rightHash.insertEntry(new SearchKey(rightfScan.getNext().getField(this.rightCol)), rightfScan.getLastRID());
    }
    this.innerIndexScan = new IndexScan(right.getSchema(), rightHash, rightfScan.getFile());
  }
  
  public void constructOutsideIndexScan(Iterator left) {
    HashIndex leftHash = new HashIndex(null);
    FileScan leftfScan = convertToFileScan(left);
    while(leftfScan.hasNext()) {
        leftHash.insertEntry(new SearchKey(leftfScan.getNext().getField(leftCol)), leftfScan.getLastRID());
    }
    this.outerIndexScan = new IndexScan(left.getSchema(), leftHash, leftfScan.getFile());  
  }
  
  public void init() {
    while(outerIndexScan.hasNext()) {
            Tuple tuple = outerIndexScan.getNext();
            SearchKey hash = new SearchKey(tuple.getField(leftCol).toString());
            hashTDups.add(hash, tuple);
    }
  }
  
  public void explain(int depth) {
    System.out.println("HashJoin Plan: Iterator " + depth);
    this.outerIndexScan.explain(depth+1);
    this.innerIndexScan.explain(depth+2);
  }


  public void restart() {
    innerIndexScan.restart();
    outerIndexScan.restart();
    tupleList = new ArrayList<String>();
    currentRightIndex = null;
    currentIndex = -1;
    init();
  }


  public boolean isOpen() { 
    if(leftBuckets.isOpen() && rightBuckets.isOpen())
        return true;
    else
        return false;
  }

  public void close() {
    if(this.outerIndexScan.isOpen())
        this.outerIndexScan.close();
    else
        throw new IllegalStateException("Random Error : Outer Index Scan already in closed state");
    
    if(this.innerIndexScan.isOpen())
        this.innerIndexScan.close();
    else
        throw new IllegalStateException("Random Error : Inner Index Scan already in closed state");
  }
  
  public boolean hasNext() {
    if(currentIndex == -1) {
        try {
            currentRightIndex = innerIndexScan.getNext();
        } catch (IllegalStateException e) {
            tupleIter = null;
            return false;
        }
        currentIndex = 0;
    }

    SearchKey key = new SearchKey(currentRightIndex.getField(rightCol).toString());
    Tuple[] tuple_Arr = hashTDups.getAll(key);
    
    if(tuple_Arr == null) {
            currentIndex = -1;
            return hasNext();
    }
    
    if(currentIndex >= tuple_Arr.length) {
            currentIndex = -1;
            return hasNext();
    }
    
    Tuple leftTuple = tuple_Arr[currentIndex];
    tupleIter = Tuple.join(leftTuple, currentRightIndex, getSchema());
    currentIndex++;
    
    if(tupleList.contains(new String(tupleIter.data)))
        return hasNext();
    else {
        tupleList.add(new String(tupleIter.data));
        return true;
    }
  }

  public Tuple getNext() {
    if(tupleIter == null)
    	throw new IllegalStateException("Random Error : No tuples left to scan");
    else
    	return tupleIter;
  }

    public Iterator getLeftSideIter() {
        return leftSideIter;
    }

    public void setLeftSideIter(Iterator leftSideIter) {
        this.leftSideIter = leftSideIter;
    }

    public Iterator getRightSideIter() {
        return rightSideIter;
    }

    public void setRightSideIter(Iterator rightSideIter) {
        this.rightSideIter = rightSideIter;
    }

    public Integer getLeftCol() {
        return leftCol;
    }

    public void setLeftCol(Integer leftCol) {
        this.leftCol = leftCol;
    }

    public Integer getRightCol() {
        return rightCol;
    }

    public void setRightCol(Integer rightCol) {
        this.rightCol = rightCol;
    }

    public ArrayList<String> getTupleList() {
        return tupleList;
    }

    public void setTupleList(ArrayList<String> tupleList) {
        this.tupleList = tupleList;
    }

    public HashTableWithDups getHashTDups() {
        return hashTDups;
    }

    public void setHashTDups(HashTableWithDups hashTDups) {
        this.hashTDups = hashTDups;
    }
  
  

} // public class HashJoin extends Iterator
