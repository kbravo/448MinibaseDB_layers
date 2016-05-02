package bufmgr;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.io.IOException;
import java.util.*;

import chainexception.ChainException;
 
class Hashtable {
	//constant for table size
	private final static int TABLE_SIZE = 101;

	static public class HashEntry {
		private PageId key;
		private int value;
		
		public HashEntry() {
			super();
			this.key = new PageId();
			this.setValue(-1);
		}
		public HashEntry(PageId key, int value) {
			super();
			this.setKey(key);
			this.value = value;
		}
		
		public PageId getKey() {
			return key;
		}
		
		public void setKey(PageId key) {
			this.key.copyPageId(key);
		}
      
		public int getValue() {
			return value;
		}
		
		public void setValue(int value) {
			this.value = value;
		} 	   
	}

	static public class Bucket {
		LinkedList<HashEntry> array;
		public Bucket() { 
			array = new LinkedList<HashEntry>();
		}
	}
	
    static Bucket[] directory;

	public Hashtable()
	{
		directory = new Bucket[TABLE_SIZE];
		for(int i = 0; i < TABLE_SIZE; i++) {
			directory[i] = new Bucket();
		}

	}

	public int hash(PageId pageNumber)
	{
		int a = 3;
		int b = 7; 
      	int hash = (a*pageNumber.pid)+b;
      	return hash % TABLE_SIZE;
	}
	

	public void put(PageId pageNumber, int frameNumber)
	{
	   int hash_code = hash(pageNumber);
	   HashEntry entry = new HashEntry();
	   entry.setKey(pageNumber);
	   entry.setValue(frameNumber);
	   directory[hash_code].array.add(entry);
	}
	
	
	public int get(PageId pageNumber)
	{
		int hash_code = hash(pageNumber);
		for(HashEntry e : directory[hash_code].array) {
			if(e.key.equals(pageNumber)) {
				return e.value;
			}
		}
		return -1;
	}

	
	public boolean contains(PageId pageNumber)
	{
		int hash_code = hash(pageNumber);
		for(HashEntry e : directory[hash_code].array) {
			if(e.key.equals(pageNumber)) {
				return true;
			}
		}
		return false;
	}
	

	public void remove(PageId pageNumber) 
	{
		int hash_code = hash(pageNumber);
		int i = 0;
		for(HashEntry e : directory[hash_code].array) {
			if(pageNumber.equals(e.key)) {
				directory[hash_code].array.remove(i);
				break;
			}
			i++;
		}
	}
}