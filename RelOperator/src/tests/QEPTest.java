package tests;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.Tuple;

// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {
	private static final String TEST_NAME = "part 3 tests";

	/** Size of tables in test3. */
	private static final int SUPER_SIZE = 2000;

	/** Drivers table schema. */
	private static Schema emp;

	/** Rides table schema. */
	private static Schema dep;

	/** Groups table schema. */
	//private static Schema s_groups;




	public static void main(String argv[]) {
	
	
	
	QEPTest qep = new QEPTest();
	qep.create_minibase();
	
	emp = new Schema(5);
	
	emp.initField(0,AttrType.INTEGER,4,"EmpId");
	emp.initField(1,AttrType.STRING,30,"Name");
	emp.initField(2,AttrType.INTEGER,4,"Age");
	emp.initField(3,AttrType.INTEGER,10,"Salary");
	emp.initField(4,AttrType.INTEGER,4,"DeptId");

	dep = new Schema(4);
	dep.initField(0,AttrType.INTEGER,4,"DeptId");
	dep.initField(1,AttrType.STRING,30,"Name");
	dep.initField(2,AttrType.INTEGER,4,"MinSalary");
	dep.initField(3,AttrType.INTEGER,10,"MaxSalary");	
	
	System.out.println("\n" + "Running " + TEST_NAME + "...");
	boolean status = PASS;
	
	status &= qep.test1(); //works
	status&=qep.test2();
	status&=qep.test3();
	status&=qep.test4();
	
	System.out.println();
	if (status != PASS) {
		System.out.println("Error(s) encountered during " + TEST_NAME + ".");
	} else {
		System.out.println("All " + TEST_NAME
				+ " completed; verify output for correctness.");
	}
	
	
	
	}

	protected boolean test1() {
		
		try{
			System.out.println("\nTest 1: Select Employee Id, Name and Age");
			Tuple tuple = new Tuple(emp);
			initCounts();
			saveCounts(null);
			HeapFile file = new HeapFile(null);
			HashIndex index = new HashIndex(null);
			int c = 0;
			int age = 0;
			//FileInputStream in = new FileInputStream("Employee.txt");
			BufferedReader br = new BufferedReader(new FileReader("src/tests//SampleData/Employee.txt"));
			String line;
			while((line=br.readLine())!=null)
			{
				if(c==0)
				{
					c++;
					continue;
				}
				else
				{
					
					String[] a = new String[5];
					//System.out.println(a);
					a = line.split(", ");
					//System.out.println(a[0]+" "+a[1]+ " "+a[2]+ " " + a[3] + " "+a[4]);
					

					tuple.setIntFld(0, Integer.parseInt(a[0]));
					tuple.setStringFld(1, a[1]);
					tuple.setIntFld(2, Integer.parseInt(a[2]));
					tuple.setIntFld(3, Integer.parseInt(a[3]));
					tuple.setIntFld(4, Integer.parseInt(a[4]));
						
					RID rid = file.insertRecord(tuple.getData());
					index.insertEntry(new SearchKey(age), rid);
					age++;	
				}
				
			}

			c=0;
			saveCounts("insert");

			// test index scan
			saveCounts(null);
			
			System.out.println("\n  ~> test projection");
			FileScan scan = new FileScan(emp, file);
			Projection pro = new Projection(scan, 0,1,2);
			pro.execute();
			saveCounts("project");
			saveCounts(null);
			//sel = null;
			pro = null;
			System.out.print("\n\nTest 1 completed without exception.");
			return PASS;
			
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
			System.out.print("\n\nTest 1 terminated because of exception.");
			return FAIL;
		}
		finally
		{
			printSummary(6);
			System.out.println();
		}
	
	}

	protected boolean test2() {

		try {
			System.out.println("\nTest 2: Select Name of department where maxSalary = minSalary");
			Tuple tuple = new Tuple(dep);
			
			initCounts();
			saveCounts(null);
			HeapFile file = new HeapFile(null);
			HashIndex index = new HashIndex(null);
			int c = 0;
			int deptId = 0;
			BufferedReader br = new BufferedReader(new FileReader("src/tests//SampleData/Department.txt"));
			String line;
			while((line=br.readLine())!=null)
			{
				if(c==0)
				{
					c++;
					continue;
				}
				else
				{
					
					String[] a = new String[4];
					//System.out.println(a);
					a = line.split(", ");
					//System.out.println(a[0]+" "+a[1]+ " "+a[2]+ " " + a[3] + " "+a[4]);
					

					tuple.setIntFld(0, Integer.parseInt(a[0]));
					tuple.setStringFld(1, a[1]);
					tuple.setIntFld(2, Integer.parseInt(a[2]));
					tuple.setIntFld(3, Integer.parseInt(a[3]));
					deptId = Integer.parseInt(a[0]);	
					RID rid = file.insertRecord(tuple.getData());
					index.insertEntry(new SearchKey(deptId), rid);
					//deptId++;	
				}
				
			}

			c=0;
			saveCounts("insert");

			// test index scan
			saveCounts(null);
			
			Predicate[] preds = new Predicate[] {
					new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 3, AttrType.FIELDNO,2
							) };
			FileScan scan = new FileScan(dep, file);
			Selection sel = new Selection(scan, preds);
			Projection pro = new Projection(sel, 1);
			pro.execute();
			saveCounts("both");

			// test join operator
			saveCounts(null);
			sel = null;
			pro = null;
			System.out.print("\n\nTest 2 completed without exception.");
			return PASS;
			
		}
		catch(Exception e) {
			e.printStackTrace(System.out);
			System.out.print("\n\nTest 2 terminated because of exception.");
			return FAIL;
		}
		finally {
			printSummary(6);
			System.out.println();
		}
	}
	
	protected boolean test3() {
		try{
			System.out.println("\nTest 3: Select Name of Employee, Department name and max salary");

			initCounts();
			saveCounts(null);

			// create and populate a temporary Drivers file and index
			Tuple tupleE = new Tuple(emp);
			Tuple tupleD = new Tuple(dep);
			HeapFile fileE = new HeapFile(null);
			HeapFile fileD = new HeapFile(null);
			HashIndex indexE = new HashIndex(null);
			HashIndex indexD = new HashIndex(null);

			int c = 0;
			int deptId = 0;
			BufferedReader brD = new BufferedReader(new FileReader("src/tests//SampleData/Department.txt"));
			BufferedReader brE = new BufferedReader(new FileReader("src/tests//SampleData/Employee.txt"));
			
			String line;
			while((line=brD.readLine())!=null)
			{
				if(c==0)
				{
					c++;
					continue;
				}
				else
				{
					
					String[] a = new String[4];
					//System.out.println(a);
					a = line.split(", ");
					//System.out.println(a[0]+" "+a[1]+ " "+a[2]+ " " + a[3] + " "+a[4]);
					

					tupleD.setIntFld(0, Integer.parseInt(a[0]));
					tupleD.setStringFld(1, a[1]);
					tupleD.setIntFld(2, Integer.parseInt(a[2]));
					tupleD.setIntFld(3, Integer.parseInt(a[3]));
					deptId = Integer.parseInt(a[0]);	
					RID rid = fileD.insertRecord(tupleD.getData());
					indexD.insertEntry(new SearchKey(deptId), rid);
					//deptId++;	
				}
				
			}

			c=0;
			saveCounts("fileD");
			saveCounts(null);

			
			while((line=brE.readLine())!=null)
			{
				if(c==0)
				{
					c++;
					continue;
				}
				else
				{
					
					String[] a = new String[5];
					//System.out.println(a);
					a = line.split(", ");
					//System.out.println(a[0]+" "+a[1]+ " "+a[2]+ " " + a[3] + " "+a[4]);
					

					tupleE.setIntFld(0, Integer.parseInt(a[0]));
					tupleE.setStringFld(1, a[1]);
					tupleE.setIntFld(2, Integer.parseInt(a[2]));
					tupleE.setIntFld(3, Integer.parseInt(a[3]));
					tupleE.setIntFld(4, Integer.parseInt(a[4]));
					deptId = Integer.parseInt(a[0]);	
					//tupleE.insertIntoFile(Employee);
					tupleE.insertIntoFile(fileE);
					RID rid = fileE.insertRecord(tupleE.getData());
					indexE.insertEntry(new SearchKey(deptId), rid);
					//deptId++;	
				}
				
			}

			c=0;
			saveCounts("fileE");
			saveCounts(null);
			
			HashJoin join1 = new HashJoin(new FileScan(emp,fileE),new FileScan(dep,fileD),4,0);
			Predicate[] preds = new Predicate[] {
					new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO,5
							) };
			Selection sel = new Selection(join1,preds);
			
			Projection pro = new Projection(sel,1,6 ,8);
			pro.execute();
			saveCounts("both");
			//set join1 to null including sel preds and pro	
			// test join operator
			saveCounts(null);
			
			System.out.print("\n\nTest 3 completed without exception.");
			join1 = null;
			sel = null;
			pro = null;
			
			
			return PASS;
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
			System.out.print("\n\nTest 3 terminated because of exception.");
			return FAIL;
		}
		finally
		{
			printSummary(6);
			System.out.println();
		}
		
		
	}

	protected boolean test4()	
	{
		try {
			System.out.println("\nTest 4: Select Name of Employee whose salary is greater than max salary");

			initCounts();
			saveCounts(null);

			// create and populate a temporary Drivers file and index
			Tuple tupleE = new Tuple(emp);
			Tuple tupleD = new Tuple(dep);
			HeapFile fileE = new HeapFile(null);
			HeapFile fileD = new HeapFile(null);
			HashIndex indexE = new HashIndex(null);
			HashIndex indexD = new HashIndex(null);

			int c = 0;
			int deptId = 0;
			BufferedReader brD = new BufferedReader(new FileReader("src/tests//SampleData/Department.txt"));
			BufferedReader brE = new BufferedReader(new FileReader("src/tests//SampleData/Employee.txt"));
			
			String line;
			while((line=brD.readLine())!=null)
			{
				if(c==0)
				{
					c++;
					continue;
				}
				else
				{
					
					String[] a = new String[4];
					//System.out.println(a);
					a = line.split(", ");
					//System.out.println(a[0]+" "+a[1]+ " "+a[2]+ " " + a[3] + " "+a[4]);
					

					tupleD.setIntFld(0, Integer.parseInt(a[0]));
					tupleD.setStringFld(1, a[1]);
					tupleD.setIntFld(2, Integer.parseInt(a[2]));
					tupleD.setIntFld(3, Integer.parseInt(a[3]));
					deptId = Integer.parseInt(a[0]);	
					RID rid = fileD.insertRecord(tupleD.getData());
					indexD.insertEntry(new SearchKey(deptId), rid);
					//deptId++;	
				}
				
			}

			c=0;
			saveCounts("fileD");
			saveCounts(null);

			
			while((line=brE.readLine())!=null)
			{
				if(c==0)
				{
					c++;
					continue;
				}
				else
				{
					
					String[] a = new String[5];
					//System.out.println(a);
					a = line.split(", ");
					//System.out.println(a[0]+" "+a[1]+ " "+a[2]+ " " + a[3] + " "+a[4]);
					

					tupleE.setIntFld(0, Integer.parseInt(a[0]));
					tupleE.setStringFld(1, a[1]);
					tupleE.setIntFld(2, Integer.parseInt(a[2]));
					tupleE.setIntFld(3, Integer.parseInt(a[3]));
					tupleE.setIntFld(4, Integer.parseInt(a[4]));
					deptId = Integer.parseInt(a[0]);	
					//tupleE.insertIntoFile(Employee);
					tupleE.insertIntoFile(fileE);
					RID rid = fileE.insertRecord(tupleE.getData());
					indexE.insertEntry(new SearchKey(deptId), rid);
					//deptId++;	
				}
				
			}

			c=0;
			saveCounts("fileE");
			saveCounts(null);
			
			HashJoin join1 = new HashJoin(new FileScan(emp,fileE),new FileScan(dep,fileD),4,0);
			Predicate[] preds = new Predicate[] {
					new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FIELDNO,8
							) };
			Selection sel = new Selection(join1,preds);
			
			Projection pro = new Projection(sel,1);
			pro.execute();
			saveCounts("both");
			
			
			saveCounts(null);
			join1 = null;
			sel = null;
			pro = null;
			System.out.print("\n\nTest 4 completed without exception.");

			
			return PASS;
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
			System.out.print("\n\nTest 4 terminated because of exception.");
			return FAIL;	
		}
		finally
		{
			printSummary(6);
			System.out.println();
		}
	}

}
