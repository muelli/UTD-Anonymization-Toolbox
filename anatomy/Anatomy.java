package anatomy;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;

import sqlwrapper.QueryResult;
import sqlwrapper.SqLiteSQLWrapper;
import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;

/**
 * Implementation of the Anatomy anonymization algorithm. For details on Anatomy, please refer to 
 * the following paper:
 * 
 * <pre>
 * &#64;inproceedings{anatomy,
 * author = {Xiao, Xiaokui and Tao, Yufei},
 * title = {Anatomy: simple and effective privacy preservation},
 * booktitle = {VLDB '06: Proceedings of the 32nd international conference on Very large data bases},
 * year = {2006},
 * pages = {139--150},
 * location = {Seoul, Korea},
 * publisher = {VLDB Endowment}
 * }
 * </pre>
 * 
 * The algorithm is described in Section 5.2 of the paper. The specific l-diversity 
 * definition supported by the algorithm is quite different than those described in the 
 * l-diversity paper. The output produces equivalence classes of size at least l, each of 
 * which contains at least l distinct sensitive values.
 * <p/>
 * Only integer valued l values are acceptable. Constructor performs the necessary check,
 * and convert l to an integer value if needed. 
 * <p/>
 * Property I of Section 5.2 in the paper mentions an eligibility condition. If this condition
 * does not hold with the input dataset, the algorithm simply throws an exception and exits.
 */
public class Anatomy extends Anonymizer {		
	/**
	 * Class constructor
	 * @param conf Configuration instance
	 */
	public Anatomy(Configuration conf) throws Exception{
		super(conf);
		
		if(conf.l <= 0) { //validate input l, the privacy parameter
			throw new Exception("Anatomy: Parameter l should be set in the configuration file!!");
		} else if(conf.l <= 1) {
			throw new Exception("Anatomy: Setting 0 < l <= 1 does not make sense!!");
		}
		//make sure that l is an integer
		int lInt = (int) Math.round(conf.l);
		if(lInt != conf.l) {
			if(lInt <= 1) {
				lInt = 2;
			}
			System.out.println("Converting rational l value to integer: " + lInt);
			conf.l = lInt;
		}
		//check the number of sensitive attributes
		if(conf.sensitiveAtts.length != 1) {
			throw new Exception("Anatomy: Set 1 (and only 1) sensitive attribute!!");
		}
		sqlwrapper = SqLiteSQLWrapper.getInstance(); //check DB connectivity
		
		//create tables
		eqTable = createEquivalenceTable("eq_init"); 
		anonTable = createAnonRecordsTable("an_init");
	}
	
	/**
	 * Overwrites the privacy parameter l
	 * @param newLvalue New value of l
	 */
	public void changeLDiversityRequirement(double newLvalue) {
		this.conf.l = newLvalue;
	}
	
	/**
	 * Create equivalence table
	 * @param tableName Name of the table
	 * @return A new equivalence table
	 */
	protected EquivalenceTable createEquivalenceTable(String tableName) {
		return new EquivalenceTable(conf.qidAtts, tableName);
	}
	
	/**
	 * Create anonRecords table
	 * @param tableName Name of the table
	 * @return A new anonymization records table
	 */
	protected AnonRecordTable createAnonRecordsTable(String tableName) {
		//list qi-attribute and sensitive attribute indices within the original source
		Integer[] qiAtts = new Integer[conf.qidAtts.length];
		for(int i = 0; i < qiAtts.length; i++) {
			qiAtts[i] = conf.qidAtts[i].index;
		}
		Integer[] sensAtts = new Integer[1];
		sensAtts[0] = conf.sensitiveAtts[0].index;
		return new AnonRecordTable(qiAtts, sensAtts, tableName);
	}
	
	/**
	 * Insert a tuple to the equivalence table
	 * @param vals All values of the tuple to be inserted (as read from the source, i.e., before any generalization)
	 * @return Equivalence id of the equivalence to which the tuple belongs
	 * @throws Exception
	 */
	protected long insertTupleToEquivalenceTable(String[] vals) throws Exception{
		return eqTable.insertTuple(vals);
	}
	
	/**
	 * Insert a tuple to the anonRecords table
	 * @param vals All values of the tuple to be inserted (as read from the source, i.e., before any generalization)
	 * @param eid Equivalence id of the equivalence to which the tuple belongs
	 * @throws Exception
	 */
	protected void insertTupleToAnonTable(String[] vals, long eid) throws Exception{
		//parse qi-attribute values
		double[] qiVals = new double[conf.qidAtts.length];
		for(int i = 0; i < conf.qidAtts.length; i++) {
			String attVal = vals[conf.qidAtts[i].index];
			if(conf.qidAtts[i].catDomMapping != null) {
				qiVals[i] = conf.qidAtts[i].catDomMapping.get(attVal);
			} else {
				qiVals[i] = Double.parseDouble(attVal);
			}
		}
		//parse sensitive attribute value
		String sensitiveValue = vals[conf.sensitiveAtts[0].index];
		double[] sensVals = new double[1];
		if(conf.sensitiveAtts[0].catDomMapping != null) {
			sensVals[0] = conf.sensitiveAtts[0].catDomMapping.get(sensitiveValue);
		} else {
			sensVals[0] = Double.parseDouble(sensitiveValue);
		}
		anonTable.insert(eid, qiVals, sensVals);
	}
	
	/**
	 * Anonymizing the input. This function can be reused (with new l maybe), as long as 
	 * eqTable and anonTable objects are fresh and the configuration has 
	 * not changed.
	 * @throws Exception
	 */
	public void anonymize() throws Exception{
		//these will replace the initial tables at the end of the iterations
		AnonRecordTable anReady = createAnonRecordsTable("an_ready");
		EquivalenceTable eqReady = createEquivalenceTable("eq_ready");
		
		//get counts for each sensitive attribute value
		LinkedList<AttValueCount> sensCountList = new LinkedList<AttValueCount>();
		String count_SQL = "SELECT ATT_" + conf.sensitiveAtts[0].index + ", COUNT(*)"
			+ " FROM " + anonTable.getName()
			+ " GROUP BY ATT_" + conf.sensitiveAtts[0].index
			+ " ORDER BY COUNT(*)";
		QueryResult result = sqlwrapper.executeQuery(count_SQL);
		double total = 0;
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			sensCountList.add(new AttValueCount(rs.getDouble(1), rs.getInt(2)));
			total += rs.getInt(2);
		}
		//convert to array and sort
		AttValueCount[] sensCounts = sensCountList.toArray(new AttValueCount[0]);
		Arrays.sort(sensCounts);
		
		//quick and dirty check for satisfiability of the privacy definition
		if(sensCounts.length < conf.l) {
			throw new Exception("Anatomy l-diversity cannot be satisfied on this dataset!!!");
		}
		//eligibility condition of property I in Section 5.2
		boolean eligible = true;
		for(int i = 0; i < sensCounts.length; i++) {
			if(sensCounts[i].getCount()/total > 1/conf.l) {
				eligible = false;
			}
		}
		if(!eligible) {
			throw new Exception("This dataset is not eligible for Anatomy!!!");
		}
		
		
		//create the generic generalized values vector
		String[] genVals = new String[conf.qidAtts.length];
		for(int i = 0; i < genVals.length; i++) {
			genVals[i] = conf.qidAtts[i].getSup();
		}
		
		//start iterations - group creation step
		int numIterations = 1;
		Random rand = new Random(3345);
		while(existsLNonEmptyBuckets(sensCounts, conf.l)) {
			//select the largest l buckets
			for(int i = 0; i < conf.l; i++) {
				//create new equivalence
				long eid = numIterations;
				double sensVal = sensCounts[i].getValue();
				
				//select a random tuple with value sensVal
				int index = rand.nextInt(sensCounts[i].getCount());
				String selTuple_SQL = "SELECT RID FROM " + anonTable.getName()
					+ " WHERE ATT_" + conf.sensitiveAtts[0].index + " = " + sensVal
					+ " LIMIT 1 OFFSET " + index;
				long rid = -1;
				result = sqlwrapper.executeQuery(selTuple_SQL);
				if(result.hasNext()) {
					rid = ((ResultSet) result.next()).getLong(1);
				}
				
				//insert the tuple to anReady with EID = eid
				anReady.cutRecord(anonTable, rid, eid);
			}
			numIterations++;
			decrementFirstLCounts(sensCounts, conf.l);
		}
		//residue assignment step
		for(int i = 0; i < sensCounts.length; i++) {
			if(sensCounts[i].getCount() > 0) {
				double sensVal = sensCounts[i].getValue();
				
				//select the only tuple with value sensVal
				String selTuple_SQL = "SELECT RID FROM " + anonTable.getName()
					+ " WHERE ATT_" + conf.sensitiveAtts[0].index + " = " + sensVal;
				long rid = -1;
				result = sqlwrapper.executeQuery(selTuple_SQL);
				if(result.hasNext()) {
					rid = ((ResultSet) result.next()).getLong(1);
				}
				
				//randomly select an equivalence that does not contain sensVal
				//get the count first
				selTuple_SQL = "SELECT EID FROM " + anReady.getName()
					+ " GROUP BY EID"
					+ " HAVING COUNT(ATT_" + conf.sensitiveAtts[0].index + " = " + sensVal + ") < 1";
				String selCount_SQL = "SELECT COUNT(*) FROM (" + selTuple_SQL + ") AS T";
				int count = ((ResultSet) sqlwrapper.executeQuery(selCount_SQL).next()).getInt(1);
				int index = rand.nextInt(count);
				//select the corresponding eid
				selTuple_SQL = selTuple_SQL	+ " LIMIT 1 OFFSET " + index;
				long eid = ((ResultSet) sqlwrapper.executeQuery(selTuple_SQL).next()).getLong(1);
				
				//insert this tuple to that equivalence
				anReady.cutRecord(anonTable, rid, eid);
			}
		}
		
		//finished, overwrite the initial tables with the results
		anReady.drop();
		eqReady.drop();
		anonTable = anReady;
		eqTable = eqReady;
	}
	
	/**
	 * Decrements the counts of the first l most frequent sensitive attribute values
	 * @param counts Counts of each sensitive attribute value
	 * @param l Privacy parameter l
	 */
	private void decrementFirstLCounts(AttValueCount[] counts, double l) {
		for(int i = 0; i < l; i++) {
			counts[i].decrement();
		}
		//make sure that the array remains sorted
		Arrays.sort(counts);
	}
	
	/**
	 * Check if there are at least l non-empty buckets of sensitive attribute values
	 * (iteration condition)
	 * @param counts Counts of each sensitive attribute value
	 * @param l Privacy parameter l
	 * @return true if the condition holds, false otherwise
	 */
	private boolean existsLNonEmptyBuckets(AttValueCount[] counts, double l) {
		for(int i = 0; i < l; i++) {
			if(counts[i].getCount() <= 0) {
				return false;
			}
		}
		return true;
	}
}
