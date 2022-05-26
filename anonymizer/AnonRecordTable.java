package anonymizer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;

import sqlwrapper.QueryResult;
import sqlwrapper.SQLWrapper;
import sqlwrapper.SqLiteSQLWrapper;

/**
 * The class that manages anonymized records. Typical schema of the table contains: 
 * <ul>
 * 	<li> rid: record identifier (assigned automatically during data input)
 *  <li> eid: identifier of the equivalence class this tuple is currently generalized to 
 *  (specified at insertion)
 * 	<li> quasi-identifier attribute values (vector of doubles, one for each qi-attribute)
 *  <li> sensitive attribute values (vector of doubles, one for each sensitive attribute)
 * </ul>
 * All privacy definitions (e.g., k-anonymity, l-diversity, t-closeness) are implemented
 * within this class.
 */
public class AnonRecordTable {
	/** Indices of quasi-identifier attributes*/
	private Integer[] qidIndices;
	/** Indices of sensitive attributes*/
	private Integer[] sensIndices;
	/** Name of the table*/
	private String tableName;
	/** SQL connection/querying object*/
	private SQLWrapper sqlwrapper;
	
	/**
	 * Class constructor
	 * @param qidIndices Indices of quasi-identifier attributes
	 * @param sensIndices Indices of sensitive attributes
	 * @param tableName Name of the table
	 */
	public AnonRecordTable(Integer[] qidIndices, Integer[] sensIndices, String tableName) {
		this.qidIndices = qidIndices;
		this.sensIndices = sensIndices;
		this.tableName = tableName;
		sqlwrapper = SqLiteSQLWrapper.getInstance();
		createTable();
	}
	
//	/**
//	 * Class constructor (call only if the table is created before)
//	 * @param name Name of the table to be fetched
//	 * @param copyFrom AnonRecordTable from which the paremeters will be copied
//	 */
//	public AnonRecordTable(String name, AnonRecordTable copyFrom) {
//		this.qidIndices = copyFrom.qidIndices;
//		this.sensIndices = copyFrom.sensIndices;
//		this.tableName = name;
//		sqlwrapper = SqLiteSQLWrapper.getInstance();
//		//no calls to createTable - table already exists
//	}
	
	/**
	 * Creates the table that will store AnonRecord objects
	 */
	private void createTable() {
		//first, check if table exists
		String select_SQL = "SELECT NAME FROM SQLITE_MASTER WHERE NAME = '" + tableName + "'";
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
		if(result.hasNext()) { // if the table exists, drop table
			String drop_SQL = "DROP TABLE " + tableName;
			sqlwrapper.execute(drop_SQL);
		}
		
		//tableName(RID BIGINT, EID BIGINT,
		//  ATT_sens1, ..., ATT_sensM,
		//	ATT_qi1, ..., ATT_qiN, PRIMARY KEY(RID))
		String createTable_SQL = "CREATE TABLE " + tableName + 
			" (RID BIGINT PRIMARY KEY," +
			" EID BIGINT, ";
		for(int i = 0; i < qidIndices.length; i++) {
			createTable_SQL += "ATT_" + qidIndices[i] + " DOUBLE, ";
		}
		for(int i = 0; i < sensIndices.length; i++) {
			createTable_SQL += "ATT_" + sensIndices[i] + " DOUBLE, ";
		}
		//trim the last comma
		createTable_SQL = createTable_SQL.substring(0, createTable_SQL.length() - 2);
		createTable_SQL += ")";
		//execute update
		sqlwrapper.execute(createTable_SQL);
	}
	
	/**
	 * Getter for tableName
	 * @return Name of the table
	 */
	public String getName() {
		return tableName;
	}
	
	/**
	 * Get the number of AnonRecords generalized to the Equivalence with ID eid
	 * @param eid Equivalence ID
	 * @return Number of records r where r.eid = eid 
	 */
	public int getEquivalenceSize(long eid) throws SQLException{
		String select_SQL = "SELECT COUNT(*) FROM " + tableName +
			" WHERE EID = " + Long.toString(eid);
		//execute query
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
		if(result.hasNext()) {
			return ((ResultSet) result.next()).getInt(1);
		} else {
			return 0;
		}
	}
	
	/**
	 * Inserts new AnonRecord to the table (Record ID (RID) assigned automatically)
	 * @param eid Equivalence ID
	 * @param qiVals Quasi-identifier attribute values
	 * @param sensVals Sensitive attribute values (if not necessary, pass "new double[0]" as argument
	 */
	public void insert(long eid, double[] qiVals, double[] sensVals) throws SQLException{
		Long rid = new Long(0);
		//get the largest ID on the table
		String select_SQL = "SELECT MAX(RID) FROM " + tableName;
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
		if(result.hasNext()) {
			rid = ((ResultSet) result.next()).getLong(1);
			rid++;
		}
		
		String insert_SQL = "INSERT INTO " + tableName + " VALUES (" + rid + ", " + eid + ", ";
		for(int i = 0; i < qiVals.length; i++) {
			insert_SQL += qiVals[i] + ", ";
		}
		for(int i = 0; i < sensVals.length; i++) {
			insert_SQL += sensVals[i] + ", ";
		}
		//trim the last comma
		insert_SQL = insert_SQL.substring(0, insert_SQL.length()-2);
		insert_SQL += ")";
		//execute update
		sqlwrapper.execute(insert_SQL);
	}
	
	/**
	 * Checks the k-anonymity privacy definition
	 * @param k Privacy parameter
	 * @return True if k-anonymous (or empty), False otherwise
	 */
	public boolean checkKAnonymityRequirement(int k) throws SQLException{
		String select_SQL = "SELECT COUNT(*) FROM " + tableName 
			+ " GROUP BY EID ORDER BY COUNT(*) ASC";
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
		while(result.hasNext()) {
			Integer min = ((ResultSet) result.next()).getInt(1);
			if(min > 0 && min < k) {
				return false;
			} else {
				return true;
			}
		}
		return true; //no equivalences, therefore no records, therefore k-anonymous
	}
	
	/**
	 * Checks the k-anonymity privacy definition while considering the suppression threshold 
	 * (i.e., k-anonymity holds only if the total size of all equivalences with size less than
	 * k is below the suppression threshold)
	 * @param k Privacy parameter
	 * @param suppThreshold Suppression threshold
	 * @return True if k-anonymous (or empty), False otherwise
	 */
	public boolean checkKAnonymityRequirement(int k, int suppThreshold) throws SQLException {
		String select_SQL = "SELECT COUNT(*) FROM " + tableName 
			+ " GROUP BY EID ORDER BY COUNT(*) ASC";
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
		
		int sumLessThanK = 0;
		while(result.hasNext()) {
			Integer min = ((ResultSet) result.next()).getInt(1);
			if(min > 0 && min < k) {
				sumLessThanK += min;
				if(sumLessThanK > suppThreshold) {
					return false; //too many records for suppression, not ready yet
				}
			}
		}
		return true;
	}
	
	/**
	 * Checks the entropy l-diversity privacy definition
	 * @param l Privacy parameter
	 * @param sensIndex index of the sensitive attribute
	 * @return True if entropy l-diverse, False otherwise
	 * @throws SQLException
	 */
	public boolean checkLDiversityRequirement(double l, int sensIndex) throws SQLException {
		String select_SQL = "SELECT COUNT(*), EID FROM " + tableName
			+ " GROUP BY ATT_" + Integer.toString(sensIndex) + ", EID ORDER BY EID";
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
		
		long currEID; //EID of the current equivalence 
		int currCount; //count for the current sensitive value of the current equivalence
		long prevEID = Long.MIN_VALUE; //previous EID  value (used to check if EID has changed)
		ArrayList<Integer> counts = null;
		double sum = 0;
		
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			currEID = rs.getLong(2);
			currCount = rs.getInt(1);
			
			//if the EID has changed 
			// (i)  validate conformity with l-div
			// (ii) refresh the counts for the next equivalence
			if(currEID != prevEID) {
				//before refreshing, check if the condition is satisfied 
				// in the current state of counts
				if(counts != null) { //if this is not the first equivalence
					double entropy = 0;
					ListIterator<Integer> iter = counts.listIterator();
					while(iter.hasNext()) {
						double prob = iter.next() / sum;
						entropy += prob * Math.log(prob);
					}
					if(-1 * entropy < Math.log(l)) { //entropy l-div constraint
						return false;
					}
				}			
				//now re-initialize the object for the next equivalence
				counts = new ArrayList<Integer>();
				sum = 0;
				prevEID = currEID;
			}
			counts.add(currCount);
			sum += currCount;
		}
		
		//validate the condition on the last set of counts
		if(counts != null && counts.size() > 0) { //if this is not the first equivalence
			double entropy = 0;
			ListIterator<Integer> iter = counts.listIterator();
			while(iter.hasNext()) {
				double prob = iter.next() / sum;
				entropy += prob * Math.log(prob);
			}
			if(-1 * entropy < Math.log(l)) { //entropy l-div constraint
				return false;
			}
		}
		
		//no violation, return true
		return true;
	}
	
	/**
	 * Checks the recursive (c,l)-diversity privacy definition
	 * @param l Privacy parameter
	 * @param c Privacy parameter
	 * @param sensIndex index of the sensitive attribute
	 * @return True if (c,l)-diverse, False otherwise
	 * @throws SQLException
	 */
	public boolean checkLDiversityRequirement(double l, double c, int sensIndex) throws SQLException {
		String select_SQL = "SELECT COUNT(*), EID FROM " + tableName
			+ " GROUP BY ATT_" + Integer.toString(sensIndex) + ", EID ORDER BY EID";
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
	
		long currEID; //EID of the current equivalence 
		int currCount; //count for the current sensitive value of the current equivalence
		long prevEID = Long.MIN_VALUE; //previous EID  value (used to check if EID has changed)
		ArrayList<Integer> counts = null;
		
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			currEID = rs.getLong(2);
			currCount = rs.getInt(1);
			
			//if the EID has changed 
			// (i)  validate conformity with l-div
			// (ii) refresh the counts for the next equivalence
			if(currEID != prevEID) {
				//before refreshing, check if the condition is satisfied 
				// in the current state of counts
				if(counts != null) { //if this is not the first equivalence
					if(counts.size() < l) {
						return false;
					} else {
						Integer[] countVals = counts.toArray(new Integer[0]);
						Arrays.sort(countVals); //sort in ascending order
						int r_1 = countVals[countVals.length-1];
						int sum = 0;
						for(int i = (int) Math.round(countVals.length - l); i >= 0; i--) {
							sum += countVals[i];
						}
						if(r_1 >= c * sum) {
							return false;
						}
					}
				}				
				//now re-initialize the object for the next equivalence
				counts = new ArrayList<Integer>();
				prevEID = currEID;
			}
			counts.add(currCount);
		}
		
		//validate the condition on the last set of counts
		if(counts != null) { //if this is not the first equivalence
			if(counts.size() < l) {
				return false;
			} else {
				Integer[] countVals = counts.toArray(new Integer[0]);
				Arrays.sort(countVals); //sort in ascending order
				int r_1 = countVals[countVals.length-1];
				int sum = 0;
				for(int i = (int) Math.round(countVals.length - l); i >= 0; i--) {
					sum += countVals[i];
				}
				if(r_1 >= c * sum) {
					return false;
				}
			}			
		}
		
		//no violation, return true
		return true;
	}
	
	/**
	 * Checks the t-closeness privacy definition
	 * @param t Privacy parameter
	 * @param sensIndex index of a categorical sensitive attribute
	 * @param sensDomSize domain size of the sensitive attribute
	 * @return True if t-close, False otherwise
	 * @throws SQLException
	 */
	public boolean checkTClosenessRequirement_Cat(double t, int sensIndex, int sensDomSize) throws SQLException {
		if(sensDomSize == 1) { //quick and dirty check for the special case
			return true;
		}
		//compute the distribution over the entire table
		int[] entireDist = new int[sensDomSize];
		for(int i = 0; i < sensDomSize; i++) {
			entireDist[i] = 0;
		}
		
		String getDist_SQL = "SELECT COUNT(*), ATT_" + sensIndex + " FROM " + tableName
			+ " GROUP BY ATT_" + sensIndex;
		QueryResult result = sqlwrapper.executeQuery(getDist_SQL);
		
		//process the results
		double entireSize = 0;
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			entireDist[(int) rs.getDouble(2)] = rs.getInt(1);
			entireSize += rs.getInt(1);
		}
		
		//now compute the distribution over each equivalence
		long prevEID = -1;
		int[] currDist = null;
		String iterEqs_SQL = "SELECT EID, COUNT(*), ATT_" + sensIndex 
			+ " FROM " + tableName
			+ " GROUP BY EID, ATT_" + sensIndex
			+ " ORDER BY EID";
		result = sqlwrapper.executeQuery(iterEqs_SQL);
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			long currEID = rs.getLong(1);
			int count = rs.getInt(2);
			int attVal = (int) rs.getDouble(3); //no problem, since the values are from
										//catDomMapping
			//if the eid has changed
			// (i)  if this is not the first iteration, check the t-closeness constraint
			// (ii) refresh the counts for the next equivalence
			if(prevEID != currEID) {
				//verify conformance with t-closeness using currDist and entireDist
				if(prevEID != -1) {
					double currEIDsize = getEquivalenceSize(prevEID); 
					double sum = 0;
					for(int i = 0; i < entireDist.length; i++) {
						sum += Math.abs(entireDist[i]/entireSize - currDist[i]/currEIDsize);
					}
					sum /= 2;
					if(sum > t) {
						return false;
					}
				}
				//refresh the distribution object
				currDist = new int[sensDomSize];
				for(int i = 0; i < sensDomSize; i++) {
					currDist[i] = 0;
				}
				//reset the prevID
				prevEID = currEID;
			}
			//update distribution
			currDist[attVal] = count;
		}
		
		//repeat the check for the last equivalence
		if(currDist!= null && prevEID != -1) {
			double sum = 0; //verify conformance with t-closeness using currDist and entireDist
			double eqSize = getEquivalenceSize(prevEID);
			for(int i = 0; i < entireDist.length; i++) {
				sum += Math.abs(entireDist[i]/entireSize - currDist[i]/eqSize);
			}
			sum /= 2;
			if(sum > t) {
				return false;
			}
		}
		
		//no violation, return true
		return true;
	}
	
	/**
	 * Checks the t-closeness privacy definition
	 * @param t Privacy parameter
	 * @param sensIndex index of a categorical sensitive attribute
	 * @return True if t-close, False otherwise
	 * @throws SQLException
	 */
	public boolean checkTClosenessRequirement_Num(double t, int sensIndex) throws SQLException {
		LinkedList<Double[]> entireCounts = new LinkedList<Double[]>();
		String getDist_SQL = "SELECT COUNT(*), ATT_" + sensIndex + " FROM " + tableName
			+ " GROUP BY ATT_" + sensIndex + " ORDER BY ATT_" + sensIndex;
		QueryResult result = sqlwrapper.executeQuery(getDist_SQL);
	
		double entireSize = 0; //necessary to convert counts to probabilities
		//process the results
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			Double[] entry = new Double[2];
			entry[0] = rs.getDouble(2); //first element will be the value
			entry[1] = new Double(rs.getInt(1)); //second will be the count
			//add this element to the list
			entireCounts.add(entry);
			entireSize += entry[1];
		}
		
		//now check t-closeness over each equivalence
		String iterEqs_SQL = "SELECT DISTINCT(EID) FROM " + tableName;
		result = sqlwrapper.executeQuery(iterEqs_SQL);
		while(result.hasNext()) {
			long eid = ((ResultSet) result.next()).getLong(1);
			double eqSize = getEquivalenceSize(eid); //necessary to convert counts to probs
			
			//get the distribution
			getDist_SQL = "SELECT COUNT(*), ATT_" + sensIndex 
				+ " FROM " + tableName
				+ " WHERE EID = " + eid 
				+ " GROUP BY ATT_" + sensIndex
				+ " ORDER BY ATT_" + sensIndex;
			QueryResult subResult = sqlwrapper.executeQuery(getDist_SQL);
			
			double sumDist = 0; //total distance so far
			double sumNoAbsolute = 0; //sum of distances w/o the absolute value 
								//(i.e., at any currIndex, \Sum_{i = 1}^currIndex r_i) 
			int m = entireCounts.size(); //domain size
			double boundary = t * (m - 1); //maximum distance allowed
			
			//we will iterate over the entire counts, calculating r_i and 
			// the ordered_dist between the last two elements
			ListIterator<Double[]> iter = entireCounts.listIterator();
			while(subResult.hasNext()) {
				//store the current values
				Double[] tableCurr = iter.next();
				ResultSet srs = (ResultSet) subResult.next();
				Double eqCurr = srs.getDouble(2);
				int eqCount = srs.getInt(1);
				
				//we know for sure that eq elements is a subset of the table elements
				// therefore, not need to check if eqCurr < tableCurr
				// similarly, no need worry about tableCurr running out before the condition is met
				while(tableCurr[0].compareTo(eqCurr) != 0) {
					//we know that q_i = 0 for the equivalence
					// therefore r_i = p_i - q_i = p_i = tableCurr[1] / entireSum
					double r_i = tableCurr[1] / entireSize;
					sumNoAbsolute += r_i;
					if(sumDist > boundary) { //check before adding, 
						return false;		 // so that only m-1 additions are accounted for
					}
					sumDist += Math.abs(sumNoAbsolute);
					tableCurr = iter.next(); //here, no need to call iter.hasNext()
				}
				//at this point, we know that tableCurr[0] == eqCurr
				//p_i = tableCurr[1] / entireSum
				//q_i = eqCount / eqCount
				//r_i = p_i - q_i
				double r_i = tableCurr[1] / entireSize - eqCount / eqSize;
				sumNoAbsolute += r_i;
				if(sumDist > boundary) {
					return false;
				}
				sumDist += Math.abs(sumNoAbsolute);
			}
			//now take care of any items still remaining in the entire distribution
			while(iter.hasNext()) {
				//we know that q_i = 0 for the equivalence
				// therefore r_i = p_i - q_i = p_i = iter.next()[1] / entireSum
				double r_i = iter.next()[1] / entireSize;
				sumNoAbsolute += r_i;
				if(sumDist > boundary) {
					return false;
				}
				sumDist += Math.abs(sumNoAbsolute);
			}
		}
		return true;
	}
	
	/**
	 * Moves records of one equivalence to the other, effectively deleting
	 *  the fromEID
	 * @param fromEID Source of the records to be moved
	 * @param toEID Destination of the records to be moved
	 */
	public void moveRecords(Long fromEID, Long toEID) {
		String update_SQL = "UPDATE " + tableName + " SET EID = " + toEID
			+ " WHERE EID = " + fromEID;
		sqlwrapper.execute(update_SQL);
	}
	
	/**
	 * From table that, copy the records with EID = oldEID into this
	 *  table, overwriting oldEID as newEID
	 * @param that Another AnonRecordTable
	 * @param oldEID EID of the records to be copied
	 * @param newEID New EID to be assigned to the copied records
	 */
	public void copyFrom(AnonRecordTable that, Long oldEID, Long newEID) {
		String insert_SQL = "INSERT INTO " + tableName + " SELECT RID, " 
			+ newEID; //overwrite newEID as the EID column value
		//select all other attributes
		for(int i = 0; i < qidIndices.length; i++) {
			insert_SQL += ", ATT_" + qidIndices[i];
		}
		for(int i = 0; i < sensIndices.length; i++) {
			insert_SQL += ", ATT_" + sensIndices[i];
		}
		insert_SQL += " FROM " + that.getName() + " WHERE EID = " + oldEID;
		sqlwrapper.execute(insert_SQL);
	}
	
	/**
	 * (1) From table that, copy the records with EID = oldEID into this
	 *  table, overwriting oldEID as newEID and 
	 *  (2) From table that, delete the records with EID = oldEID
	 * @param that Another AnonRecordTable
	 * @param oldEID EID of the records to be copied
	 * @param newEID New EID to be assigned to the copied records
	 */
	public void cutFrom(AnonRecordTable that, Long oldEID, Long newEID) {
		String insert_SQL = "INSERT INTO " + tableName + " SELECT RID, " 
			+ newEID; //overwrite newEID as the EID column value
		//select all other attributes
		for(int i = 0; i < qidIndices.length; i++) {
			insert_SQL += ", ATT_" + qidIndices[i];
		}
		for(int i = 0; i < sensIndices.length; i++) {
			insert_SQL += ", ATT_" + sensIndices[i];
		}
		insert_SQL += " FROM " + that.getName() + " WHERE EID = " + oldEID;
		sqlwrapper.execute(insert_SQL);
		
		String delete_SQL = "DELETE FROM " + that.tableName + " WHERE EID = " + oldEID;
		sqlwrapper.execute(delete_SQL);
	}
	
	/**
	 * (1) From table that, copy the record with the specified RID into this
	 * table, overwriting oldEID as newEID and 
	 * (2) From table that, delete the records wth the speicified RID
	 * @param that Another AnonRecordTable
	 * @param RID record ID of the tuple to be moved
	 * @param newEID new EID to be assigned to the copied record
	 */
	public void cutRecord(AnonRecordTable that, Long RID, Long newEID) {
		String insert_SQL = "INSERT INTO " + tableName + " SELECT RID, " 
			+ newEID; //overwrite newEID as the EID column value
		//select all other attributes
		for(int i = 0; i < qidIndices.length; i++) {
			insert_SQL += ", ATT_" + qidIndices[i];
		}
		for(int i = 0; i < sensIndices.length; i++) {
			insert_SQL += ", ATT_" + sensIndices[i];
		}
		insert_SQL += " FROM " + that.tableName + " WHERE RID = " + RID;
		sqlwrapper.execute(insert_SQL);
		
		String delete_SQL = "DELETE FROM " + that.tableName + " WHERE RID = " + RID;
		sqlwrapper.execute(delete_SQL);
	}
	
	/**
	 * Drops this table from the database
	 */
	public void drop() {
		String drop_SQL = "DROP TABLE " + tableName;
		sqlwrapper.execute(drop_SQL);
	}
}
