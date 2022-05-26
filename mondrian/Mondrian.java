package mondrian;

import java.sql.ResultSet;

import sqlwrapper.QueryResult;
import sqlwrapper.SqLiteSQLWrapper;
import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;
import anonymizer.Interval;

/**
 * Implementation of the Mondrian multi-dimensional partitioning algorithm for 
 * satisfying k-anonymity described in the following paper:
 * 
 * <pre>
 * &#64;article{mondrian,
 * author = {Kristen Lefevre and David J. Dewitt and Raghu Ramakrishnan},
 * title = {Mondrian multidimensional k-anonymity},
 * booktitle = {In ICDE},
 * year = {2006}
 * }
 * </pre>
 * 
 * Our dimension selection heuristic is the one described in Section 4 of the paper. Namely,
 * at each iteration, we "choose the dimension with the widest (normalized) range of values".
 * <p/>
 * When multiple dimensions have the same width, our implementation chooses the first dimension
 * that contains an allowable cut. 
 * <p/>
 * Given dimension, partitions are built by splitting the domain on the median value. If med
 * represents the median, LHS contains all values &lt= med and RHS contains all values &gt med.
 */
public class Mondrian extends Anonymizer{
	/** Generalized values for the suppression equivalence*/
	private String[] suppEq;
	
	/**
	 * Class constructor
	 * @param conf Configuration instance
	 */
	public Mondrian(Configuration conf) throws Exception{
		super(conf);
		
		if(conf.k <= 0) { //validate input k, the privacy parameter
			throw new Exception("Mondrian: Parameter k should be set in the configuration file!!");
		}

		//set suppression generalized values through suppEq
		suppEq = new String[conf.qidAtts.length];
		for(int i = 0; i < suppEq.length; i++) {
			suppEq[i] = conf.qidAtts[i].getSup();
		}
		
		sqlwrapper = SqLiteSQLWrapper.getInstance(); //check DB connectivity

		//create tables
		eqTable = createEquivalenceTable("eq_init"); 
		anonTable = createAnonRecordsTable("an_init");
	}
	
	/**
	 * Overwrites the privacy parameter k
	 * @param newKvalue New value of k
	 */
	public void changeKAnonymityRequirement(int newKvalue) {
		this.conf.k = newKvalue;
		this.suppressionThreshold = newKvalue;
	}

	/**
	 * Create anonRecords table
	 * @param tableName Name of the table
	 * @return A new anonymization records table
	 */
	protected AnonRecordTable createAnonRecordsTable(String tableName) {
		//list qi-attribute indices within the original source
		//no sensitive attributes for Datafly (since this is k-anonymity)
		Integer[] qiAtts = new Integer[conf.qidAtts.length];
		for(int i = 0; i < qiAtts.length; i++) {
			qiAtts[i] = conf.qidAtts[i].index;
		}
		return new AnonRecordTable(qiAtts, new Integer[0], tableName);
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
	 * Insert a tuple to the equivalence table
	 * @param vals All values of the tuple to be inserted (as read from the source, i.e., before any generalization)
	 * @return Equivalence id of the equivalence to which the tuple belongs
	 * @throws Exception
	 */
	protected long insertTupleToEquivalenceTable(String[] vals) throws Exception{
		//check if an equivalence matching suppEq already exists
		Long eid = eqTable.getEID(suppEq); 
		if(eid.compareTo(new Long(-1)) == 0) { //if not, insert new equivalence
			eid = eqTable.insertEquivalence(suppEq);
		}
		return eid;
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
		anonTable.insert(eid, qiVals, new double[0]);
	}

	/**
	 * Anonymizes the input. This function can be reused (with new k maybe), as long as 
	 * eqTable and anonTable objects are fresh and the configuration has 
	 * not changed.
	 * @throws Exception
	 */
	public void anonymize() throws Exception {
		AnonRecordTable readyRecords = createAnonRecordsTable("an_ready");
		EquivalenceTable readyEqs = createEquivalenceTable("eq_ready");
		
		int numUnprocessedEqs = 1; //initially, eqTable contains the suppression equivalence
		while(numUnprocessedEqs > 0) { //while there are more tables to be processed
			String getEID_SQL = "SELECT EID FROM " + eqTable.getName();
			long eid = ((ResultSet) sqlwrapper.executeQuery(getEID_SQL).next()).getLong(1);
			
			//get generalized values for the equivalence
			String[] genVals = eqTable.getGeneralization(eid);
			String genConcat = "[[" + genVals[0] + "]";
			for(int i = 1; i < genVals.length; i++) {
				genConcat += ",[" + genVals[i] + "]";
			}
			System.out.println("Processing EID = " + eid + ", " + genConcat + "]");
			
			//choose partitioning dimension and the split value
			int dim = -1;
			double splitVal = Double.NaN;
			double maxNormalizedWidth = 0;
			Double[] medians = new Double[conf.qidAtts.length];
			for(int i = 0; i < conf.qidAtts.length; i++) {
				medians[i] = getMedian(eid, conf.qidAtts[i].index);
				if(medians[i] != null) {
					double normWidth = getNormalizedWidth(genVals[i], suppEq[i]);
					if(normWidth > maxNormalizedWidth) {
						maxNormalizedWidth = normWidth;
						dim = i;
						splitVal = medians[dim];
					}
				}
			}
			
			//split on the median of dim
			if(dim != -1) { //if there exists allowable cut
				Interval rangeOrig = new Interval(genVals[dim]);
				Interval[] newRanges = rangeOrig.splitInclusive(splitVal);
				
				//create equivalence for LHS
				String[] genValsLHS = genVals.clone();
				genValsLHS[dim] = newRanges[0].toString(); //update the value on dim
				long lhsEID = eqTable.insertEquivalence(genValsLHS); //insert into eqTable (to be processed later)
				updateEIDs(eid, lhsEID, newRanges[0], conf.qidAtts[dim].index); //update EIDs on anonTable
				
				String[] genValsRHS = genVals;
				genValsRHS[dim] = newRanges[1].toString(); //update the value on dim
				long rhsEID = eqTable.insertEquivalence(genValsRHS); //insert into eqTable (to be processed later)
				updateEIDs(eid, rhsEID, newRanges[1], conf.qidAtts[dim].index); //update EIDs on anonTable
				
				//remove existing EID from eqTable
				eqTable.deleteEquivalence(eid);
				
				//update for the while condition 
				numUnprocessedEqs += 1;
				System.out.println("\tInserted " + lhsEID + " (left) and " + rhsEID + " (right)");
			} else { //move this eq to readyEqs
				long newEid = readyEqs.insertEquivalence(genVals);
				readyRecords.cutFrom(anonTable, eid, newEid);
				eqTable.deleteEquivalence(eid);
				
				//update for the while condition
				numUnprocessedEqs --;
				System.out.println("\tRemoving " + eid + " (no allowable cuts)");
			}
		}
		
		if(!readyRecords.checkKAnonymityRequirement(conf.k)) {
			throw new Exception("This table cannot be anonymized at k = " + conf.k);
		}
		
		anonTable.drop();
		eqTable.drop();
		anonTable = readyRecords;
		eqTable = readyEqs;
	}
	
	/**
	 * Calculates the normalized width of a generalized value, based on the suppression value
	 * @param gen String representation of the generalization interval
	 * @param sup String representation of the suppresion interval (i.e., the entire domain)
	 * @return normalized width of the generalization (genWidth / supWidth)
	 * @throws Exception
	 */
	private double getNormalizedWidth(String gen, String sup) throws Exception{
		//calculate the domain width
		Interval supRange = new Interval(sup);
		double supWidth = supRange.high - supRange.low;
		//calculate the generalization width
		Interval genRange = new Interval(gen);
		double genWidth = genRange.high - genRange.low;
		if(genWidth == 0 && genRange.isSingleton()) { //special case: gen = [1]
			genWidth = 1;
		}
		//return the ratio
		return genWidth / supWidth;
	}
	
	/**
	 * Updates the EIDs of equivalences with EID = oldEID to newEID
	 * if the predicate indicated by the interval is satisfied on the qi-attribute
	 * at the specified index
	 * @param oldEID ID of the equivalence being split
	 * @param newEID New ID assigned to those records matching the range
	 * @param range The interval that specifies the new range on newEID for attribute at index
	 * @param index Index of the partitioning dimension
	 */
	private void updateEIDs(long oldEID, long newEID, Interval range, int index) {
		String update_SQL = "UPDATE " + anonTable.getName() 
			+ " SET EID = " + newEID 
			+ " WHERE EID = " + oldEID + " AND " + range.getPredicate("ATT_" + index);
		sqlwrapper.execute(update_SQL);
	}
	
	/**
	 * Computes the median value on attribute indexed att for equivalence with ID eid
	 * @param eid Equivalence ID
	 * @param att An attribute index
	 * @return the median value
	 */
	public Double getMedian(long eid, int att) throws Exception{
		//size of the equivalence
		double totalSize = anonTable.getEquivalenceSize(eid); 
		
		//iterate over all values of the attribute
		String iterVals_SQL = "SELECT ATT_" + att + ", COUNT(*)"
			+ " FROM " + anonTable.getName()
			+ " WHERE EID = " + eid
			+ " GROUP BY ATT_" + att 
			+ " ORDER BY ATT_" + att;
		QueryResult result = sqlwrapper.executeQuery(iterVals_SQL);
		
		int currSize = 0;
		Double median = null;
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			currSize += rs.getInt(2); //increment current size by the count
			
			//found median
			if(currSize >= totalSize / 2) {
				//set the last value as the median
				median = rs.getDouble(1);
				break;
			}
		}
		//check whether this cut is allowable
		if(currSize >= conf.k && (totalSize - currSize) >= conf.k) {
			return median;
		} else {
			return null;
		}
	}
}
