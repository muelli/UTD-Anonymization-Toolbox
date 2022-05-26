package datafly;

import java.sql.ResultSet;
import java.util.LinkedList;

import sqlwrapper.QueryResult;
import sqlwrapper.SqLiteSQLWrapper;
import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;

/**
 * Implementation of the Datafly algorithm for satisfying k-anonymity described 
 * in the following paper:
 * 
 * <pre>
 * &#64;article{datafly,
 * author = {L. Sweeney and Latanya Sweeney},
 * title = {Achieving K-Anonymity Privacy Protection Using Generalization and Suppression},
 * journal = {International Journal on Uncertainty, Fuzziness and Knowledge-based Systems},
 * year = {2002},
 * volume = {10},
 * pages = {2002}
 * }
 * </pre>
 * 
 * By default, suppression threshold is set to k of k-anonymity.
 * <p/>
 * We also allow a relaxation of full-domain generalization. In this alternative, equivalences
 * that reached a size of k or larger before the entire table is k-anonymous will not be further
 * generalized. Please use the flag fullDomainGeneralization to de/active this version.
 */
public class Datafly extends Anonymizer{
	/**	Flag indicating whether domain generalization will be applied strictly or not. The default
	 * value is TRUE, in which case, an equivalence that reaches size >=k before the entire table
	 * becomes k-anonymous is not generalized anymore. This violates the domain generalization
	 * concept, but certainly not k-anonymity.*/
	public boolean fullDomainGeneralization = true;
	
	/**	Current iteration number for the equivalence table. Starts with 1, used for
	 * keeping track of table names in the database. */
	private int eqTableIndex;
	
	/**	Current iteration number for the anonRecords table. Starts with 1, used for
	 * keeping track of table names in the database. */
	private int anonTableIndex;
	
	/**
	 * Class constructor
	 * @param conf Configuration instance
	 */
	public Datafly(Configuration conf) throws Exception{
		super(conf);
		
		if(conf.k <= 0) { //validate input k, the privacy parameter
			throw new Exception("Datafly: Parameter k should be set in the configuration file!!");
		}
		for(int i = 0; i < conf.qidAtts.length; i++) { //validate VGH as a DGH
			if(!conf.qidAtts[i].isDomainGeneralizationHierarchy()) {
				throw new Exception("Datafly: all leaf nodes should be at the same depth!!");
			}
		}
		suppressionThreshold = conf.k;
		sqlwrapper = SqLiteSQLWrapper.getInstance(); //check DB connectivity
		
		//set initial indices to 1
		eqTableIndex = 1;
		anonTableIndex = 1;
		//create tables
		eqTable = createEquivalenceTable("eq_" + eqTableIndex); 
		anonTable = createAnonRecordsTable("an_" + anonTableIndex);
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
		//list qi-attribute indices within the original source
		//no sensitive attributes for Datafly (since this is k-anonymity)
		Integer[] qiAtts = new Integer[conf.qidAtts.length];
		for(int i = 0; i < qiAtts.length; i++) {
			qiAtts[i] = conf.qidAtts[i].index;
		}
		return new AnonRecordTable(qiAtts, new Integer[0], tableName);
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
		anonTable.insert(eid, qiVals, new double[0]);
	}
	
	/**
	 * Anonymizes the input. This function can be reused (with new k maybe), as long as 
	 * eqTable and anonTable objects are fresh and the configuration has 
	 * not changed.
	 * @throws Exception
	 */
	public void anonymize() throws Exception{
		LinkedList<Long> suppressionList;
		
		String checkSize_SQL = "SELECT COUNT(*) FROM " + anonTable.getName();
		int totalSize = ((ResultSet) sqlwrapper.executeQuery(checkSize_SQL).next()).getInt(1);
		if(totalSize < conf.k) {
			throw new Exception("This input cannot be anonymized at k = " + conf.k);
		}	
		
		while((suppressionList = isReadyForSuppression(anonTable.getName())) == null) {
			//select attribute (the QI-attribute with the largest number
			// of values is to be generalized)
			int[] genDomainCounts = new int[conf.qidAtts.length];
			for(int i = 0; i < genDomainCounts.length; i++) {
				String count_SQL = "SELECT COUNT(*) FROM "
					+ "(SELECT COUNT(*) FROM " + eqTable.getName() + " GROUP BY"
					+ " ATT_" + conf.qidAtts[i].index + ") AS T";
				QueryResult result = sqlwrapper.executeQuery(count_SQL);
				genDomainCounts[i] = ((ResultSet) result.next()).getInt(1);
			}
			//select the generalization attribute based on counts
			int genAttribute = 0;
			int maxSize = genDomainCounts[genAttribute];
			for(int i = 1; i < genDomainCounts.length; i++) {
				if(genDomainCounts[i] > maxSize) { //update genAttribute
					genAttribute = i;
					maxSize = genDomainCounts[i];
				}
			}
			
			//generalize
			System.out.println("Generalizing attribute " + conf.qidAtts[genAttribute].index);
			//create new tables for equivalences and anonRecords
			String newEqvTableName = "eq_" + (++eqTableIndex);
			EquivalenceTable newEqTable = createEquivalenceTable(newEqvTableName);
			String newAnonTableName = "an_" + (++anonTableIndex);
			AnonRecordTable newAnTable = createAnonRecordsTable(newAnonTableName);
			
			String iterateEquivalences = "SELECT EID FROM " + eqTable.getName();
			QueryResult result = sqlwrapper.executeQuery(iterateEquivalences);
			while(result.hasNext()) {
				ResultSet rs = (ResultSet) result.next();
				Long oldEID = rs.getLong(1);
				double count = anonTable.getEquivalenceSize(oldEID);
				
				//build new genVals
				String[] genVals = eqTable.getGeneralization(oldEID);
				if(count < conf.k || fullDomainGeneralization) {
					genVals[genAttribute] = conf.qidAtts[genAttribute].generalize(genVals[genAttribute]);
				}
				
				//set new equivalence ID
				Long newEID = newEqTable.getEID(genVals);
				if(newEID.compareTo(new Long(-1)) == 0) { //this equivalence does not exist yet, insert to get newEID
					newEID = newEqTable.insertEquivalence(genVals); //insert into newEqTable
				}
				newAnTable.copyFrom(anonTable, oldEID, newEID); //copy records from currAnon to newAnon
			}
			//set new tables as current tables
			anonTable.drop();
			eqTable.drop();
			eqTable = newEqTable;
			anonTable = newAnTable;
		}
		//apply suppression
		suppressEquivalences(suppressionList);
	}
}
