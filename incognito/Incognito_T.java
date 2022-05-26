package incognito;

import java.sql.ResultSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.ListIterator;

import sqlwrapper.QueryResult;
import sqlwrapper.SqLiteSQLWrapper;
import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;

/**
 * Implementation of the t-closeness privacy principle based on 
 * the Incognito anonymization algorithm. For details on t-closeness, please refer to 
 * the following paper:
 * 
 * <pre>
 * &#64;inproceedings{tcloseness,
 * author    = {Ninghui Li and Tiancheng Li and Suresh Venkatasubramanian},
 * title     = {t-Closeness: Privacy Beyond k-Anonymity and l-Diversity},
 * booktitle = {ICDE},
 * year      = {2007},
 * pages     = {106-115}
 * }
 * </pre>
 * 
 * Our implementation handles only one sensitive attribute, which could be
 * numerical or categorical, but not hierarchical. Data type of the sensitive
 * attribute is inferred from the configuration file (i.e., if a categorical to numeric
 * domain mapping has been specified, then the sensitive attribute is considered
 * categorical).
 * <p/>
 * If needed, hiearchical attributes can be handled as follows: specify a generalization
 * hierarchy for the sensitive attribute (similar to quasi-identifier attributes) and then
 * update the t-closeness check in AnonRecordTable to handle hierarchical attributes.
 * <p/>
 * For numerical attribute, our implementation is quite straightforward. We first query
 * all possible values in the domain together with corresponding counts. Then, for each 
 * equivalence in the table, we check for t-closeness by comparing the two distributions.
 * Specifically, this is achieved by incrementally traversing two linked lists of (value,count)
 * pairs. We assume ordered distance among two successive values in the domain.
 */
public class Incognito_T extends Anonymizer {	
	/** Lattice manager that controls how the generalization lattice is traversed */
	private LatticeManager man;
	
	/** Number of values in the domain of the sensitive attribute */
	private int sensDomainSize;
	/**
	 * Class constructor
	 * @param conf Configuration instance
	 */
	public Incognito_T(Configuration conf) throws Exception{
		super(conf);
		int[] dghDepths = null;
		
		if(conf.t <= 0) { //validate input t, the privacy parameter
			throw new Exception("Incognito: Parameter t should be set in the configuration file!!");
		} else if(conf.t < 0 || conf.t > 1) {
			throw new Exception("Incognito: Setting 0 < t or t > 1 does not make sense!!");
		}
		//check the number of sensitive attributes
		if(conf.sensitiveAtts.length != 1) {
			throw new Exception("Incognito: Set 1 (and only 1) sensitive attribute!!");
		}
		//make sure that the class attribute has a mapping 
		// this mapping will be used as an index when counting class values
		if(conf.sensitiveAtts[0].catDomMapping != null) { //reset the mapping to start from 0 and increment by 1
			System.out.println("Resetting the mapping for attribute " 
					+ conf.sensitiveAtts[0].index + " so that the mapped values "
					+ "start with 0 and increment by 1");
			
			Hashtable<String, Integer> map = new Hashtable<String, Integer>(); //my new map
			//iterate over existing elements, changing the integer values
			int index = 0;
			Enumeration<String> enu = conf.sensitiveAtts[0].catDomMapping.keys();
			while(enu.hasMoreElements()) {
				String sVal = enu.nextElement();
				map.put(sVal, index);
				index++;
			}
			//overwrite existing mapping
			conf.sensitiveAtts[0].catDomMapping = map;
			sensDomainSize = conf.sensitiveAtts[0].catDomMapping.size();
		}
		//get DGH depths, perform VGH validation
		dghDepths = new int[conf.qidAtts.length];
		for(int i = 0; i < dghDepths.length; i++) {
			dghDepths[i] = conf.qidAtts[i].getVGHDepth(true);
		}
		sqlwrapper = SqLiteSQLWrapper.getInstance(); //check DB connectivity
		
		//create a latticeManager
		int[] root = new int[conf.qidAtts.length];
		for(int i = 0; i < root.length; i++) {
			root[i] = 0;
		}
		LatticeEntry superRoot = new LatticeEntry(root);
		man = new LatticeManager(superRoot, dghDepths);
		
		//create tables
		eqTable = createEquivalenceTable("eq_" + superRoot.toString()); 
		anonTable = createAnonRecordsTable("an_" + superRoot.toString());
	}
	
	/**
	 * Overwrites the privacy parameter t
	 * @param newTvalue New value of t
	 */
	public void changeTClosenessRequirement(double newTvalue) {
		conf.t = newTvalue;
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
	 * Anonymizing the input. This function can be reused (with new t maybe), as long as 
	 * eqTable and anonTable objects are fresh and the configuration has 
	 * not changed.
	 * @throws Exception
	 */
	public void anonymize() throws Exception{
		/* Assuming that input data is already read, anonTable and eqTable 
		 * contains the initial, ungeneralized tuple values. Therefore, for 
		 * the first round, there is no need to build new tables. */
		man.next();
		if(satisfiesPrivacyDef(anonTable)) {
			man.setResult(true, anonTable, eqTable);
		} else {
			man.setResult(false, null, null);
		}
		
		//now continue with the children of the superRoot, if any
		while(man.hasNext()) {
			LatticeEntry currRoot = man.next();
			generalize(currRoot);
		}
		
		//select among successful generalizations
		selectAnonymization(man.getSuccessfulEntries());
	}
	
	/**
	 * Checks whether the parameter table satisfies the privacy definition (in this case, 
	 * t-closeness)
	 * @param table Table to be checked for anonymity
	 * @return true if the privacy definition is satisfied
	 * @throws Exception
	 */
	public boolean satisfiesPrivacyDef(AnonRecordTable table) throws Exception{
		if(conf.sensitiveAtts[0].catDomMapping != null) {
			return table.checkTClosenessRequirement_Cat(conf.t, conf.sensitiveAtts[0].index, sensDomainSize);	
		} else {
			return table.checkTClosenessRequirement_Num(conf.t, conf.sensitiveAtts[0].index);
		}
	}
	
	/**
	 * Generalizes the original table according to a lattice entry and 
	 * sets the result for the generated table as anonymous or not. If not, 
	 * the corresponding table will be dropped from the database. If too,
	 * this root will be added to the set of successful anonymizations.
	 * @param root An entry of the generalization lattice that specifies how many 
	 * times each qi-attribute will be generalized
	 * @throws Exception
	 */
	private void generalize(LatticeEntry root) throws Exception {
		//collect root info, create anonTable and equivalenceTable objects
		EquivalenceTable currET = createEquivalenceTable("eq_" + root.toString());
		AnonRecordTable currAT = createAnonRecordsTable("an_" + root.toString());
		
		String iterateEquivalences = "SELECT EID FROM " + eqTable.getName();
		QueryResult result = sqlwrapper.executeQuery(iterateEquivalences);
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			Long oldEID = rs.getLong(1);
			
			//build new genVals - generalize each attribute root.heightAt(j) times
			String[] genVals = eqTable.getGeneralization(oldEID);
			for(int i = 0; i < genVals.length; i++) {
				for(int j = 0; j < root.heightAt(i); j++) {
					genVals[i] = conf.qidAtts[i].generalize(genVals[i]);
				}
			}
			
			//set new equivalence ID
			Long newEID = currET.getEID(genVals);
			if(newEID.compareTo(new Long(-1)) == 0) { //this equivalence does not exist yet, insert to get newEID
				newEID = currET.insertEquivalence(genVals); //insert into newEqTable
			}
			currAT.copyFrom(anonTable, oldEID, newEID); //copy records from currAnon to newAnon
		}
		
		//check if current generalization is k-anonymous
		if(satisfiesPrivacyDef(currAT)) {
			man.setResult(true, currAT, currET);
		} else {
			currAT.drop();
			currET.drop();
			man.setResult(false, null, null);
		}
	}
	
	/**
	 * Simply select the anonymization that yields the maximum number of distinct equivalences
	 * @param anons List of lattice entries that correspond to anonymized tables
	 */
	private void selectAnonymization(LinkedList<LatticeEntry> anons) throws Exception {
		int numEquivalences = 0; //maximum number of equivalences
		LatticeEntry selection = null; //lattice entry of choice
		
		//prepare the output message
		String title = Integer.toString(conf.qidAtts[0].index);
		for(int i = 1; i < conf.qidAtts.length; i++) {
			title += "_" + conf.qidAtts[i].index;
		}
		System.out.println();
		System.out.println("QI-Atts: " + title);
		System.out.println("------------");
		System.out.println("Anonymous generalizations (" + anons.size() + ") :");
		
		ListIterator<LatticeEntry> iter = anons.listIterator();
		while(iter.hasNext()) { //iterate through all successful anonymizations
			try {
				LatticeEntry root = iter.next(); //current root
				System.out.println(root.toString());
				//get the number of equivalences for the root
				String count_SQL = "SELECT COUNT(*) FROM " + root.eqTable.getName();
				QueryResult result = sqlwrapper.executeQuery(count_SQL);
				int currNumEqs = ((ResultSet) result.next()).getInt(1);
				
				//if the number of equivalences for the root is higher, update the choice
				if(currNumEqs > numEquivalences) {
					selection = root;
					numEquivalences = currNumEqs;
				} else {
					root.anonTable.drop();
					root.eqTable.drop();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		if(selection == null) {
			throw new Exception("No anonymous generalizations!!!");
		} else {
			//if necessary, drop the original tables
			for(int i = 0; i < conf.qidAtts.length; i++) {
				if(selection.heightAt(i) != 0) {
					eqTable.drop();
					anonTable.drop();
					break;
				}
			}
			//set the choice
			eqTable = selection.eqTable;
			anonTable = selection.anonTable;
			
			System.out.println("Selection: " + selection.toString());
		}
	}
}
