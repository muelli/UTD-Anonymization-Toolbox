package incognito;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.ListIterator;

import sqlwrapper.QueryResult;
import sqlwrapper.SqLiteSQLWrapper;
import anonymizer.AnonRecordTable;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import anonymizer.EquivalenceTable;

/**
 * Implementation of the l-diversity privacy principle based on 
 * the Incognito anonymization algorithm. For details on l-diversity, please refer to 
 * the following paper:
 * 
 * <pre>
 * &#64;inproceedings{lDiversity,
 * author = {Daniel Kifer and Johannes Gehrke},
 * title = {l-Diversity: Privacy Beyond k-Anonymity},
 * booktitle = {In ICDE},
 * year = {2006},
 * pages = {24}
 * }
 * </pre>
 * 
 * The paper discusses anonymization through generalization (as opposed to generalization
 * with suppression). That's why, unlike our Incognito implementation which allows suppression,
 * this implementation of Incognito disallows suppression.
 * <p/>
 * Apart from this minor change, the only difference with the original Incognito algorithm
 * is the different privacy definition (i.e., various l-diversity instantiations). 
 */
public class Incognito_L extends Anonymizer {	
	/** Lattice manager that controls how the generalization lattice is traversed */
	private LatticeManager man;
	
	/**
	 * Class constructor
	 * @param conf Configuration instance
	 */
	public Incognito_L(Configuration conf) throws Exception{
		super(conf);
		int[] dghDepths = null;
		
		if(conf.l <= 0) { //validate input l, the privacy parameter
			throw new Exception("Incognito: Parameter l should be set in the configuration file!!");
		} else if(conf.l <= 1) {
			throw new Exception("Incognito: Setting 0 < l <= 1 does not make sense!!");
		}
		if(conf.c <= 0) {
			System.out.println("Parameter c was not set, therefore using Entropy l-diversity" +
					" as the privacy definition.");
		}
		//check the number of sensitive attributes
		if(conf.sensitiveAtts.length != 1) {
			throw new Exception("Incognito: Set 1 (and only 1) sensitive attribute!!");
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
	 * Overwrites the privacy parameter l
	 * @param newLvalue New value of l
	 */
	public void changeLDiversityRequirement(double newLvalue) {
		this.conf.l = newLvalue;
	}
	
	/**
	 * Overwrites the privacy parameter l
	 * @param newLvalue New value of l
	 * @param newCvalue New value of c
	 */
	public void changeLDiversityRequirement(double newLvalue, double newCvalue) {
		this.conf.l = newLvalue;
		this.conf.c = newCvalue;
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
	 * some l-diversity instantiation)
	 * @param table Table to be checked for anonymity
	 * @return true if the privacy definition is satisfied
	 * @throws Exception
	 */
	public boolean satisfiesPrivacyDef(AnonRecordTable table) throws Exception{
		if(conf.c <= 0) {
			return table.checkLDiversityRequirement(conf.l, conf.sensitiveAtts[0].index);
		} else {
			return table.checkLDiversityRequirement(conf.l, conf.c, conf.sensitiveAtts[0].index);
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
	private void selectAnonymization(LinkedList<LatticeEntry> anons) throws Exception{
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
