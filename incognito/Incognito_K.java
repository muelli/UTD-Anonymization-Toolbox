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
 * Implementation of the Incognito algorithm for k-anonymity that appeared
 * in  the following paper:
 * 
 * <pre>
 * &#64;inproceedings{Incognito,
 * author = {LeFevre, Kristen and DeWitt, David J. and Ramakrishnan, Raghu},
 * title = {Incognito: efficient full-domain K-anonymity},
 * booktitle = {SIGMOD '05: Proceedings of the 2005 ACM SIGMOD international conference on Management of data},
 * year = {2005},
 * isbn = {1-59593-060-4},
 * pages = {49--60},
 * location = {Baltimore, Maryland},
 * doi = {http://doi.acm.org/10.1145/1066157.1066164},
 * publisher = {ACM},
 * address = {New York, NY, USA},
 * }
 * </pre>
 * 
 * Our implementation uses the bottom-up precomputation optimization described 
 * in Section 3.3.2 of the paper. At any time during anonymization, only the original table
 * and lattice entries that are k-anonymous are kept on the database. 
 * <p/>
 * After the entire generalization lattice is traversed (in breadth-first order), among all
 * successful anonymization, we choose the one that yields the maximum number of equivalence
 * classes (please see selectAnonymization() for details).
 * <p/>
 * The paper discusses suppression as an option (the idea is the same as Datafly by Sweeney). Our
 * implementation allows suppression through the data member suppressionThreshold. When set to 0,
 * our method will simply disallow suppression.
 */
public class Incognito_K extends Anonymizer {
	/**	Suppression threshold (set to 0 if not needed)*/
	public int suppressionThreshold;
	
	/** Lattice manager that controls how the generalization lattice is traversed */
	private LatticeManager man;
	
	/**
	 * Class constructor
	 * @param conf Configuration instance
	 */
	public Incognito_K(Configuration conf) throws Exception{
		super(conf);
		int[] dghDepths = null;
		
		if(conf.k <= 0) { //validate input k, the privacy parameter
			throw new Exception("Incognito: Parameter k should be set in the configuration file!!");
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
		//no sensitive attributes for Incognito (since this is k-anonymity)
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
	 * Anonymizing the input. This function can be reused (with new k maybe), as long as 
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
	 * k-anonymity with suppression)
	 * @param table Table to be checked for anonymity
	 * @return true if the privacy definition is satisfied
	 * @throws Exception
	 */
	private boolean satisfiesPrivacyDef(AnonRecordTable table) throws Exception{
		return table.checkKAnonymityRequirement(conf.k, suppressionThreshold);
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
		LinkedList<Long> selectionSuppList = null; //suppression list of the selection
		
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
		LinkedList<Long> suppressionList = null;
		while(iter.hasNext()) { //iterate through all successful anonymizations
			try {
				LatticeEntry root = iter.next(); //current root
				System.out.println(root.toString());
				//get the number of equivalences for the root
				String count_SQL = "SELECT COUNT(*) FROM " + root.eqTable.getName();
				QueryResult result = sqlwrapper.executeQuery(count_SQL);
				int currNumEqs = ((ResultSet) result.next()).getInt(1);
				//get the number of equivalences that will be suppressed
				suppressionList = isReadyForSuppression(root.anonTable.getName());
				//update for the net number
				currNumEqs -= suppressionList.size();
				
				//if the number of equivalences for the root is higher, update the choice
				if(currNumEqs > numEquivalences) {
					selection = root;
					numEquivalences = currNumEqs;
					selectionSuppList = suppressionList;
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
			suppressEquivalences(selectionSuppList);
			
			System.out.println("Selection: " + selection.toString());
		}
	}
}
