package anonymizer;

import incognito.Incognito_K;
import incognito.Incognito_L;
import incognito.Incognito_T;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.ListIterator;

import mondrian.Mondrian;
import sqlwrapper.QueryResult;
import sqlwrapper.SQLWrapper;
import anatomy.Anatomy;
import datafly.Datafly;

/**
 * An abstract class for anonymization methods. Basic functions include 
 * reading data from a source and outputtung the results.
 * <p>
 * Every class that extends Anonymizer should implement methods for (1) creating
 * an equivalence table, (2) creating an anonymized records table, (3) insertion
 * into equivalence table, (3) insertion into anonymized records table and obviously,
 * (5) anonymization.
 */
public abstract class Anonymizer {
	/**	Configuration object */
	protected Configuration conf;
	
	/**	Current working equivalence table */
	protected EquivalenceTable eqTable;
	
	/** Current working anonRecords table*/
	protected AnonRecordTable anonTable;
	
	/** Embedded database connection object */
	protected SQLWrapper sqlwrapper;
	
	/** Suppression threshold (required only if the method allows suppression) */
	protected int suppressionThreshold;
	
	/** Class constructor*/
	public Anonymizer(Configuration conf){
		this.conf = conf;
	}
	
	/** 
	 * Creates the equivalence table
	 * @param tableName Name of the table
	 * @return The table that was just created
	 */
	protected abstract EquivalenceTable createEquivalenceTable(String tableName);
	
	/**
	 * Creates the anonymized records table
	 * @param tableName Name of the table
	 * @return The table that was just created
	 */
	protected abstract AnonRecordTable createAnonRecordsTable(String tableName);
	
	/**
	 * Insert a tuple to the equivalence table
	 * @param vals All values of the tuple to be inserted (as read from the source, i.e., before any generalization)
	 * @return Equivalence id of the equivalence to which the tuple belongs
	 * @throws Exception
	 */
	protected abstract long insertTupleToEquivalenceTable(String[] vals) throws Exception;
	
	/** Insert a tuple to the anonRecords table
	 * @param vals All values of the tuple to be inserted (as read from the source, i.e., before any generalization)
	 * @param eid Equivalence id of the equivalence to which the tuple belongs
	 * @throws Exception
	 */
	protected abstract void insertTupleToAnonTable(String[] vals, long eid) throws Exception;
	
	/**
	 * Read input data
	 */
	public void readData() throws Exception{
		String filename = conf.inputFilename;
		FileReader fr = new FileReader(filename);
		BufferedReader input = new BufferedReader(fr);
		
		int count = 0;
		String line;
		while( (line = input.readLine()) != null && line.length() > 0) {
			if(line.contains("?")) { //omit all lines with missing values
				continue;
			}
			count++;
			String[] vals = line.split(conf.separator);
			for(int i = 0; i < vals.length; i++) {
				vals[i] = vals[i].trim();
			}
			//get Equivalence index
			Long eid = insertTupleToEquivalenceTable(vals);
			//insert into AnonRecords
			insertTupleToAnonTable(vals, eid);
		}
		
		input.close();
	}
	
	/**
	 * Anonymizes the input
	 */
	public abstract void anonymize() throws Exception;
	
	/**
	 * Checks if the generalized data is ready for suppression
	 * @param anonRecordTable Name of the current anonymization record table 
	 * @return Empty list if already k-anonymous, List of EIDs of 
	 * equivalences that should be suppressed if ready for suppression,
	 * null otherwise. 
	 */
	protected LinkedList<Long> isReadyForSuppression(String anonRecordTable) throws Exception{
		//for all equivalences with less than k generalized tuples,
		// sumEquivalenceSizes stores their total size
		int sumEquivalenceSizes = 0;
		LinkedList<Long> equivalencesToBeSuppressed = new LinkedList<Long>();
		String select_SQL = "SELECT EID, COUNT(*) FROM " + anonRecordTable 
			+ " GROUP BY EID ORDER BY COUNT(*) ASC";
		QueryResult result = sqlwrapper.executeQuery(select_SQL);
		while(result.hasNext()) {
			ResultSet rs = (ResultSet) result.next();
			Integer currCount = rs.getInt(2);
			if(currCount < conf.k) {
				//add EID to the list
				equivalencesToBeSuppressed.add(rs.getLong(1));
				sumEquivalenceSizes += currCount;
				if(suppressionThreshold > 0 
						&& sumEquivalenceSizes > suppressionThreshold) {
					return null; //too many records for suppression, not ready yet
				}
			} else { //currCount >= conf.k
				return equivalencesToBeSuppressed; //this is only to save time
				//any tuple with currCount >= conf.k cannot be suppressed
			}
		}
		return equivalencesToBeSuppressed;
	}
	
	/**
	 * Suppress the equivalences with provided IDs.
	 * @param suppressionList IDs of equivalences to be suppressed
	 */
	protected void suppressEquivalences(LinkedList<Long> suppressionList) {
		//generate the suppression equivalence
		String[] genVals = new String[conf.qidAtts.length];
		for(int i = 0; i < genVals.length; i++) {
			genVals[i] = conf.qidAtts[i].getSup();
		}
		
		//suppress according to suppression list
		if(suppressionList != null) {
			Long suppEID = new Long(-1);
			if(!suppressionList.isEmpty()) { //add the new equivalence to the table
				try {
					suppEID = eqTable.insertEquivalence(genVals);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			while(!suppressionList.isEmpty()) { //delete suppressed eq.s and move anonRecords 
					//to the suppression equivalence
				Long eid = suppressionList.removeFirst();
				eqTable.deleteEquivalence(eid);
				anonTable.moveRecords(eid, suppEID);
				System.out.println("Suppressing equivalence " + eid);
			}
		}
	}
	
	/**
	 * Output the results and close the connection to the database
	 */
	public void outputResults() throws Exception {
		if(conf.outputFormat == Configuration.OUTPUT_FORMAT_GENVALS) {
			outputResults_GenVals();
		} else if(conf.outputFormat == Configuration.OUTPUT_FORMAT_GENVALSDIST) {
			outputResults_GenValsDist();
		} else {
			outputResults_Anatomy();
		}
		anonTable.drop();
		eqTable.drop();
		sqlwrapper.flush();
	}	
	
	/**
	 * Output the results and close the connection to the database
	 */
	public void outputResults_GenVals() throws Exception{
		//carriage return for the system
		String newline = System.getProperty("line.separator");
		//open output file
		FileWriter fw = new FileWriter(conf.outputFilename);
		BufferedWriter output = new BufferedWriter(fw);
		//open input file
		FileReader fr = new FileReader(conf.inputFilename);
		BufferedReader input = new BufferedReader(fr);
		
		long rid = 1;
		String inputline;
		//need to synchronize the input and output records, will iterate through the input
		while( (inputline = input.readLine()) != null && inputline.length() > 0) {
			if(inputline.contains("?")) { //omit all lines with missing values
				continue;
			}
			String[] vals = inputline.split(conf.separator);
			
			//get the eid for the current rid
			String getEIDforRID  ="SELECT EID FROM " + anonTable.getName() + " WHERE RID = " + rid;
			QueryResult result = sqlwrapper.executeQuery(getEIDforRID);
			Long eid = ((ResultSet) result.next()).getLong(1);
			rid++; //increment rid for the next tuple to be retrieved
			
			//from the equivalence ID, get the generalized values
			String[] genVals = eqTable.getGeneralization(eid);			
			//overwrite vals with values from genVals
			for(int i = 0; i < conf.qidAtts.length; i++) {
				vals[conf.qidAtts[i].index] = genVals[i];
			}
			//overwrite id values
			if(conf.idAttributeIndices != null) {
				ListIterator<Integer> iter = conf.idAttributeIndices.listIterator();
				while(iter.hasNext()) {
					vals[iter.next()] = null;
				}
			}
			
			//build output string
			String line = "";
			for(int i = 0; i < vals.length; i++) {
				if(vals[i] != null) {
					line += conf.separator + vals[i];
				}
			}
			line = line.substring(conf.separator.length()); //remove the extra separator in the beginning
			line += newline;
			output.write(line);
		}
		//close the output file
		output.close();
	}
	
	/**
	 * Output the results and close the connection to the database
	 */
	public void outputResults_GenValsDist() throws Exception{
		//first generate distribution - this call updates genVals of each equivalence in eqTable
		generateDistributions();
		//then output genVals as usual
		outputResults_GenVals();
	}
	
	/**
	 * Generates quasi-identifier distributions for each quasi-identifier
	 * @throws Exception
	 */
	private void generateDistributions() throws Exception{
		//iterate over all equivalences
		String iterateEqs = "SELECT EID FROM " + eqTable.getName();
		QueryResult result = sqlwrapper.executeQuery(iterateEqs);
		while(result.hasNext()) {
			//get the eid and the current generalized values
			long eid = ((ResultSet) result.next()).getLong(1);
			String[] genVals = eqTable.getGeneralization(eid);
			
			//array for statistical information on qid attributes
			boolean existsNumerical = false;
			double[][] statistics = new double[conf.qidAtts.length][];
			for(int i = 0; i < conf.qidAtts.length; i++) {
				if(conf.qidAtts[i].catDomMapping != null) { //categorical, get pmf
					statistics[i] = new double[conf.qidAtts[i].catDomMapping.size()];
				} else { //numerical, get pdf
					statistics[i] = new double[2];
					existsNumerical = true;
				}
			}
			//iterate over all tuples
			int numTuples = 0;
			String iterateTuples = "SELECT * FROM " + anonTable.getName() + " WHERE EID = " + eid;
			QueryResult subresult = sqlwrapper.executeQuery(iterateTuples);
			while(subresult.hasNext()) {
				numTuples++;
				ResultSet rs = (ResultSet) subresult.next();
				//update statistics
				for(int i = 0; i < conf.qidAtts.length; i++) {
					if(conf.qidAtts[i].catDomMapping != null) { //categorical
						//increment the count for the value
						statistics[i][(int)rs.getDouble("ATT_" + conf.qidAtts[i].index)]++;
					} else { //numerical
						//increment mean by value
						statistics[i][0] += rs.getDouble("ATT_" + conf.qidAtts[i].index);
					}
				}
			}
			//convert counts to probs for categorical atts, sums to means for numerical atts
			for(int i = 0; i < conf.qidAtts.length; i++) {
				if(conf.qidAtts[i].catDomMapping != null) { //categorical
					for(int j = 0; j < statistics[i].length; j++) {
						statistics[i][j] /= numTuples;
					}
				} else { //numerical
					statistics[i][0] /= numTuples;
				}
			}
			//if there are numerical attributes, re-iterate to calculate the std
			if(existsNumerical) {
				subresult = sqlwrapper.executeQuery(iterateTuples);
				while(subresult.hasNext()) {
					ResultSet rs = (ResultSet) subresult.next();
					//update statistics
					for(int i = 0; i < conf.qidAtts.length; i++) {
						if(conf.qidAtts[i].catDomMapping == null) { //numerical
							double diff = rs.getDouble("ATT_" + conf.qidAtts[i].index) - statistics[i][0]; 
							statistics[i][1] += diff * diff;
						}
					}
				}
				//compute stdev
				for(int i = 0; i < conf.qidAtts.length; i++) {
					if(conf.qidAtts[i].catDomMapping == null) { //numerical
						statistics[i][1] /= numTuples;
					}
				}
			}
			//now that we have all statistical information regarding an equivalence class,
			// we can overwrite generalized values to contain as such
			for(int i = 0; i < conf.qidAtts.length; i++) {
				String dist = "";
				if(conf.qidAtts[i].catDomMapping != null) { //categorical
					dist = Double.toString(statistics[i][0]);
					for(int j = 1; j < statistics[i].length; j++) {
						dist += ":" + statistics[i][j];
					}
				} else { //numerical
					dist = statistics[i][0] + ":" + statistics[i][1];
				}
				//genVals[i] = genVals[i] + conf.separator + dist;
				genVals[i] = dist;
			}
			//overwrite generalized values in eqTable
			eqTable.setGeneralization(eid, genVals);
		}
	}
	
	/**
	 * Output the results and close the connection to the database
	 */
	public void outputResults_Anatomy() throws Exception{
		//carriage return for the system
		String newline = System.getProperty("line.separator");
		//open output files
		int extensionIndex = conf.outputFilename.lastIndexOf(".");
		String f1, f2;
		if(extensionIndex < conf.outputFilename.length()) { //no dots in the filename
			String prefix = conf.outputFilename.substring(0,extensionIndex);
			String suffix = conf.outputFilename.substring(extensionIndex, conf.outputFilename.length());
			f1 = prefix + "_QIT" + suffix;
			f2 = prefix + "_ST" + suffix;
		} else {
			f1 = conf.outputFilename + "_QIT";
			f2 = conf.outputFilename + "_ST";
		}
		FileWriter fw1 = new FileWriter(f1);
		BufferedWriter outputQIT = new BufferedWriter(fw1);
		FileWriter fw2 = new FileWriter(f2);
		BufferedWriter outputST = new BufferedWriter(fw2);		
		//open input file
		FileReader fr = new FileReader(conf.inputFilename);
		BufferedReader input = new BufferedReader(fr);
		
		long rid = 1;
		String inputline;
		//need to synchronize the input and output records, will iterate through the input
		while( (inputline = input.readLine()) != null && inputline.length() > 0) {
			if(inputline.contains("?")) { //omit all lines with missing values
				continue;
			}
			String[] vals = inputline.split(conf.separator);
			
			//get the eid for the current rid
			String getEIDforRID  ="SELECT * FROM " + anonTable.getName() + " WHERE RID = " + rid;
			QueryResult result = sqlwrapper.executeQuery(getEIDforRID);
			ResultSet rs = (ResultSet) result.next();
			Long eid = rs.getLong("EID");
			rid++; //increment rid for the next tuple to be retrieved
			
			//get qi-values of the tuple
			String[] qiVals = new String[conf.qidAtts.length];
			for(int i = 0; i < conf.qidAtts.length; i++) {
				vals[conf.qidAtts[i].index] = null;
				qiVals[i] = Double.toString(rs.getDouble("ATT_" + conf.qidAtts[i].index));
			}
			
			//to QIT, output EID and qiVals
			String line = qiVals[0];
			for(int i = 1; i < qiVals.length; i++) {
				line += conf.separator + qiVals[i];
			}
			line += conf.separator + Long.toString(eid) + newline;
			outputQIT.write(line);
			
			//to SIT, output the rest
			if(conf.idAttributeIndices != null) { 
				//overwrite id values
				ListIterator<Integer> iter = conf.idAttributeIndices.listIterator();
				while(iter.hasNext()) {
					vals[iter.next()] = null;
				}
			}
			line = Long.toString(eid); //build output string
			for(int i = 0; i < vals.length; i++) {
				if(vals[i] != null) {
					line += conf.separator + vals[i];
				}
			}
			line += newline;
			outputST.write(line);
		}
		//close the output files
		outputQIT.close();
		outputST.close();
	}
	
//	/**
//	 * Display useage information
//	 */
//	private static void showUsage() {
//		String newline = System.getProperty("line.separator");
//		//String usage = "USAGE: -method {datafly, incognito_k, incognito_l,"
//		//		+ " incognito_t, mondrian, tds, anatomy}"
//	    String usage = "USAGE: -method {datafly, incognito_k, incognito_l,"
//		+ " incognito_t, mondrian, anatomy}"
//			+ newline + "\t|  -config STRING"
//			+ newline + "\t|  -k INT"
//			+ newline + "\t|  -l DOUBLE"
//			+ newline + "\t|  -t DOUBLE"
//			+ newline + "\t|  -c DOUBLE"
//			+ newline + "\t|  -suppthreshold INT"
//			+ newline + "\t|  -input STRING"
//			+ newline + "\t|  -separator STRING"
//			+ newline + "\t|  -output STRING"
//			+ newline + "\t|  -outputformat {genVals, genValsDist, anatomy}";
//		System.out.println(usage);
//	}
	
	/**
	 * Anonymize an input based on the specified argument list.
	 * @param args list of program arguments.
	 */
	public static void anonymizeDataset(String[] args) throws Exception {
		Configuration conf = new Configuration(args);
		anonymizeDataset(conf);
	}
	
	public static void anonymizeDataset(Configuration conf) throws Exception {
		Anonymizer anon = null;
		//initialize anonymizer
		switch(conf.anonMethod) {
		case Configuration.METHOD_DATAFLY: 
			anon = new Datafly(conf);
			break;
		case Configuration.METHOD_MONDRIAN: 
			anon = new Mondrian(conf);
			break;
		case Configuration.METHOD_INCOGNITO_K:
			anon = new Incognito_K(conf);
			break;
		case Configuration.METHOD_INCOGNITO_L:
			anon = new Incognito_L(conf);
			break;
		case Configuration.METHOD_INCOGNITO_T:
			anon = new Incognito_T(conf);
			break;
		case Configuration.METHOD_ANATOMY:
			anon = new Anatomy(conf);
			break;				
		}
		
		//read data
		long start = System.currentTimeMillis();
		anon.readData();
		long stop = System.currentTimeMillis();
		System.out.println("Reading data takes " + Long.toString((stop - start)/1000) + "sec.s");
		//anonymize
		start = System.currentTimeMillis();
		anon.anonymize();
		stop = System.currentTimeMillis();
		System.out.println("Anonymization takes " + Long.toString((stop - start)/1000) + "sec.s");
		//output results
		start = System.currentTimeMillis();
		anon.outputResults();
		stop = System.currentTimeMillis();
		System.out.println("Writing data takes " + Long.toString((stop - start)/1000) + "sec.s");
	}
	
	public static void main(String[] args) {
		try {
			Anonymizer.anonymizeDataset(args);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
