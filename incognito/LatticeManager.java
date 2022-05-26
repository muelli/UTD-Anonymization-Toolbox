package incognito;

import java.util.LinkedList;

import anonymizer.AnonRecordTable;
import anonymizer.EquivalenceTable;

/**
 * Constructed from a super root and the depth of quasi-identifier attributes, 
 * this class manages the generalization lattice entries. 
 */
public class LatticeManager {
	/**	Depths of qi-attributes */
	private int[] dghDepths = null;
	
	/** List of currently unvisited roots */
	private LatticeEntry[] roots = null;
	/** Index of the next root to be visited*/
	private int nextEntryIndex = 0;

	/** Value of the last entry that was returned*/
	private LatticeEntry lastReturned;
	/** List of successfully anonymized lattice entries*/
	private LinkedList<LatticeEntry> successfulEntries;
	
	/**
	 * Class constructor
	 * @param superRoot Lattice entry whose children will be visited
	 * @param dghDepths Maximum depth of domain generalization hierarchies of the qi-attributes
	 */
	public LatticeManager(LatticeEntry superRoot, int[] dghDepths) {
		this.dghDepths = dghDepths;
		
		roots = new LatticeEntry[1];
		roots[0] = superRoot;
		nextEntryIndex = 0;
		
		successfulEntries = new LinkedList<LatticeEntry>();
	}
	
	/**
	 * Checks if there are more lattice entries that need visiting
	 * @return true if there are more, false otherwise
	 */
	public boolean hasNext() {
		//breadth-first retrieval
		if(nextEntryIndex < roots.length) { //there are more entries in reserve, use one of them
			return true;
		} else { //get new entries by populating entries of the next level
			nextEntryIndex = 0;
			//generate new entries
			LinkedList<LatticeEntry> newRoots = new LinkedList<LatticeEntry>();
			for(int i = 0; i < roots.length; i++) { //for every current root
				if(roots[i] == null) { //roots[i] represents a successful anonymization
					continue; //skip its children for that will be successful as well
				}
				LatticeEntry curr = roots[i];
				for(int j = curr.incIndex; j < dghDepths.length; j++) {
					LatticeEntry newConf = new LatticeEntry(curr, j);
					if(newConf.heightAt(j) <= dghDepths[j]) { //check if this att can be further generalized
						newRoots.add(newConf);
					}
				}
			}
			if(newRoots.isEmpty()) { //no higher levels or all successful
				return false; //no more entries to visit
			}
			roots = newRoots.toArray(new LatticeEntry[0]);
			return true;
		}
	}
	
	/**
	 * Getter for successful entries
	 * @return list of lattice entries that map to k-anonymous views
	 */
	public LinkedList<LatticeEntry> getSuccessfulEntries() {
		return successfulEntries;
	}
	
	/**
	 * Get the next entry from the generalization lattice
	 * @return The next unvisited lattice entry
	 */
	public LatticeEntry next() {
		lastReturned = roots[nextEntryIndex]; //set lastReturned
		return lastReturned;
	}
	
	/**
	 * Set the anonymization result for the last lattice entry.
	 * @param successFlag true if the last returned entry satisfied 
	 * the privacy/anonymity requirement, false otherwise
	 */
	public void setResult(boolean successFlag, AnonRecordTable anonTable, EquivalenceTable eqTable) {		
		if(successFlag) { //if successful
			lastReturned.setTables(anonTable, eqTable);
			successfulEntries.add(lastReturned);
			//set this root to null so that its children are not generated.
			/* Based on the apriori rule, all generalizations (children) of a
			 * node that satisfies the privacy definition, satisfy the definition 
			 * as well. Therefore no need to check.*/
			roots[nextEntryIndex] = null; 
		} else {
			System.out.println("Generalization " + lastReturned.toString() + " has failed");
		}
		nextEntryIndex++; //next time, return the sibling.
	}
}
