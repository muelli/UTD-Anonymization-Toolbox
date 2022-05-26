package incognito;

import anonymizer.AnonRecordTable;
import anonymizer.EquivalenceTable;

/**
 * This class represent a node of the generalization lattice. These nodes
 * are represented by the amount of generalization from the original table.
 * Original table corresponds to the root entry that has no parent, and has
 * the root value of all 0s. All other entries will be assigned a parent, 
 * from which, based on the increment index (index of the quasi-identifier
 * that is being generalized), the node will derive the number of generalization
 * for each attribute. 
 * For example, the entry built by the parent 0_1_0, incremented on index 1 is
 * 0_2_0. Similarly, 0_2_0 incremented on 2 returns the entry 0_2_1.
 */
public class LatticeEntry {
	/**	Index of the qi-attribute that is to be generalized */
	public int incIndex;
	
	/**	Number of generalizations for each qi-attribute (size = number of qi-attributes */
	private int[] root;
	
	/** Anonoymized records table that corresponds to this entry (filled in if successful)*/
	public AnonRecordTable anonTable;
	
	/** Equivalence table that corresponds to this entry (filled in if successful) */
	public EquivalenceTable eqTable;
	
	/**
	 * Class constructor for the superroot (i.e., entry of the original table). 
	 * @param root Integer array of all 0s (size = number of qi-attributes)
	 */
	public LatticeEntry(int[] root) {
		this.root = root;
		this.incIndex = 0;
	}
	
	/**
	 * Class constructor for intermediate nodes of the generalization lattice
	 * @param parent The entry from which this entry is to be derived
	 * @param incIndex Index of the qi-attribute that is to be generalized
	 */
	public LatticeEntry(LatticeEntry parent, int incIndex) {
		this.incIndex = incIndex;
		
		this.root = parent.root.clone(); //create a fresh copy
		this.root[this.incIndex]++;
	}
	
	/**
	 * Checks if this lattice entry generalizes to that lattice entry.
	 * The basic requirement is that that entry should be a parent of this entry (i.e., 
	 * on any qi-attribute, the number of generalizations of this should be LEQ of that)
	 * @param that Another lattice entry
	 * @return true if this generalizes to that, false otherwise
	 */
	public boolean generalizesTo(LatticeEntry that) {
		//check compatibility
		if(this.root.length != that.root.length) {
			try {
				throw new Exception("Configurations are incompatible.");
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		//go over all root values
		for(int i = 0; i < this.root.length; i++) {
			if(this.root[i] > that.root[i]) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Get the number of generalizations on the quasi-identifier at index
	 * @param index The index of a quasi-identifier (larger than 0, 
	 * less than the number of quasi-identifiers)
	 * @return Number of generalization on the attribute
	 */
	public int heightAt(int index) {
		return root[index];
	}
	
	/**
	 * Get the string representation of this entry's parent
	 * @return Parent's name
	 */
	public String parentsName() {
		//create the roots for parent
		int[] parentRoot = root.clone();
		parentRoot[incIndex]--;
		
		//convert to String
		String retVal = Integer.toString(parentRoot[0]);
		for(int i = 1; i < parentRoot.length; i++) {
			retVal += "_" + parentRoot[i];
		}
		return retVal;
	}
	
	public void setTables(AnonRecordTable anonTable, EquivalenceTable eqTable) {
		this.anonTable = anonTable;
		this.eqTable = eqTable;
	}
	
	public String toString() {
		String retVal = Integer.toString(root[0]);
		for(int i = 1; i < root.length; i++) {
			retVal += "_" + Integer.toString(root[i]);
		}
		return retVal;
	}
}
