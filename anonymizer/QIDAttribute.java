package anonymizer;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.ListIterator;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Data structure that holds all relevant information related to a quasi-identifier attribute.
 * Namely, attribute index in the input, the value generalization hierarchy (read from the 
 * configuration file), string->integer domain mapping for categorical attributes.
 */
public class QIDAttribute {
	/**	Mapping from the categorical domain to discrete-valued numerical (integer)
	 * domain for categorical attributes*/
	public Hashtable<String, Integer> catDomMapping = null;
	
	/** Index of the attribute */
	public int index = -1;
	
	/** Suppression value for the VGH */
	private String suppValue = null;
	
	/** Lookup to find the generalization of an interval*/
	private Hashtable<String, String> parentLookup = null;
	
	/** Lookup to find children (specializations) of an interval*/
	private Hashtable<String, String[]> childLookup = null;
	
	/** List of leaf intervals in the VGH*/
	private LinkedList<Interval> leafIntervals = null;
	
	/**
	 * Class constructor
	 * @param att The node that describes a QID attribute in the configuration file
	 * @param index Index of the attribute
	 */
	public QIDAttribute(int index, Node att) throws Exception {
		this.index = index;
		parentLookup = new Hashtable<String, String>();
		childLookup = new Hashtable<String, String[]>();
		leafIntervals = new LinkedList<Interval>();
		
		//iterate through the children to get the mapping (if exists) and the VGH
		NodeList childList = att.getChildNodes(); 
		for(int i = 0; i < childList.getLength(); i++) {
			Node child = childList.item(i);
			if(child.getNodeType() != Node.ELEMENT_NODE) { //omit any non-element nodes (e.g., comments)
				continue;
			}
			else if(child.getNodeName().compareTo("map") == 0) {
				parseMapping(child);
			} else if(child.getNodeName().compareTo("vgh") == 0) {
				parseVGH(child);
			}
		}
		
		if(suppValue == null) {
			throw new Exception("Cannot parse the VGH of qid-attribute at index " + index);
		}
	}
	
	/**
	 * Assign the mapping from the categorical domain to discrete valued numerical domain
	 * @param map Root node that contains (category, integer-value) pairs
	 */
	private void parseMapping(Node map) throws Exception{
		//initialize the mapping
		catDomMapping = new Hashtable<String, Integer>();
		
		NodeList childList = map.getChildNodes();
		for(int i = 0; i < childList.getLength(); i++) {
			Node child = childList.item(i);
			if(child.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			//get the values for the attributes named "cat" and "int" 
			NamedNodeMap nodeAtts = child.getAttributes();
			String key = null;
			int value = -1;
			Node n = nodeAtts.getNamedItem("cat");
			if(n == null) {
				throw new Exception("Cannot parse the mapping of the qid-attribute at index" + index);
			} else {
				key = n.getNodeValue();
			}
			n = nodeAtts.getNamedItem("int");
			if(n == null) {
				throw new Exception("Cannot parse the mapping of the qid-attribute at index" + index);
			} else {
				value = Integer.parseInt(n.getNodeValue());
			}
			//add the pair to the mapping
			catDomMapping.put(key, value);
		}
	}
	
	/**
	 * Obtain the value-generalization hierarchy for the attribute
	 * @param vgh Root node that contains the suppression value and all its chilren (specializations) 
	 */
	private void parseVGH(Node vgh) throws Exception{
		Node temp = vgh.getAttributes().getNamedItem("value");
		if(temp == null) {
			throw new Exception("Error in VGH structure, attribute " + index + "!!!");
		}
		suppValue = temp.getNodeValue();
		
		//breadth-first traversel of the VGH
		LinkedList<Node> unprocessedNodes = new LinkedList<Node>();
		unprocessedNodes.add(vgh);
		while(!unprocessedNodes.isEmpty()) {
			Node n = unprocessedNodes.removeFirst();
			temp = n.getAttributes().getNamedItem("value");
			if(temp == null) {
				throw new Exception("Error in VGH structure, attribute " + index + "!!!");
			}
			String parent = temp.getNodeValue();
			Interval parInt = new Interval(parent); //just to validate the user input
			
			//iterate through the child nodes and their names
			LinkedList<String> children = new LinkedList<String>();
			NodeList childNodes = n.getChildNodes();
			for(int i = 0; i < childNodes.getLength(); i++) {
				Node child = childNodes.item(i);
				if(child.getNodeType() == Node.ELEMENT_NODE) {
					//child will be processed later
					unprocessedNodes.add(child);
					//assign the parent into parentLookup
					temp = child.getAttributes().getNamedItem("value");
					if(temp == null) {
						throw new Exception("Error in VGH structure, attribute " + index + "!!!");
					}
					String value = temp.getNodeValue();
					parentLookup.put(value, parent);
					//add to children for childLookup
					children.add(value);
					//validate containment of the child to the parent
					if(!parInt.contains(new Interval(value))) {
						throw new Exception("Error in VGH structure, attribute " + index + "!!!");
					}
				}
			}
			
			if(!children.isEmpty()) {
				//insert into parentLookup and childLookup
				String[] childNames = children.toArray(new String[0]);
				childLookup.put(parent, childNames);
			} else {
				//this should be a leaf interval
				leafIntervals.add(parInt);
			}	
		}
		
		//further input validation: if the attribute is categorical, 
		// check whether all numeric values map to a parent
		if(catDomMapping != null) {
			Enumeration<String> keys = catDomMapping.keys();
			while(keys.hasMoreElements()) {
				int val = catDomMapping.get(keys.nextElement());
				String gen = generalize(Integer.toString(val));
				if(gen == null) {
					throw new Exception("Malformed VGH, " + val + " cannot be generalized to any value!!!");
				}
			}
		}
	}
	
	/**
	 * Checks whether the VGH for this qid-attribute corresponds to a domain generalization
	 * hierarchy. This basically requires that all leave values in the VGH be at the same 
	 * depth from the root (i.e., the suppression value).  The check should be carried out 
	 * all quasi-identifier attributes of all full-domain anonymization methods (e.g., Datafly,
	 * Incognito, etc.).
	 * @return true if VGH corresponds to a DGH, false otherwise
	 */
	public boolean isDomainGeneralizationHierarchy() {
		int treeDepth = -1;
		ListIterator<Interval> leaves = leafIntervals.listIterator();
		while(leaves.hasNext()) {
			if(treeDepth == -1) {
				treeDepth = getDepth(leaves.next().toString());
			} else if(treeDepth != getDepth(leaves.next().toString())) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Calculates the depth of the value generalization hierarchy
	 * @param validateDGH if true, this method first checks whether the domain generalization
	 * hiearchy represents a value generalization hierarchy (i.e., whether all leaves are at 
	 * the same depth or not)
	 * @return Depth of the VGH
	 * @throws Exception If input validation fails
	 */
	public int getVGHDepth(boolean validateDGH) throws Exception{
		if(validateDGH) {
			//first check if the DGH corresponds to a VGH
			if(!isDomainGeneralizationHierarchy()) {
				throw new Exception("The DGH does not represent a VGH!!!");
			}
		}
		//get the depth of the first leaf
		int retVal = getDepth(leafIntervals.peek().toString());
		//return the depth of the first leaf
		if(leafIntervals.peek().isSingleton()) {
			return retVal;
		} else { //add +1 to count the first generalization from an original value 
			// (e.g., "35") to the corresponding leaf interval (e.g., "[25:50)")  
			return retVal + 1;
		}
	}
	
	/**
	 * Calculates the depth of a leaf interval (i.e., length of the path to the suppression value)
	 * @param leafInterval String representation of a leaf interval
	 * @return depth of the leaf interval
	 */
	private int getDepth(String leafInterval) {
		int retVal = 0;
		while(leafInterval.compareTo(suppValue) != 0) { //if equal, should return 0
			retVal++;
			leafInterval = generalize(leafInterval);
		}
		return retVal;
	}
	
	/**
	 * Getter for supression value
	 * @return root value of the DGH
	 */
	public String getSup() { //getter
		return suppValue;
	}
	
	/**
	 * Retrieves the generalization of any QID value (leaf or non-leaf)
	 * @param value some value from the QID's domain
	 * @return immediate parent of the value (or itself if DGH root is specified)
	 */
	public String generalize(String value) {
		//suppressed values can not be generalized, return itself
		if(value.compareTo(suppValue) == 0) {
			return value;
		}
		
		//first try fetching from parentLookup
		String parent = parentLookup.get(value);
		if(parent != null) {
			return parent;
		}
		//check containment into leaf intervals
		for(int i = 0; i < leafIntervals.size(); i++) {
			Interval curr = leafIntervals.get(i);
			if(curr.compareTo(value)) {
				return curr.toString();
			}
		}
		return null; //should never get here
	}
	
	/**
	 * Retrieves higher granularity values of some generalized value
	 * @param value some non-leaf value from DGH
	 * @return array of possible specializations of the value, or null if
	 *  the attribute is continuous and the value is not a node of the VGH 
	 */
	public String[] specialize(String value) throws Exception{
		//first try fetching from childLookup
		String[] children = childLookup.get(value);
		if(children != null) {
			return children;
		}
		//for categorical attributes that were mapped to a discrete domain,
		// we can populate the children from the interval bounds
		if(catDomMapping != null) {
			Interval intrvl = new Interval(value);
			LinkedList<String> specs = new LinkedList<String>(); //list of all possible specs
			for(int i = (int) intrvl.low; i < intrvl.high; i++) {
				specs.addLast(Integer.toString(i));
			}
			//depending on inclusion/exclusion type, some values in specs will be invalid
			if(intrvl.incType == Interval.TYPE_ExcLowExcHigh) {
				specs.removeFirst();
				specs.removeLast();
			} else if(intrvl.incType == Interval.TYPE_ExcLowIncHigh) {
				specs.removeFirst();
			} else if(intrvl.incType == Interval.TYPE_IncLowExcHigh) {
				specs.removeLast();
			} else { //intrvl.incType == Interval.TYPE_IncLowIncHigh
				if(intrvl.low == intrvl.high) { //e.g., [3,3]
					specs.removeFirst();
				}
			}
			//convert to String[] and return
			return specs.toArray(new String[0]);
		} else { //certainly not categorical. We are possibly dealing with a continuous domain! 
			Interval i = new Interval(value);
			if(i.incType == Interval.TYPE_IncLowIncHigh 
					&& i.low == i.high) {
				//return an array that contains value only
				String[] retVal = new String[1];
				retVal[0] = value;
				return retVal;
			} else {
				//possibly infinitely many specialization, cannot help you here.
				return null;
			}
		}
	}
	
	/**
	 * Checks whether a generalization matches a value
	 * @param value some highest granularity value (not generalized)
	 * @param generalization Any value from the VGH domain
	 * @return true if the value generalizes to the specified generalized value
	 */
	public boolean generalizesTo(String value, String generalization) throws Exception{
		Interval gen = new Interval(generalization);
		return gen.compareTo(value);
	}
	
	/**	Maps every non-leaf VGH node to a unique index, assigned by
	 * a breadth-first traversal of the VGH*/
	private Hashtable<String, Integer> nonLeaves = null;
	
	/**
	 * Get the non-leaf nodes of the VGH
	 * @return A hashtable that maps every non-leaf VGH node to a unique index, 
	 * assigned by a breadth-first traversal of the VGH
	 */
	public Hashtable<String, Integer> getNonLeaves() {
		if(nonLeaves == null) { //if not set yet, fill-in the hashtable
			nonLeaves = new Hashtable<String, Integer>();
			//list of VGH nodes that have not been processed yet
			LinkedList<String> unprocessed = new LinkedList<String>();
			unprocessed.add(suppValue);
			int count = 0;
			while(!unprocessed.isEmpty()) {
				String curr = unprocessed.removeFirst();
				//index of this VGH node will be the count
				nonLeaves.put(curr, new Integer(count));
				count++;
				//fetch the children from childLookup and add all to unprocessed
				String[] children = childLookup.get(curr);
				for(int i = 0; children != null && i < children.length; i++) {
					unprocessed.add(children[i]);				
				}
			}
		}
		return nonLeaves;
	}
	
	/** Maps each VGH entry to a list of ground-domain values (i.e., not generalized values)*/
	private Hashtable<String, String[]> generalizationSeq = null;
	
	/**
	 * Get the list of ground-domain values that the provided value represents
	 * (applies only to categorical attributes mapped to a discrete domain)
	 * @param val some value in the VGH
	 * @return starting with the value itself, returns a vector of generalizations
	 * all the way upto the suppression value
	 */
	public String[] getGeneralizationSequence(String val) {
		if(catDomMapping == null) { //discard if attribute is numeric
			return null;
		}
		if(generalizationSeq == null) { //if not set yet, fill-in the hashtable
			//first process the leaves, obtained from catDomMapping
			generalizationSeq = new Hashtable<String, String[]>();
			Enumeration<String> enu = catDomMapping.keys();
			while(enu.hasMoreElements()) {
				//current value from the ground-domain
				String key = enu.nextElement();
				LinkedList<String> seq = new LinkedList<String>();
				seq.add(key); //seq will become the generalization sequence
				double doubleVal = catDomMapping.get(key);
				String parent = "[" + doubleVal + "]";
				while((parent = generalize(parent)) != null) { //all the way to suppValue
					seq.add(parent);
					if(parent.compareTo(suppValue) == 0) {
						break; //if reached suppValue, stop
					}
				}
				//convert the list to array, and put into the hashtable
				String[] genSeq = seq.toArray(new String[0]);
				//for lookups with the categorical value
				generalizationSeq.put(key, genSeq); 
				//for lookups with the interval representation
				generalizationSeq.put("[" + Double.toString(doubleVal) + "]", genSeq);
			}
			//then add parents
			//list of generalizations that have been processed yet
			LinkedList<String> unprocessed = new LinkedList<String>();
			unprocessed.add(suppValue); //start with the root, suppression value
			while(!unprocessed.isEmpty()) {
				String curr = unprocessed.removeFirst();
				//child lookup gives easy access to children, who should be added
				// to unprocessed
				String[] children = childLookup.get(curr);
				for(int i = 0; children != null && i < children.length; i++) {
					unprocessed.add(children[i]);
				}
				//generate sequence for curr
				LinkedList<String> seq = new LinkedList<String>();
				seq.add(curr); //add itself
				String parent = curr;
				while((parent = generalize(parent)).compareTo(curr) != 0) { //all the way to suppValue
					seq.add(parent);
					if(parent.compareTo(suppValue) == 0) {
						break; //break at the suppression value
					}
				}
				//convert to array
				String[] genSeq =seq.toArray(new String[0]);
				//store in the table (only one version - since the value definitely
				// is not from the ground-domain)
				generalizationSeq.put(curr, genSeq);
			}
		}
		if(generalizationSeq.containsKey(val)) {
			return generalizationSeq.get(val);
		} 
//		try {
//			Interval i = new Interval(val);
//			double doubleVal = -1;
//			if(i.incType == Interval.TYPE_IncLowExcHigh || i.incType == Interval.TYPE_IncLowIncHigh) {
//				doubleVal = i.low;
//			} else {
//				doubleVal = i.high;
//			}
//			return generalizationSeq.get("[" + doubleVal + "]");
//		} catch(Exception e) {
//			e.printStackTrace();
//		}
		return null;
	}
	
	public int[] getLeafCategories(String val) {
		try { //try and parse to an interval
			int lowInc, highInc;
			Interval range = new Interval(val);
			if(range.incType == Interval.TYPE_IncLowExcHigh || range.incType == Interval.TYPE_IncLowIncHigh) {
				lowInc = (int) range.low;
			} else {
				lowInc = (int) range.low + 1;
			}
			if(range.incType == Interval.TYPE_ExcLowIncHigh || range.incType == Interval.TYPE_IncLowIncHigh) {
				highInc = (int) range.high;
			} else {
				highInc = (int) range.high - 1;
			}
			int[] retVal = new int[highInc - lowInc + 1];
			for(int i = lowInc; i <= highInc; i++) {
				retVal[i-lowInc] = i;
			}
			return retVal;
		} catch(Exception e) {
			int[] retVal = new int[1];
			retVal[0] = catDomMapping.get(val);
			return retVal;
		}
	}
}
