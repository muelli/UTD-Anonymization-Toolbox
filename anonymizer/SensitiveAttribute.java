package anonymizer;

import java.util.Hashtable;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Data structure that holds all relevant information related to a sensitive attribute.
 * Namely, attribute index in the input, string->integer domain mapping for 
 * categorical attributes and etc.
 */
public class SensitiveAttribute {
	/**	Mapping from the categorical domain to discrete-valued numerical (integer)
	 * domain for categorical attributes*/
	public Hashtable<String, Integer> catDomMapping = null;
	
	/** Index of the attribute */
	public int index = -1;
	
	/**
	 * Class constructor
	 * @param att The node that describes a QID attribute in the configuration file
	 * @param index Index of the attribute
	 */
	public SensitiveAttribute(int index, Node att) throws Exception {
		this.index = index;
		
		//iterate through the children to get the mapping (if exists) and the VGH
		NodeList childList = att.getChildNodes(); 
		for(int i = 0; i < childList.getLength(); i++) {
			Node child = childList.item(i);
			if(child.getNodeType() != Node.ELEMENT_NODE) { //omit any non-element nodes (e.g., comments)
				continue;
			}
			else if(child.getNodeName().compareTo("map") == 0) {
				parseMapping(child);
			}
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
				throw new Exception("Cannot parse the mapping of the sensitive attribute at index" + index);
			} else {
				key = n.getNodeValue();
			}
			n = nodeAtts.getNamedItem("int");
			if(n == null) {
				throw new Exception("Cannot parse the mapping of the sensitive attribute at index" + index);
			} else {
				value = Integer.parseInt(n.getNodeValue());
			}
			//add the pair to the mapping
			catDomMapping.put(key, value);
		}
	}
	
}
