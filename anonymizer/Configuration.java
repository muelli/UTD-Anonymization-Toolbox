package anonymizer;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * The class that parses the configuration input, which includes 
 * QID attribute set, sensitive attributes and anonymization parameters (e.g., k and l).
 * 
<?xml version="1.0"?>
<!-- Sample configuration file. Attributes 12 and 0 are part of the QID, attribute 41 is sensitive. k = 32-->
<!-- Name attributes of 'att' nodes are not used, included just for reference.-->
<config method = 'Datafly' k = '5'>
	<input filename='census-income_ALL.data' separator=','/> <!-- If left blank, separator will be set as comma by default.-->
	<output filename='census-incomeK5.data' format ='genValsDist'/> <!-- Format options = {genVals, genValsDist, anatomy}. If left blank,
	output format will be set as genVals by default.-->
	<id> <!-- List of identifier attributes, if any, these will be excluded from the output -->
		<att index='1' name='att1'/>
		<att index='2' name='att2'/>
	</id>
	<qid>
		<att index='12' name='sex'>
			<map> <!-- Mapping of a categorical domain to a discrete-valued numeric domain.-->
				<entry cat='Female' int='0' /> <!-- Make sure that (1) the order matches that of the depth-first traversel order 
				(i.e., the left-most leaf gets the smallest value, right-most the largest) and (2) values start with 0 and are 
				incremented by 1.-->
				<entry cat='Male' int='1' />
			</map>
			<vgh value='[1:2]'> <!-- It is recommended that you omit the children that correspond to a category.-->
			</vgh>
		</att>
		<att index='0' name ='age'>
			<vgh value='[0:100)'>
				<node value='[0:50)'>
					<node value='[0:25)'/> <!-- No need to list the leaves, as the domain might be continuous.-->
					<node value='[25:50)'/>
				</node>
				<node value='[50:100)'>
					<node value='[50:75)'/>
					<node value='[75:100)'/>
				</node>
			</vgh>
		</att>
	</qid>
	<sens>
		<att index='41' name='salary'>
			<map>
				<entry cat='- 50000.' int='1' />
				<entry cat='50000+.' int='2' />
			</map>
		</att>
	</sens>
</config>
 */
public class Configuration {
	/** Outputs anonymized records in the same format as the input records, replacing
	 * quasi-identifier attribute values with their generalizations */
	public static final int OUTPUT_FORMAT_GENVALS = 1;
	
	/** Outputs anonymized records in the same format as the input records, replacing 
	 * quasi-identifier attribute values with their generalization and additional 
	 * information describing the distribution of values within each equivalence class.
	 * <p>
	 * Specifics of this output method and examples can be found in the following paper:
	 * <pre>
	 * &#64;inproceedings{anonClassification,
	 * author    = {Ali Inan and Murat Kantarcioglu and Elisa Bertino},
	 * title     = {Using Anonymized Data for Classification},
	 * booktitle = {ICDE},
	 * year      = {2009},
	 * pages     = {429-440}
	 * }
	 * </pre>
	 */
	public static final int OUTPUT_FORMAT_GENVALSDIST = 2;
	
	/** Outputs anonymized records in anatomized form (i.e., output two tables
	 * where the first one QIT contains quasi-identifier values and the second table
	 * ST contains sensitive values and all other attributes. The two tables can be
	 * joined over the field EID (added as the first column to both) representing the
	 * equivalence ID. Among these tables, QIT is written to a file named 
	 * <code>outputFilename + "QIT"</code> and ST is written to a file named
	 * <code>outputFilename + "ST"</code>.
	 * <p>
	 * Specifics of this output method and examples can be found in the following paper:
	 * <pre>
	 * &#64;inproceedings{anatomy,
	 * author = {Xiao, Xiaokui and Tao, Yufei},
	 * title = {Anatomy: simple and effective privacy preservation},
	 * booktitle = {VLDB '06: Proceedings of the 32nd international conference on Very large data bases},
	 * year = {2006},
	 * pages = {139--150},
	 * location = {Seoul, Korea},
	 * publisher = {VLDB Endowment}
	 * }
	 * </pre>
	 */
	public static final int OUTPUT_FORMAT_ANATOMY = 3;
	
	/** Datafly anonymization method*/
	public static final int METHOD_DATAFLY = 1;
	
	/** Mondrian anonymization method*/
	public static final int METHOD_MONDRIAN = 2;
	
	/** Incognito anonymization method with k-anonymity as privacy definition*/
	public static final int METHOD_INCOGNITO_K = 3;
	
	/** Incognito anonymization method with l-diversity as privacy definition*/
	public static final int METHOD_INCOGNITO_L = 4;
	
	/** Incognito anonymization method with t-closeness as privacy definition*/
	public static final int METHOD_INCOGNITO_T = 5;
	
	/** Anatomy anonymization method*/
	public static final int METHOD_ANATOMY = 6;
	
	/** Anonymization method */
	public int anonMethod = METHOD_MONDRIAN;
	
	/** Maximum number of tuples to be suppressed */
	public int suppressionThreshold = 10;
	
	/**k of k-anonymity */
	public int k = 10;
	
	/**l of l-diversity */
	public double l = 10;
	
	/**c of recursive (c,l)-diversity (not needed for entropy l-diversity*/
	public double c = 0.2;
	
	/**t of t-closeness */
	public double t = 0.2;
	
	/** input filename */
	public String inputFilename = null;
	
	/** attribute value separator for the input*/
	public String separator = ",";
	
	/** output filename */
	public String outputFilename = null;
	
	/** output format */
	public int outputFormat = OUTPUT_FORMAT_GENVALS;
	
	/** indices of identifier attributes*/
	public LinkedList<Integer> idAttributeIndices = new LinkedList<Integer>();
		
	/** attribute indices corresponding to sensitive attributes */
	public SensitiveAttribute[] sensitiveAtts = null;
	
	/** quasi-identifier attributes */
	public QIDAttribute[] qidAtts = null;
	
	/** configuration filename */
	public String configFilename = null;
	
	/**
	 * Class constructor
	 * @param configFile path to the configuration file
	 * @throws Exception
	 */
	public Configuration(String configFile) throws Exception {
		this.configFilename = configFile;
		//check if configuration file exists
		File f = new File(configFilename);
		if(!f.exists()) {
			throw new Exception("Configuration file " + configFilename + " does not exist!!!");
		}
		
		inputFilename = null;
		outputFilename = null;
		
		//parse the XML file
		parseConfigFile(configFilename);
		
		//check validity of the input
		checkValidity();
	}
	
	/**
	 * Class constructor
	 * @param args Vector of program arguments 
	 * (to set various options, including configuration filename)
	 * @throws Exception
	 */
	public Configuration(String[] args) throws SAXException, IOException, ParserConfigurationException, Exception {
		//set the configuration filename
		int index = getOptionPos("-config", args);
		if(index >= 0) {
			configFilename = args[index];
		} else {
			configFilename = "config.xml";
		}
		//check if configuration file exists
		File f = new File(configFilename);
		if(!f.exists()) {
			throw new Exception("Configuration file " + configFilename + " does not exist!!!");
		}
		
		inputFilename = null;
		outputFilename = null;
		
		//parse the XML file
		parseConfigFile(configFilename);
		
		//overwrite file parameters with user's program arguments
		setOptions(args);
		
		//check validity of the input
		checkValidity();
	}
	
	/**
	 * Parses the XML file
	 */
	private void parseConfigFile(String configFilename) throws Exception{
		Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File(configFilename));
		Node root = doc.getFirstChild();
		while(root.getNodeType() != Node.ELEMENT_NODE) { //iterate until first element node
			root = root.getNextSibling();
			if(root == null) {
				throw new Exception("Empty configuration file???");
			}
		}
		
		//root contains anonymization parameters k (of k-anonymity) and l (of l-diversity)
		NamedNodeMap atts = root.getAttributes();
		for(int j = 0; j < atts.getLength(); j++) {
            String attName = atts.item(j).getNodeName();
            if(attName.compareToIgnoreCase("k") == 0) {
            	this.k = Integer.parseInt(atts.item(j).getNodeValue());
            } else if(attName.compareToIgnoreCase("l") == 0) {
            	this.l = Double.parseDouble(atts.item(j).getNodeValue());
            } else if(attName.compareToIgnoreCase("c") == 0) {
            	this.c = Double.parseDouble(atts.item(j).getNodeValue());
            } else if(attName.compareToIgnoreCase("t") == 0) {
            	this.t = Double.parseDouble(atts.item(j).getNodeValue());
            } else if(attName.compareToIgnoreCase("method") == 0) {
            	String method = atts.item(j).getNodeValue();
            	setMethod(method);
            } else { //if you want to add more parameters, simply add more cases here
            	//throw new Exception("Unrecognized configuration parameter " + attName);
            }
		}
		
		//id, qid attributes and sensitive attributes are denoted by children
		// "id", "qid" and "sens" of the root	
		Node id = null;
		Node qid = null;
		Node sens = null;
		NodeList rootChildren = root.getChildNodes();
		for(int i = 0; i < rootChildren.getLength(); i++) {
			Node child = rootChildren.item(i);
			if(child.getNodeType() == Node.ELEMENT_NODE) {
				if(child.getNodeName().compareToIgnoreCase("input") == 0) {
					//parse input info
					NamedNodeMap nodeAtts = child.getAttributes();
					for(int j = 0; j < nodeAtts.getLength(); j++) {
			            String attName = nodeAtts.item(j).getNodeName();
			            if(attName.compareToIgnoreCase("filename") == 0) {
			            	//input filename
							String fname = nodeAtts.item(j).getNodeValue();
							setInputFilename(fname);
			            } else if(attName.compareToIgnoreCase("separator") == 0) {
			            	//separator
							separator = nodeAtts.item(j).getNodeValue();
			            }
					}
				} else if(child.getNodeName().compareToIgnoreCase("output") == 0) {
					//parse output info
					NamedNodeMap nodeAtts = child.getAttributes();
					for(int j = 0; j < nodeAtts.getLength(); j++) {
			            String attName = nodeAtts.item(j).getNodeName();
			            if(attName.compareToIgnoreCase("filename") == 0) {
			            	//output filename
							String fname = nodeAtts.item(j).getNodeValue();
							setOutputFilename(fname);
			            } else if(attName.compareToIgnoreCase("format") == 0) {
			            	//output format
							String format = nodeAtts.item(j).getNodeValue();
							setOutputFormat(format);
			            }
					}
				} else if(child.getNodeName().compareToIgnoreCase("id") == 0) {
					//parse the list of sensitive attributes
					id = child;
					parseIDAtts(id);
				} else if(child.getNodeName().compareToIgnoreCase("qid") == 0) {
					//parse quasi-identifier info
					qid = child;
					parseQIDAtts(qid);
				} else if(child.getNodeName().compareToIgnoreCase("sens") == 0) {
					//parse sensitive info
					sens = child;
					parseSensitiveAtts(sens);
				}
			}
		}
		
		//has to have qid atts
		if(qid == null) { 
			throw new Exception("No quasi-identifier???");
		}
	}
	
	/**
	 * Parses the info on a identifier attributes
	 * @param id The root node of id attributes
	 * @throws Exception
	 */
	private void parseIDAtts(Node id) throws Exception{
		//initialize the list of qid attributes
		NodeList idList = id.getChildNodes(); //iterate through qid attributes
		for(int i = 0; i < idList.getLength(); i++) {
			Node att = idList.item(i);
			if(att.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			NamedNodeMap nodeAtts = att.getAttributes();
			Node n = nodeAtts.getNamedItem("index");
			if(n != null) {
				int index = Integer.parseInt(n.getNodeValue());
				idAttributeIndices.add(index);
			}
		}
	}
	
	/**
	 * Parses the info on a quasi-identifier attribute
	 * @param qid The root node of a qid attribute
	 * @throws Exception
	 */
	private void parseQIDAtts(Node qid) throws Exception{
		//initialize the list of id attributes
		LinkedList<QIDAttribute> qidAttsList = new LinkedList<QIDAttribute>();
		NodeList qidList = qid.getChildNodes(); //iterate through id attributes
		for(int i = 0; i < qidList.getLength(); i++) {
			Node att = qidList.item(i);
			if(att.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			NamedNodeMap nodeAtts = att.getAttributes();
			Node n = nodeAtts.getNamedItem("index");
			if(n == null) {
				throw new Exception("Every quasi-identifier attribute should have an index!!!");
			}
			int index = Integer.parseInt(n.getNodeValue());
			qidAttsList.add(new QIDAttribute(index, att));
		}
		qidAtts = qidAttsList.toArray(new QIDAttribute[0]);
	}
	
	/**
	 * Parses the info on a sensitive attribute
	 * @param sens The root node of a sensitive attribute
	 */
	private void parseSensitiveAtts(Node sens) throws Exception{
		//initialize the list of sensitive attributes
		LinkedList<SensitiveAttribute> sensitiveAttsList = new LinkedList<SensitiveAttribute>();
		NodeList sensList = sens.getChildNodes(); //iterate through sensitive attributes
		for(int i = 0; i < sensList.getLength(); i++) {
			Node att = sensList.item(i);
			if(att.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			NamedNodeMap nodeAtts = att.getAttributes();
			Node n = nodeAtts.getNamedItem("index");
			if(n == null) {
				throw new Exception("Every sensitive attribute should have an index!!!");
			}
			int index = Integer.parseInt(n.getNodeValue());
			sensitiveAttsList.add(new SensitiveAttribute(index, att)); //insert the attribute
		}
		sensitiveAtts = sensitiveAttsList.toArray(new SensitiveAttribute[0]);
	}
	
	/**
	 * Sets the input filename
	 * @param filename path to the input file 
	 * @throws Exception
	 */
	public void setInputFilename(String filename) throws Exception{
		inputFilename = filename;
	}
	
	/**
	 * Sets the input filename
	 * @param filename path to the input file 
	 * @throws Exception
	 */
	public void setOutputFilename(String filename) {
		outputFilename = filename;
	}
	
	/**
	 * Sets the anonymization method
	 * @param method an anonymization method identifier
	 */
	public void setMethod(String method) {
    	if(method.compareToIgnoreCase("datafly") == 0) {
    		this.anonMethod = METHOD_DATAFLY;
    	} else if(method.compareToIgnoreCase("mondrian") == 0) {
    		this.anonMethod = METHOD_MONDRIAN;
    	} else if(method.compareToIgnoreCase("incognito_k") == 0) {
    		this.anonMethod = METHOD_INCOGNITO_K;
    	} else if(method.compareToIgnoreCase("incognito_l") == 0) {
    		this.anonMethod = METHOD_INCOGNITO_L;
    	} else if(method.compareToIgnoreCase("incognito_t") == 0) {
    		this.anonMethod = METHOD_INCOGNITO_T;
    	} else if(method.compareToIgnoreCase("anatomy") == 0) {
    		this.anonMethod = METHOD_ANATOMY;
    	} else {
    		System.out.println("WARNING: Unrecognized anonymization method, " +
    				"Mondrian will be used for anonymization!!!");
    		this.anonMethod = METHOD_MONDRIAN;    		
    	}
	}
	
	/**
	 * Sets the output format
	 * @param format an output format identifier
	 */
	public void setOutputFormat(String format) {
		if(format.toLowerCase().compareTo("genvals") == 0) {
			outputFormat = OUTPUT_FORMAT_GENVALS;
		} else if(format.toLowerCase().compareTo("genvalsdist") == 0) {
			outputFormat = OUTPUT_FORMAT_GENVALSDIST;
		} else if(format.toLowerCase().compareTo("anatomy") == 0) {
			outputFormat = OUTPUT_FORMAT_ANATOMY;
		} else {
			System.out.println("WARNING: Unrecognized output format, GenVals will be used");
		}
	}
	
	/**
	 * Sets all possible options specified in the arguments
	 * @param args list of arguments
	 */
	public void setOptions(String[] args) {
		int index = -1;
		if( (index = getOptionPos("-k", args)) >= 0) { 
			k = Integer.parseInt(args[index]);
		}
		if( (index = getOptionPos("-l", args)) >= 0) {
			l = Double.parseDouble(args[index]);
		}
		if( (index = getOptionPos("-t", args)) >= 0) {
			t = Double.parseDouble(args[index]);
		}
		if( (index = getOptionPos("-c", args)) >= 0) { 
			c = Double.parseDouble(args[index]);
		}
		if( (index = getOptionPos("-suppthreshold", args)) >= 0) {
			suppressionThreshold = Integer.parseInt(args[index]);
		}
		if( (index = getOptionPos("-input", args)) >= 0) {
			inputFilename = args[index];
		}
		if( (index = getOptionPos("-separator", args)) >= 0) { 
			separator = args[index];
		}
		if( (index = getOptionPos("-output", args)) >= 0) {
			outputFilename = args[index];
		}
		if( (index = getOptionPos("-outputformat", args)) >= 0) {
			setOutputFormat(args[index]);
		}
		if( (index = getOptionPos("-method", args)) >= 0) {
			setMethod(args[index]);
		}
	}
	
	/**
	 * If set, gets the index of the option
	 * @param option Option to be searched
	 * @param args List of all arguments
	 * @return Index of the option if set, -1 otherwise 
	 */
	private int getOptionPos(String option, String[] args) {
		int retVal = -1;
		for(int i = 0; i < args.length; i++) {
			if(args[i].compareToIgnoreCase(option) == 0) {
				retVal = i+1;
				break;
			}
		}
		if(retVal < 0 || retVal >= args.length) {
			return -1;
		} else {
			return retVal;
		}
	}
	
	/**
	 * Checks validity of the configuration file
	 * @throws Exception If not valid
	 */
	public void checkValidity() throws Exception{
		if(inputFilename == null) {
			throw new Exception("Error: No input file???");
		}
		//check if file exists
		File f = new File(inputFilename);
		if(!f.exists()) {
			throw new Exception("Input file not found: " + inputFilename + "!!!");
		}
		if(outputFilename == null) {
			throw new Exception("Error: No input file???");
		}
		//check if file exists
		f = new File(outputFilename);
		if(f.exists()) {
			System.out.println("WARNING: Output file will be overwritten (" + outputFilename + ")");
		}
		//check if Anatomy is used with Anatomy outputFormat
		if(anonMethod == METHOD_ANATOMY && outputFormat != OUTPUT_FORMAT_ANATOMY) {
			System.out.println("WARNING: Output format set to Anatomy, since anonymization" +
					" method is Anatomy");
			outputFormat = OUTPUT_FORMAT_ANATOMY;			
		}
	}
}
