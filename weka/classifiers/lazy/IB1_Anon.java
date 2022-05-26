package weka.classifiers.lazy;

import java.util.Enumeration;
import java.util.Vector;

import weka.classifiers.Classifier;
import weka.classifiers.UpdateableClassifier;
import weka.core.Capabilities;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Option;
import weka.core.TechnicalInformation;
import weka.core.TechnicalInformationHandler;
import weka.core.Utils;
import weka.core.Capabilities.Capability;
import weka.core.TechnicalInformation.Field;
import weka.core.TechnicalInformation.Type;
import anonymizer.Configuration;
import anonymizer.Interval;
import anonymizer.QIDAttribute;

/**
 * Instance-based classification method for anonymized data as described in 
 * the following paper:
 * 
 * <pre>
 * &#64;inproceedings{DBLP:conf/icde/InanKB09,
 * author    = {Ali Inan and
 *             Murat Kantarcioglu and
 *             Elisa Bertino},
 * title     = {Using Anonymized Data for Classification},
 * booktitle = {ICDE},
 * year      = {2009},
 * pages     = {429-440}
 * }
 * </pre>
 * 
 * All quasi-identifier information is read from the configuration file 
 * that was used in anonymizing the original dataset. Please make sure that
 * any parameters passed as program arguments during anonymization are 
 * reflected in the configuration file (especially the outputFormat field).
 * <p/>
 * We assume that the configuration file did not contain any specifications 
 * for identifier attributes. Otherwise, the configuration file should be edited
 * such that all reference to id attributes are removed and the indices of 
 * quasi-identifier attributes are re-set according to the new schema (e.g., if
 * the index of a QID attribute was 4 and an id attribute was at index 2; the updated
 * index of the QID attribute should become 3 to reflect the removal of the id attribute).
 * <p/>
 * Generalized attributes are represented as attributes of type String within the 
 * WEKA framework. Such representation allows handling of complex generalizations. For
 * example if QI-Statistics are provided instead of generalizations we can compare the 
 * two distributions. On the other hand, if generalized values are provided, we can set
 * the mid-point for numeric attributes and treat generalizations as new categories. Another
 * major advantage is that, the user does not have to deal with the tedious work of listing
 * all possible generalizations in the ARFF file header (i.e., simply input 
 * "&#64;ATTRIBUTE name string" for a quasi-identifier attribute with the name "name").
 * <p/>
 * When comparing two generalized values, a distinct comparison function gen_distance is 
 * called. This function assumes that both generalized values are obtained from the same output
 * format of the anonymization toolbox. If this assumption fails, gen_distance will print an
 * error message and exit with an error code. Therefore it is important that the input records
 * are all output in a similar fashion.
 * <p/>
 * Among different output formats supported by the anonymization toolbox, this classifier
 * can only handle genVals and genValsDist. For the time being, we do not plan add support 
 * for the anatomy format due to the costly join operation involved. 
 */
public class IB1_Anon 
extends Classifier 
implements UpdateableClassifier, TechnicalInformationHandler {

	/** path to the configuration file*/
	private String configFile = "config.xml";
	
	/** for serialization */
	static final long serialVersionUID = -6152154127304895851L;

	/** The training instances used for classification. */
	private Instances m_Train;

	/** The minimum values for numeric attributes. */
	private double [] m_MinArray;

	/** The maximum values for numeric attributes. */
	private double [] m_MaxArray;

	/** QuasiIdentifiers for generalized attributes. */
	private QIDAttribute [] qids;
	
	/**
	 * Class constructor.
	 * @param confFilename name of the configuration file, used for anonymization
	 */
	public IB1_Anon() {
		super();
	}
	
	/**
	 * Class constructor.
	 * @param conf Anonymization configuration
	 */
	public IB1_Anon(Configuration conf) {
		super();
		try {
			if(conf.outputFormat == Configuration.OUTPUT_FORMAT_ANATOMY
					|| conf.anonMethod == Configuration.METHOD_ANATOMY) {
				//if the outputFormat has been changed through program arguments,
				// we have no way to discover that, because we have read the configuration
				// from file (where argument updates are not reflected)
				String message = "Anatomy output format is not supported (do not use program " +
						"arguments to set outputFormat)!!!";
				throw new Exception(message);
			}
			this.qids = conf.qidAtts;
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Returns a string describing classifier
	 * @return a description suitable for
	 * displaying in the explorer/experimenter gui
	 */
	public String globalInfo() {

		return "Nearest-neighbour classifier";
	}

	/**
	* Returns an enumeration describing the available options.
	* @return an enumeration of all the available options.
	*/
	public Enumeration listOptions() {
		Vector newVector = new Vector(1);
	    newVector.addElement(new Option(
	    		"\tPath to the anonymization configuration file\n" +
	    		"\t(default: config.xml)",
		      "config", 1, "-config <path>"));

	    Enumeration en = super.listOptions();
	    while (en.hasMoreElements()) {
	    	newVector.addElement(en.nextElement());
	    }
	    return  newVector.elements();
	}
	
	/**
	 * Returns the tip text for this property
	 * @return tip text for this propert
	 */
	public String configFileTipText() {
		  return "set the path to anonymization configuration file";
	}
	
	/**
	 * Get the anonymization configuration file path
	 * @return path to the configuration file
	 */
	public String getConfigFile() {
		return configFile;
	}
	
	/**
	 * Gets the current settings of IB1_Anon
	 * @return an array of strings suitable for passing to setOptions()
	 */
	public String[] getOptions () {
		Vector result = new Vector();

		result.add("-config");
		result.add("" + getConfigFile());

		String[] options = super.getOptions();
		for(int i = 0; i < options.length; i++) {
			result.add(options[i]);
		}
		return (String[]) result.toArray(new String[result.size()]);	  
	}
	
	/**
	 * <pre> -config &lt;num&gt;
	 * path to the anonymization configuration file
	 * (default config.xml).
	 * </pre>
	 * @param options the list of options as an array of strings
	 */
	public void setOptions (String[] options) throws Exception {
		String optionString = Utils.getOption("config", options);
		if (optionString.length() != 0) {
			setConfigFile(optionString);
		}
		super.setOptions(options);
	}
	
	private void setConfigFile(String config) throws Exception{
		configFile = config;		
		Configuration conf = new Configuration(config);
		if(conf.outputFormat == Configuration.OUTPUT_FORMAT_ANATOMY
				|| conf.anonMethod == Configuration.METHOD_ANATOMY) {
			//if the outputFormat has been changed through program arguments,
			// we have no way to discover that, because we have read the configuration
			// from file (where argument updates are not reflected)
			String message = "Anatomy output format is not supported!!!";
			throw new Exception(message);
		}
		this.qids = conf.qidAtts;
	}
	
	public TechnicalInformation getTechnicalInformation() {
		TechnicalInformation result;
		result = new TechnicalInformation(Type.CONFERENCE);
		result.setValue(Field.AUTHOR, "Ali Inan");
		result.setValue(Field.TITLE, "Hybrid Classification");	    
		return result;
	}

	/**
	 * Returns default capabilities of the classifier.
	 * @return      the capabilities of this classifier
	 */
	public Capabilities getCapabilities() {
		Capabilities result = super.getCapabilities();

		// attributes
		result.enable(Capability.NOMINAL_ATTRIBUTES);
		result.enable(Capability.NUMERIC_ATTRIBUTES);
		result.enable(Capability.STRING_ATTRIBUTES); //for generalized numeric attributes
		//result.enable(Capability.DATE_ATTRIBUTES);
		//result.enable(Capability.MISSING_VALUES);

		// class
		result.enable(Capability.NOMINAL_CLASS);
		//result.enable(Capability.MISSING_CLASS_VALUES);

		// instances
		result.setMinimumNumberInstances(0);

		return result;
	}

	/**
	 * Generates the classifier.
	 * @param instances set of instances serving as training data 
	 * @throws Exception if the classifier has not been generated successfully
	 */
	public void buildClassifier(Instances instances) throws Exception {

		// can classifier handle the data?
		getCapabilities().testWithFail(instances);

		// remove instances with missing class
		instances = new Instances(instances);
		instances.deleteWithMissingClass();

		m_Train = new Instances(instances, 0, instances.numInstances());

		m_MinArray = new double [m_Train.numAttributes()];
		m_MaxArray = new double [m_Train.numAttributes()];
		for (int i = 0; i < m_Train.numAttributes(); i++) {
			m_MinArray[i] = m_MaxArray[i] = Double.NaN;
		}
		Enumeration enu = m_Train.enumerateInstances();
		while (enu.hasMoreElements()) {
			updateMinMax((Instance) enu.nextElement());
		}
	}

	/**
	 * Updates the classifier.
	 *
	 * @param instance the instance to be put into the classifier
	 * @throws Exception if the instance could not be included successfully
	 */
	public void updateClassifier(Instance instance) throws Exception {

		if (m_Train.equalHeaders(instance.dataset()) == false) {
			throw new Exception("Incompatible instance types");
		}
		if (instance.classIsMissing()) {
			return;
		}
		m_Train.add(instance);
		updateMinMax(instance);
	}

	/**
	 * Classifies the given test instance.
	 *
	 * @param instance the instance to be classified
	 * @return the predicted class for the instance 
	 * @throws Exception if the instance can't be classified
	 */
	public double classifyInstance(Instance instance) throws Exception {

		if (m_Train.numInstances() == 0) {
			throw new Exception("No training instances!");
		}

		double distance, minDistance = Double.MAX_VALUE, classValue = 0;
		updateMinMax(instance);
		Enumeration enu = m_Train.enumerateInstances();
		while (enu.hasMoreElements()) {
			Instance trainInstance = (Instance) enu.nextElement();
			if (!trainInstance.classIsMissing()) {
				distance = distance(instance, trainInstance);
				if (distance < minDistance) {
					minDistance = distance;
					classValue = trainInstance.classValue();
				}
			}
		}

		return classValue;
	}

	/**
	 * Returns a description of this classifier.
	 *
	 * @return a description of this classifier as a string.
	 */
	public String toString() {

		return ("IB1 classifier");
	}

	/**
	 * Calculates the distance between two instances
	 *
	 * @param first the first instance
	 * @param second the second instance
	 * @return the distance between the two given instances
	 */          
	private double distance(Instance first, Instance second) throws Exception {

		double diff, distance = 0;

		for(int i = 0; i < m_Train.numAttributes(); i++) { 
			if (i == m_Train.classIndex()) {
				continue;
			}
			if (m_Train.attribute(i).isNominal()) {

				// If attribute is nominal
				if (first.isMissing(i) || second.isMissing(i)) {
					distance += 1;
				}
				else {
					if((int)first.value(i) != (int)second.value(i)) {
						distance += 1;
					}
				}
			} else if (m_Train.attribute(i).isNumeric()){

				// If attribute is numeric
				if (first.isMissing(i) || second.isMissing(i)){
					if (first.isMissing(i) && second.isMissing(i)) {
						diff = 1;
					} else {
						if (second.isMissing(i)) {
							diff = norm(first.value(i), i);
						} else {
							diff = norm(second.value(i), i);
						}
						if (diff < 0.5) {
							diff = 1.0 - diff;
						}
					}
				} else {
					diff = norm(first.value(i), i) - norm(second.value(i), i);
				}
				distance += diff * diff;
			} else {
				for(int j = 0; j < qids.length; j++) {
					if(qids[j].index == i) {
						distance += gen_distance(first.stringValue(i), second.stringValue(i), qids[j]);
					}
				}
			}
		}

		return distance;
	}
	
	/**
	 * Method of calculating the generalized distance between any two values.
	 * @param val1 String representation of the first generalized value
	 * @param val2 String representation of the second generalized value
	 * @param qid Quasi-identifier attribute
	 * @return Expected distance between two generalized values
	 */
	private double gen_distance(String val1, String val2, QIDAttribute qid) throws Exception {
		//first convert category value to its discrete mapping
		if(qid.catDomMapping != null) {
			Integer index = qid.catDomMapping.get(val1); 
			if(index != null) {
				val1 = Integer.toString(index);
			}
			index = qid.catDomMapping.get(val2); 
			if(index != null) {
				val2 = Integer.toString(index);
			}
		}
		
		//there are 2 different cases
		// (1) value is an interval
		// (2) value is a distribution
		//Assumption: the same case holds for both values
		// If the assumption fails, outputs error message and exits
		boolean parsableAsIntervals = true;
		Interval i1 = null, i2 = null;
		try {
			i1 = new Interval(val1);
			i2 = new Interval(val2);
		} catch(Exception e) {
			parsableAsIntervals = false;
		}
		if(qid.catDomMapping == null) { //numeric, get the mid-point
			if(parsableAsIntervals) {
				//represent each with the mid-point
				double mid1 = (i1.high + i1.low) / 2;
				double mid2 = (i2.high + i2.low) / 2;
				//normalize according to the range (obtained from suppression value
				Interval sup = null;
				try { //there should be no exceptions here
					sup = new Interval(qid.getSup());
				} catch(Exception e) { e.printStackTrace();}				
				double norm1 = (mid1 - sup.low) / (sup.high - sup.low);
				double norm2 = (mid2 - sup.low) / (sup.high - sup.low);
				//calculate difference
				double diff = norm1 - norm2;
				return diff * diff;
			} else { //parse as distribution
				try {
					//in case one is original, let's find out which one is a distribution
					if(val2.contains(":")) { //if it is val2, swap the two
						String temp = val2;
						val2 = val1;
						val1 = temp;
					}
					String[] fVal = val1.split(":");
					double mean1 = Double.parseDouble(fVal[0]);
					double var1 = Double.parseDouble(fVal[1]);
					
					//now if val2 does not contain ":", we should convert to 
					// a prob distribution manually
					double mean2, var2;
					if(val2.contains(":")) {
						String[] sVal = val2.split(":");
						mean2 = Double.parseDouble(sVal[0]);
						var2 = Double.parseDouble(sVal[1]);
					} else {
						var2 = 0;
						mean2 = Double.parseDouble(val2);
					}
					//return expected distance
					return (var1 + var2 + mean1*mean1 + mean2*mean2 - 2*mean1*mean2);
				} catch(Exception e) {
					throw new Exception("Incompatible values cannot be compared!!!");
				}
			}
		} else { //categorical
			if(parsableAsIntervals) { //since each generalization is a separate value
				// and the comparison function is Hamming distance, just compare the string
				// representations. If same, distance is 0, otherwise 1.
				if(val1.compareTo(val2) == 0) {
					return 0;
				} else {
					return 1;
				}
			} else { //parse as distribution
				try {
					//in case one is original, let's find out which one is a distribution
					if(val2.contains(":")) { //if it is val2, swap the two
						String temp = val2;
						val2 = val1;
						val1 = temp;
					}
					String[] fVal = val1.split(":");
					double[] fProb = new double[fVal.length];
					for(int i = 0; i < fVal.length; i++) {
						fProb[i] = Double.parseDouble(fVal[i]);
					}

					//now if val2 does not contain ":", we should convert to 
					// a prob distribution manually
					double[] sProb = new double[fVal.length];
					if(val2.contains(":")) {
						String[] sVal = val2.split(":");
						for(int i = 0; i < sVal.length; i++) {
							sProb[i] = Double.parseDouble(sVal[i]);
						}
					} else {
						int index = Integer.parseInt(val2);
						sProb[index] = 1;
					}
					
					//compare the two distributions
					double sum = 0;
					for(int i = 0; i < fVal.length; i++) {
						sum += fProb[i] * sProb[i];
					}
					return 1 - sum;
				} catch(Exception e) {
					throw new Exception("Incompatible values cannot be compared!!!");
				}
			}
		}
	}

	/**
	 * Normalizes a given value of a numeric attribute.
	 *
	 * @param x the value to be normalized
	 * @param i the attribute's index
	 * @return the normalized value
	 */
	private double norm(double x,int i) {

		if (Double.isNaN(m_MinArray[i]) || Utils.eq(m_MaxArray[i], m_MinArray[i])) {
			return 0;
		} else {
			return (x - m_MinArray[i]) / (m_MaxArray[i] - m_MinArray[i]);
		}
	}

	/**
	 * Updates the minimum and maximum values for all the attributes
	 * based on a new instance.
	 *
	 * @param instance the new instance
	 */
	private void updateMinMax(Instance instance) {

		for (int j = 0;j < m_Train.numAttributes(); j++) {
			if ((m_Train.attribute(j).isNumeric()) && (!instance.isMissing(j))) {
				if (Double.isNaN(m_MinArray[j])) {
					m_MinArray[j] = instance.value(j);
					m_MaxArray[j] = instance.value(j);
				} else {
					if (instance.value(j) < m_MinArray[j]) {
						m_MinArray[j] = instance.value(j);
					} else {
						if (instance.value(j) > m_MaxArray[j]) {
							m_MaxArray[j] = instance.value(j);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Main method for testing this class.
	 *
	 * @param argv should contain command line arguments for evaluation
	 * (see Evaluation).
	 */
	public static void main(String [] argv) {
		runClassifier(new IB1_Anon(), argv);
	}
}
