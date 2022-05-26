package featRep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedList;

import libsvm.svm_node;
import anonymizer.QIDAttribute;

/**
 * This class converts records in relational format to a feature vector 
 * based on various heuristics described in the following paper:
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
 * The class attribute is assumed to be the last attribute of the input file. Please replace
 * if that is not the case.
 * <p/>
 * The constructor parameters are as follows:
 * <ul> 
 * <li> A descriptor file that specifies attribute types (and domains for categorical atts)
 * <li> An anonymization configuration file
 * <li> Numeric feature representation heuristic (see Params)
 * <li> Categorical feature representation heuristic (see Params)
 * <li> If expected distance will be used, the method of computation (i.e., either assuming
 * all values are distributed uniformly over the domain or using QI-statistics)
 * </ul>
 */
public class FeatureRepresentation {
	/** Vector of class values*/
	private String[] classValues;
	
	/** Vector of attribute (either NumericAtt or CategoricalAtt object*/
	public Object[] attributes;
	
	/** Entry i is true if i^th attribute is numerical*/
	public boolean[] isCont;
	
	/** Entry i is true if i^th attribute has been generalized*/
	private boolean[] isGeneralized;
	
	/** Numeric representation heuristic of choice. See Params for alternatives.*/
	private int numRep;
	
	/** Categorical representation heuristic of choice. See Params for alternatives.*/
	private int catRep;
	
	/** Choice of expected distance calculation method. This one uses the PDF 
	 * obtained from an anonymization outputted with the genValsDist output format
	 * option of the anonymization toolbox.*/
	private boolean usePDFExp;
	
	/** Choice of expected distance calculation method. This one assumes all
	 * values within a generalization are uniformly distributed.*/
	private boolean useUniExp;
	
	/** Quasi-identifier attributes (indices should be adjusted
	 * according to id attributes)*/
	private QIDAttribute[] qids;
	
	/** List of identifier attribute indices within the original file*/
	private LinkedList<Integer> idAttributes;
	
	/**
	 * Class constructor
	 * @param descriptorFilename A names files that describes attribute types and/or domains
	 * @param anonConfig Anonymization configuration file
	 * @param numRep Numeric feature representation heuristic
	 * @param catRep Categorical feature representation heuristic
	 * @param usePDFExp Flag for calculating expected distance based on QI-statistics
	 * @param useUniExp Flag for calculating expected distance based on uniform distribution assumption
	 */
	public FeatureRepresentation(String descriptorFilename, QIDAttribute[] qids,
			LinkedList<Integer> idAttributes, int numRep, int catRep, 
			boolean usePDFExp, boolean useUniExp) {
		this.numRep = numRep;
		this.catRep = catRep;
		this.usePDFExp = usePDFExp;
		this.useUniExp = useUniExp;
		this.qids = qids;
		this.idAttributes = idAttributes;
		readDescriptor(descriptorFilename);
	}
	
	/**
	 * Checks if the attribute at index is an identifier
	 * @param index attribute index
	 * @return true if identifier, false otherwise
	 */
	private boolean isIdentifier(int index) {
		//iterate through conf.idAttributeIndices to find out
		for(int i = 0; i < idAttributes.size(); i++) {
			if(idAttributes.get(i) == index) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Read the descriptor file
	 * @param descriptor filename
	 */
	private void readDescriptor(String descriptor){
		try {
			FileReader descFileReader = new FileReader(descriptor);
			BufferedReader descInput = new BufferedReader(descFileReader);
			String line, attName;
			
			int numAtts = 0; //numAtts after discarding identifiers
			int numTotalAtts = 0; //total number of attributes
			ArrayList<Boolean> isCont_AL = new ArrayList<Boolean>();
			ArrayList<Object> attributes_AL = new ArrayList<Object>();
			while( (line = descInput.readLine()) != null) {
				//skip any comment lines and the @Relation descriptor
				if(!line.toLowerCase().startsWith("@attribute")) {
					continue;
				} else {
					//current line describes an attribute
					// remove the @attribute identifier and trim
					line = line.substring(10).trim();
					String[] temp = line.split(" ", 2);
					line = temp[1].trim();
					attName = temp[0].toLowerCase().trim();
				}
				//skip identifier attributes
				if(isIdentifier(numTotalAtts)) {
					numTotalAtts++;
					continue;
				}
				
				//check if current attribute is the class attribute
				if(attName.compareTo("class") == 0) {
					//get the class values
					if(line.compareTo("numeric") == 0) {
						throw new Exception("Numeric class attributes are not allowed.");
					} else {
						//remove the set characters on both ends
						line = line.substring(1, line.length()-1);
						classValues = line.replaceAll(" ", "").split(",");
						break;
					}
				} else {
					//check if attribute is numeric or categorical
					if(line.toLowerCase().compareTo("numeric") == 0) {
						isCont_AL.add(true);
						QIDAttribute qid = null;
						for(int i = 0; i < qids.length; i++) {
							if(numAtts == qids[i].index) {
								qid = qids[i];
							}
						}
						if(qid == null) {
							attributes_AL.add(new NumericAtt(attName, numAtts));
						} else {
							attributes_AL.add(new NumericAtt(attName, numAtts, qid, numRep));
						}
					} else {
						//remove the set characters on both ends
						line = line.substring(1, line.length()-1);
						String[] leaves = line.replaceAll(" ", "").split(",");
						
						isCont_AL.add(false);
						QIDAttribute qid = null;
						for(int i = 0; i < qids.length; i++) {
							if(numAtts == qids[i].index) {
								qid = qids[i];
							}
						}
						if(qid == null) {
							attributes_AL.add(new CategoricalAtt(attName, numAtts, leaves));
						} else {
							attributes_AL.add(new CategoricalAtt(attName, numAtts, leaves, qid, catRep));
						}
					}
				}
				numTotalAtts++;
				numAtts++;
			}
			numAtts++; //for the class-value
			
			//convert from array list to array for easy access
			isCont = new boolean[isCont_AL.size()];
			attributes = new Object[isCont.length];
			for(int i =0; i < isCont.length; i++) {
				isCont[i] = isCont_AL.get(i);
				attributes[i] = attributes_AL.get(i);
			}
			//isGeneralized i is true if attribute is a quasi-identifier attribute
			isGeneralized = new boolean[attributes.length];
			for(int i = 0; i < attributes.length; i++) {
				if(isCont[i] && ((NumericAtt) attributes[i]).qid != null)
					isGeneralized[i] = true;
				else if(!isCont[i] && ((CategoricalAtt) attributes[i]).qid != null)
					isGeneralized[i] = true;
				else
					isGeneralized[i] = false;
			}
			//close the file
			descInput.close();
			descFileReader.close();
		} catch(Exception e) {e.printStackTrace();}
	}
	
	/**
	 * Sets lower/upper bound for numeric attributes  (does nothing for categorical)
	 * @param inputFile Input data filename
	 */
	public void initialScan(String inputFile) {
		try {
			//open file
			FileReader myInputFile = new FileReader(inputFile);
			BufferedReader input = new BufferedReader(myInputFile);
			//go through the lines
			String line = null;
			while((line = input.readLine())!=null && line.compareTo("")!=0) {
				String[] temp = line.split(",");
				for(int i = 0; i < attributes.length; i++) {
					if(isCont[i]) {
						NumericAtt att = (NumericAtt) attributes[i]; 
						att.addValue(temp[att.getIndex()]); //use index to skip id attributes
					}
					else {
						CategoricalAtt att = (CategoricalAtt) attributes[i];
						att.addCategory(temp[att.getIndex()]); //use index to skip id attributes
					}
				}
			}
			//close the file
			input.close();
			myInputFile.close();
		} catch(Exception e) {e.printStackTrace();}
	}
	
	/** The i^th entry contains the index of the attribute feature i correspond to*/
	private int[] featAttMapping;
	
	/**
	 * Assigns feature indices to each attribute
	 */
	public void assignFeatureIndices() {
		//count the number of features needed to represent as a vector
		int maxFIndex = 1;
		for(int i=0; i < attributes.length; i++) {
			if(isCont[i]) { //attribute is continuous
				maxFIndex = ((NumericAtt) attributes[i]).updateIndices(maxFIndex);				
			}
			else {
				maxFIndex = ((CategoricalAtt) attributes[i]).updateIndices(maxFIndex);
			}
		}
		featAttMapping = new int[maxFIndex];
		featAttMapping[0] = -1; //class attribute
		for(int i = 0; i < attributes.length; i++) {
			if(isCont[i]) { //attribute is continuous
				int attFeatIndex = ((NumericAtt) attributes[i]).featureIndex;
				int numFeats = ((NumericAtt) attributes[i]).numFeatures;
				for(int j = 0; j < numFeats; j++) {
					featAttMapping[attFeatIndex+j] = i;
				}
			}
			else {
				int attFeatIndex = ((CategoricalAtt) attributes[i]).featureIndex;
				int numFeats = ((CategoricalAtt) attributes[i]).numFeatures;
				for(int j = 0; j < numFeats; j++) {
					featAttMapping[attFeatIndex+j] = i;
				}
			}
		}
	}
	
	/**
	 * Convert the input to a set of feature vectors, which will be written to the output
	 * file 
	 * @param inputFile input filename
	 * @param outputFile output filename
	 */
	public void featurize(String inputFile, String outputFile) {
		try {
			FileReader myInputFile = new FileReader(inputFile);
			BufferedReader input = new BufferedReader(myInputFile);
			
			FileWriter myOutputFile = new FileWriter(outputFile);
			BufferedWriter output = new BufferedWriter(myOutputFile);
			
			String line = null;
			String newline = System.getProperty("line.separator");
			String outputLine = null;
			while((line = input.readLine())!=null && line.trim().compareTo("") != 0) {
				String[] temp = line.split(",");
				//grab the class value from the last entry
				if(temp[temp.length-1].compareTo(classValues[0]) == 0) {
					outputLine = "-1 ";
				}
				else {
					outputLine = "+1 ";
				}
				for(int i=0; i < attributes.length; i++) {
					if(isCont[i]) { //attribute is continuous
						NumericAtt num = (NumericAtt) attributes[i]; //use index to skip identifiers
						outputLine += num.getFeaturizedValue(temp[num.getIndex()]); 
					}
					else {
						CategoricalAtt cat = (CategoricalAtt) attributes[i]; //use index to skip identifiers
						outputLine += cat.getFeaturizedValue(temp[cat.getIndex()]);
					}
				}
				output.write(outputLine+newline);
			}
			//close the files
			input.close();
			myInputFile.close();
			output.close();
			myOutputFile.close();
		} catch(Exception e) {e.printStackTrace();}
	}
	
	/**
	 * Computes the expected square distance
	 * @param x vector of features
	 * @param y vector of features
	 * @return Expected square distance between x and y
	 */
	public double squareDistance(svm_node[] x, svm_node[] y) {
		if(usePDFExp) {
			return squareDistancePDF(x, y);
		} else if(useUniExp){
			return squareDistanceUni(x, y);
		}
		return -1;
	}
	
	/**
	 * Computes the expected dot product
	 * @param x vector of features
	 * @param y vector of features
	 * @return Expected dot product of x and y
	 */
	public double dotProduct(svm_node[] x, svm_node[] y) {
		if(usePDFExp) {
			return dotProductPDF(x, y);
		} else if(useUniExp){
			return dotProductUni(x, y);
		}
		return -1;
	}
	
	/**
	 * Computes the expected dot product
	 * @param x vector of features
	 * @return Expected dot product of x and x
	 */
	public double dotProduct(svm_node[] x) {
		if(usePDFExp) {
			return dotProductPDF(x);
		} else if(useUniExp){
			return dotProductUni(x);
		}
		return -1;
	}
	
	/**
	 * Computes the expected square distance based on QI-statistics
	 * @param x vector of features
	 * @param y vector of features
	 * @return Expected square distance between x and y
	 */
	private double squareDistancePDF(svm_node[] x, svm_node[] y) {
		double dist = 0;
		int xlen = x.length;
		int ylen = y.length;
		int i = 0;
		int j = 0;
		while(i < xlen && j < ylen)
		{
			if(x[i].index == y[j].index) {
				int attIndex = featAttMapping[x[i].index];
				boolean useExpected = isGeneralized[attIndex];
				if(useExpected) { //generalized
					if(isCont[attIndex] && numRep != 4) {
						useExpected = false; //will use baseline represented by numRep
					} else if(!isCont[attIndex] && catRep != 4) {
						useExpected = false; //will use baseline represented by catRep
					}
				}
				if(!useExpected) {
					double diff = x[i++].value - y[j++].value; 
					dist += diff * diff;
				} else {
					if(isCont[attIndex]) {
						double mean1 = x[i++].value;
						double var1 = x[i++].value;
						double mean2 = y[j++].value;
						double var2 = y[j++].value;
						dist += var1 + var2 + mean1 * mean1 + mean2 * mean2
							- 2 * mean1 * mean2;
					} else {
						int leafCount = ((CategoricalAtt) attributes[attIndex]).leafValues.length;
						double sum = 0;
						for(int k = 0; k < leafCount; k++) {
							sum += x[i++].value * y[j++].value;
						}
						dist+= 1 - sum;
					}
				}
			}
			else if(x[i].index > y[j].index){
				dist += y[j].value * y[j].value;
				++j;
			}
			else{
				dist += x[i].value * x[i].value;
				++i;
			}
		}
		while(i < xlen){
			dist += x[i].value * x[i].value;
			i++;
		}
		while(j < ylen){
			dist += y[j].value * y[j].value;
			j++;
		}
		
		return dist;
	}
	
	/**
	 * Computes the expected dot product based on QI-statistics
	 * @param x vector of features
	 * @param y vector of features
	 * @return Expected dot product between x and y
	 */
	private double dotProductPDF(svm_node[] x, svm_node[] y) {
		double sum = 0;
		int xlen = x.length;
		int ylen = y.length;
		int i = 0;
		int j = 0;
		while(i < xlen && j < ylen)
		{
			if(x[i].index == y[j].index) {
				int attIndex = featAttMapping[x[i].index];
				boolean useExpected = isGeneralized[attIndex];
				if(useExpected) { //generalized
					if(isCont[attIndex] && numRep != 4) {
						useExpected = false; //will use baseline represented by numRep
					} else if(!isCont[attIndex] && catRep != 4) {
						useExpected = false; //will use baseline represented by catRep
					}
				}
				if(!useExpected) {
					sum += x[i++].value * y[j++].value;
				} else {
					if(isCont[attIndex]) {
						double mean1 = x[i++].value;
						i++; //by-pass variance of x[i]
						double mean2 = y[j++].value;
						j++; //by-pass variance of y[j]
						sum += mean1 * mean2;
					} else {
						int leafCount = ((CategoricalAtt) attributes[attIndex]).leafValues.length;
						for(int k = 0; k < leafCount; k++) {
							sum += x[i++].value * y[j++].value;
						}
					}
				}
			}
			else
			{
				if(x[i].index > y[j].index)
					++j;
				else
					++i;
			}
		}
		
		return sum;
	}
	
	/**
	 * Computes the expected dot product based on QI-statistics
	 * @param x vector of features
	 * @return Expected dot product between x and x
	 */
	private double dotProductPDF(svm_node[] x) {
		double sum = 0;
		int xlen = x.length;
		int i = 0;
		while(i < xlen)
		{
			int attIndex = featAttMapping[x[i].index];
			boolean useExpected = isGeneralized[attIndex];
			if(useExpected) { //generalized
				if(isCont[attIndex] && numRep != 4) {
					useExpected = false; //will use baseline represented by numRep
				} else if(!isCont[attIndex] && catRep != 4) {
					useExpected = false; //will use baseline represented by catRep
				}
			}
			if(!useExpected) {
				sum += x[i].value * x[i].value;
				i++;
			} else {
				if(isCont[attIndex]) {
					double mean = x[i++].value;
					double var = x[i++].value;
					sum += var + mean * mean;
				} else {
					int leafCount = ((CategoricalAtt) attributes[attIndex]).numFeatures;
					i += leafCount;
					sum += 1;
				}
			}
		}
		
		return sum;
	}
	
	/**
	 * Computes the expected square distance based on the assumption
	 * that values of a generalization are distributed uniformly
	 * @param x vector of features
	 * @param y vector of features
	 * @return Expected square distance between x and y
	 */
	private double squareDistanceUni(svm_node[] x, svm_node[] y) {
		double dist = 0;
		int xlen = x.length;
		int ylen = y.length;
		int i = 0;
		int j = 0;
		while(i < xlen && j < ylen)
		{
			if(x[i].index == y[j].index) {
				int attIndex = featAttMapping[x[i].index];
				boolean useExpected = isGeneralized[attIndex];
				if(useExpected) { //generalized
					if(isCont[attIndex] && numRep != 4) {
						useExpected = false; //will use baseline represented by numRep
					} else if(!isCont[attIndex] && catRep != 4) {
						useExpected = false; //will use baseline represented by catRep
					}
				}
				if(!useExpected) {
					double diff = x[i++].value - y[j++].value; 
					dist += diff * diff;
				} else {
					if(isCont[attIndex]) {
						double xLow = x[i++].value;
						double xHigh = x[i++].value;
						double yLow = y[j++].value;
						double yHigh = y[j++].value;
						dist += (1.0/3)*(xLow*xLow + xHigh*xHigh + yLow*yLow + yHigh*yHigh)
							+ (1.0/3)*(xLow*xHigh + yLow*yHigh)
							- (1.0/2)*(xLow*yLow + xLow*yHigh + yLow*xHigh + xHigh*yHigh);
					} else {
						int leafCount = ((CategoricalAtt) attributes[attIndex]).leafValues.length;
						double intersection = 0; //intersection on leaves
						int xCount = 0, yCount = 0;
						for(int k = 0; k < leafCount; k++) {
							if(x[i].value == 1) {
								xCount++;
							}
							if(y[j].value == 1) {
								yCount++;
							}
							intersection += x[i++].value * y[j++].value; 
						}
						dist += 1 - intersection / (xCount*yCount);
					}
				}
			}
			else if(x[i].index > y[j].index){
				dist += y[j].value * y[j].value;
				++j;
			}
			else{
				dist += x[i].value * x[i].value;
				++i;
			}
		}
		while(i < xlen){
			dist += x[i].value * x[i].value;
			i++;
		}
		while(j < ylen){
			dist += y[j].value * y[j].value;
			j++;
		}
		
		return dist;
	}
	
	/**
	 * Computes the expected dot product based on the assumption
	 * that values of a generalization are distributed uniformly
	 * @param x vector of features
	 * @param y vector of features
	 * @return Expected dot product of x and y
	 */
	private double dotProductUni(svm_node[] x, svm_node[] y) {
		double sum = 0;
		int xlen = x.length;
		int ylen = y.length;
		int i = 0;
		int j = 0;
		while(i < xlen && j < ylen)
		{
			if(x[i].index == y[j].index) {
				int attIndex = featAttMapping[x[i].index];
				boolean useExpected = isGeneralized[attIndex];
				if(useExpected) { //generalized
					if(isCont[attIndex] && numRep != 4) {
						useExpected = false; //will use baseline represented by numRep
					} else if(!isCont[attIndex] && catRep != 4) {
						useExpected = false; //will use baseline represented by catRep
					}
				}
				if(!useExpected) {
					sum += x[i++].value * y[j++].value;
				} else {
					if(isCont[attIndex]) {
						double xLow = x[i++].value;
						double xHigh = x[i++].value;
						double yLow = y[j++].value;
						double yHigh = y[j++].value;
						sum += ((xLow+xHigh)/2) * ((yLow+yHigh)/2); //use midpoint
					} else {
						double intersection = 0;
						int xCount = 0, yCount = 0;
						int leafCount = ((CategoricalAtt) attributes[attIndex]).leafValues.length;
						for(int k = 0; k < leafCount; k++) {
							if(x[i].value == 1) {
								xCount++;
							}
							if(y[j].value == 1) {
								yCount++;
							} //intersection is incremented only if both values are 1
							intersection += x[i++].value * y[j++].value;
						}
						sum += intersection / (xCount * yCount);
					}
				}
			}
			else
			{
				if(x[i].index > y[j].index)
					++j;
				else
					++i;
			}
		}
		
		return sum;
	}
	
	/**
	 * Computes the expected dot product based on the assumption
	 * that values of a generalization are distributed uniformly
	 * @param x vector of features
	 * @return Expected dot product of x and x
	 */
	private double dotProductUni(svm_node[] x) {
		double sum = 0;
		int xlen = x.length;
		int i = 0;
		while(i < xlen)
		{
			int attIndex = featAttMapping[x[i].index];
			boolean useExpected = isGeneralized[attIndex];
			if(useExpected) { //generalized
				if(isCont[attIndex] && numRep != 4) {
					useExpected = false; //will use baseline represented by numRep
				} else if(!isCont[attIndex] && catRep != 4) {
					useExpected = false; //will use baseline represented by catRep
				}
			}
			if(!useExpected) {
				sum += x[i].value * x[i].value;
				i++;
			} else {
				if(isCont[attIndex]) {
					double low = x[i++].value;
					double high = x[i++].value;
					sum += ((high-low)*(high-low))/12 + ((high+low)*(high+low))/4;
				} else {
					int leafCount = ((CategoricalAtt) attributes[attIndex]).leafValues.length;
					i += leafCount;
					sum += 1;
				}
			}
		}
		
		return sum;
	}
}
