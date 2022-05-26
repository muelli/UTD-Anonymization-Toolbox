package featRep;

import methods.Params;
import anonymizer.QIDAttribute;
import anonymizer.Interval;

public class NumericAtt{
	public String name;
	private int index;
	public int numFeatures;
	public double lowerBound;
	private double upperBound;
	boolean isNormalizationValSet;
	double normalizationFactor;
	public int featureIndex; //this index holds mean, (featureIndex+1) holds variance
	public QIDAttribute qid;
	private int numRep;
	public NumericAtt(String name, int index) {
		this.name = name;
		this.index = index;
		numFeatures = 1;
		lowerBound = Double.MAX_VALUE;
		upperBound = -1* Double.MAX_VALUE;
		isNormalizationValSet = false;
		normalizationFactor = 0;
		featureIndex = -1;
		qid = null;
		numRep = -1;
	}
	public NumericAtt(String name, int index, QIDAttribute qid, int numRep) {
		this.name = name;
		this.index = index;
		lowerBound = Double.MAX_VALUE;
		upperBound = -1* Double.MAX_VALUE;
		isNormalizationValSet = false;
		normalizationFactor = 0;
		featureIndex = -1;
		this.qid = qid;
		this.numRep = numRep;

		if(numRep == Params.Num_ReplaceWithMean) {
			numFeatures = 1;
		} else if(numRep == Params.Num_MinMaxFeatures || numRep == Params.Num_PDF) {
			numFeatures = 2;
		} else if(numRep == Params.Num_RangeAsNewFeature) {
			numFeatures = qid.getNonLeaves().size() + 1;
			//one per generalization, and one for the actual value
		} else {
			numFeatures = -1;
		}
	}
	public String getName() {
		return name;
	}
	public int getIndex() {
		return index;
	}
	public void addValue(String val) {
		if(qid == null) {
			double valDouble = Double.parseDouble(val);
			if(valDouble < lowerBound) {
				lowerBound = valDouble;
			}
			else if(valDouble > upperBound) {
				upperBound = valDouble;
			}
		}
	}
	public int updateIndices(int maxFIndex) {
		featureIndex = maxFIndex;
		maxFIndex+= numFeatures;
		return maxFIndex;
	}
	public String getFeaturizedValue(String val) {
		if(qid == null) {
			double numVal = normalize(Double.parseDouble(val));
			return Integer.toString(featureIndex)+":"+Double.toString(numVal)+" ";
		}
		else {
			if(numRep == Params.Num_PDF) {
				return getProbVector(val);
			} else if(numRep == Params.Num_MinMaxFeatures) {
				return getMinMaxFeatures(val);
			} else if(numRep == Params.Num_RangeAsNewFeature) {
				return getGenFeature(val);
			} else if(numRep == Params.Num_ReplaceWithMean){
				return getMidPoint(val);
			}
		}
		return null;
	}
	private String getProbVector(String val) {
		double mean = Double.NaN, variance = Double.NaN;
		if(val.indexOf(":") == -1) {
			mean = normalize(Double.parseDouble(val));
			variance = 0;
		}
		else {
			String[] temp = val.split(":");
			mean = 2 * Double.parseDouble(temp[0]) - 1;
			variance = 4* Double.parseDouble(temp[1]);
		}
		String retVal = Integer.toString(featureIndex)+":"+Double.toString(mean)+" ";
		retVal += Integer.toString(featureIndex+1)+":"+Double.toString(variance)+" ";
		return retVal;
	}
	private String getMinMaxFeatures(String val) {
		double min = Double.NaN, max = Double.NaN;
		if(val.indexOf(":") == -1) {
			min = normalize(Double.parseDouble(val));
			max = min;
		}
		else {
			String[] temp = val.substring(1, val.length()-1).split(":");
			min = normalize(Double.parseDouble(temp[0]));
			max = normalize(Double.parseDouble(temp[1]));
		}
		String retVal = Integer.toString(featureIndex)+":"+Double.toString(min)+" ";
		retVal += Integer.toString(featureIndex+1)+":"+Double.toString(max)+" ";
		return retVal;
	}
	private String getGenFeature(String val) {
		if(val.indexOf(":") == -1) {
			double numVal = normalize(Double.parseDouble(val));
			String retVal = Integer.toString(featureIndex)+":"+Double.toString(numVal)+" ";
			return retVal;
		} else {
			int catIndex = qid.getNonLeaves().get(val);
			if(catIndex == -1) {
				System.out.println("Error");
			}
			String retVal = Integer.toString(featureIndex+catIndex+1)+":1 ";
			return retVal;
		}
	}
	private String getMidPoint(String val) {
		double mid = Double.NaN;
		if(val.indexOf(":") == -1) {
			mid = normalize(Double.parseDouble(val));
		}
		else {
			String[] temp = val.substring(1, val.length()-1).split(":");
			double min = normalize(Double.parseDouble(temp[0]));
			double max = normalize(Double.parseDouble(temp[1]));
			mid = (max + min)/2;
		}
		String retVal = Integer.toString(featureIndex)+":"+Double.toString(mid)+" ";
		return retVal;
	}
	public double normalize(double val) { //maps any value into [-1, 1] according to upper and lower bounds
		if(qid!=null && !isNormalizationValSet) {
			try {
				Interval range = new Interval(qid.getSup());
				upperBound = range.high;
				lowerBound = range.low;
			} catch(Exception e) {e.printStackTrace();}
		}
		if(!isNormalizationValSet) {
			normalizationFactor = 2 / (upperBound-lowerBound);
			isNormalizationValSet = true;
		}
		return ((val-lowerBound)*normalizationFactor)-1;
	}
}
