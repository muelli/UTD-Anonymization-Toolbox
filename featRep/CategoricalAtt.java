package featRep;

import methods.Params;
import anonymizer.Interval;
import anonymizer.QIDAttribute;

public class CategoricalAtt {
	public String name;
	private int index;
	public String[] leafValues;
	public int numFeatures;
	public int featureIndex;
	public QIDAttribute qid;
	public int catRep;
	public CategoricalAtt(String name, int index, String[] leafValues) {
		this.name = name;
		this.index = index;
		this.leafValues = leafValues;
		numFeatures = leafValues.length; //not a quasi-identifier
		featureIndex = -1;
		qid = null;
		catRep = -1;
	}
	public CategoricalAtt(String name, int index, String[] leafValues, QIDAttribute qid, int catRep) {
		this.name = name;
		this.index = index;
		this.leafValues = leafValues;
		if(catRep == Params.Cat_PDF || catRep == Params.Cat_SetLeaves) {
			numFeatures = leafValues.length;
		} else if(catRep == Params.Cat_GenAsNewFeature || catRep == Params.Cat_PathToRoot){
			numFeatures = leafValues.length + qid.getNonLeaves().size();
		} else {
			numFeatures = -1;
		}
		featureIndex = -1;
		this.qid = qid;
		this.catRep = catRep;
	}
	public String getName() {
		return name;
	}
	public int getIndex() {
		return index;
	}
	public void addCategory(String val) {
	}
	public int updateIndices(int maxFIndex) {
		featureIndex = maxFIndex;
		maxFIndex+= numFeatures;
		return maxFIndex;
	}
	public String getFeaturizedValue(String val) {
		if(qid == null) {
			for(int i = 0; i < leafValues.length; i++) {
				if(val.compareTo(leafValues[i])==0) {
					return Integer.toString(featureIndex+i)+":1 ";
				}
			}
		} else {
			if(catRep == Params.Cat_PDF) {
				return getProbVector(val);
			} else if(catRep == Params.Cat_GenAsNewFeature) {
				return getGenFeature(val);
			} else if(catRep == Params.Cat_PathToRoot) {
				return getParentFeatures(val);
			} else if(catRep == Params.Cat_SetLeaves){
				return getLeafFeatures(val);
			}
		}
		return null;
	}
	private String getProbVector(String val) {
		String retVal = "";
		if(val.indexOf(":") == -1) {
			int catIndex = qid.catDomMapping.get(val);
			for(int i = 0; i < qid.catDomMapping.size(); i++) {
				if(i!=catIndex) {
					retVal += Integer.toString(featureIndex+i)+":0 ";
				} else {
					retVal += Integer.toString(featureIndex+catIndex)+":1 ";
				}
			}
			if(catIndex == -1) 
				System.out.println("Error in CategoricalAtt.getFeaturizedValue");
		} else {
			String[] temp = val.split(":");
			for(int i = 0; i < temp.length; i++) {
				retVal += Integer.toString(featureIndex+i)+":"+temp[i]+" ";
			}
		}
		return retVal;
	}
	private String getGenFeature(String val) {
		String retVal = "";
		int catIndex = -1;
		if(qid.catDomMapping.containsKey(val)) {
			catIndex = qid.catDomMapping.get(val);
		} else {
			try {
				catIndex = Integer.parseInt(val);
			} catch(Exception e1) {
				try {
					catIndex = (int) Double.parseDouble(val.substring(1, val.length()-1));
				} catch(Exception e2) {
					try {
						Interval i = new Interval(val);
						if(i.incType == Interval.TYPE_IncLowExcHigh || i.incType == Interval.TYPE_IncLowIncHigh) {
							catIndex = (int) i.low;
						} else {
							catIndex = (int) i.high;
						}
					} catch(Exception e3) {}
				}
			}
		}
		if(catIndex == -1)
			catIndex = qid.getNonLeaves().get(val)+qid.catDomMapping.size();
		if(catIndex == -1) {
			System.out.println("Error");
		}
		retVal += Integer.toString(featureIndex+catIndex)+":1 ";
		return retVal;
	}
	private String getParentFeatures(String val) {
		//check if the value is a leaf,
		// if so, add its index into the indices ds
		//always shift all nonLeaf indices by leaves.length
		String retVal = "";
		String[] parentList = qid.getGeneralizationSequence(val);
		int[] indices = new int[parentList.length];
		for(int i = 0; i < parentList.length; i++) {
			indices[i] = Integer.MAX_VALUE;
		}
		boolean isLeaf = false;
		for(int i = 0; !isLeaf && i < leafValues.length; i++) {
			if(leafValues[i].compareTo(parentList[0]) == 0) {
				indices[0] = featureIndex+i;
				isLeaf = true;
			}
		}
		int parentIndex = 0;
		if(isLeaf)
			parentIndex++;
		for(; parentIndex < parentList.length; parentIndex++) { //first sort
			int index = featureIndex+leafValues.length+qid.getNonLeaves().get(parentList[parentIndex]);
			int temp = 0;
			while(index > indices[temp]) {
				temp++;
			}
			for(int j = indices.length-2; j >= temp; j--) {
				indices[j+1] = indices[j]; 
			}
			indices[temp] = index;
		}
		for(int i = 0; i < parentList.length; i++) {
			retVal += Integer.toString(indices[i])+":1 ";
		}
		
		
		for(int i = 0; i < parentList.length-1; i++) {
			if(indices[i] >= indices[i+1] || indices[i] == Integer.MAX_VALUE || indices[i+1] == Integer.MAX_VALUE) {
				System.out.println();
			}
		}
		return retVal;
	}
	private String getLeafFeatures(String val) {
		String retVal = "";
		int[] indices = qid.getLeafCategories(val);
		for(int i = 0; i < indices.length; i++) {
			retVal += Integer.toString(featureIndex+indices[i])+":1 ";
		}
		return retVal;
	}
}
