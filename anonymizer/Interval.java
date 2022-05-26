package anonymizer;

/**
 * Data struture for generalized numeric attributes (intervals).
 * 
 * Each interval has its high and low value. Inc/exclusiveness of the
 * lower and upper bounds are determined by the incType property.
 * For easy storage, and/or hashing purposes a string representation is 
 * included. The string representation consists of low and high values, 
 * separated by ":". The boundary conditions are represented mathematically,
 * e.g., [3,4).
 */
public class Interval {
	/** lower bound of the interval	 */
	public double low;
	/** upper bound of the interval	 */
	public double high;
	/** string representation	 */
	private String str;
	/** inclusion type (e.g., [low, high) vs. [low, high])*/
	public int incType;
	
	/** lower bound inclusive, upper bound inclusive (e.g., [3,4])*/
	public static final int TYPE_IncLowIncHigh = 1;
	/** lower bound inclusive, upper bound exclusive (e.g., [3,4))*/
	public static final int TYPE_IncLowExcHigh = 2;
	/** lower bound exclusive, upper bound inclusive (e.g., (3,4])*/
	public static final int TYPE_ExcLowIncHigh = 3;
	/** lower bound exclusive, upper bound exclusive (e.g., (3,4))*/
	public static final int TYPE_ExcLowExcHigh = 4;
	
	/**
	 * Class constructor specifying the string representation
	 * @param stringRep lower and upper bounds separated by ":" OR a double value
	 */
	public Interval(String stringRep) throws Exception{
		if((stringRep.startsWith("[") || stringRep.startsWith("("))
				&& (stringRep.endsWith("]") || stringRep.endsWith(")"))) {
			if(stringRep.contains(":")) {		
				//set lower and upper bounds
				String[] boundaries = stringRep.substring(1, stringRep.length()-1).split(":");
				low = Double.parseDouble(boundaries[0]);
				high = Double.parseDouble(boundaries[1]);
				
				//set incType
				boolean incLow = false;
				if(stringRep.charAt(0) == '[') {
					incLow = true;
				}
				boolean incHigh = false;
				if(stringRep.charAt(stringRep.length()-1) == ']') {
					incHigh = true;
				}
				if(incLow) {
					if(incHigh) incType = TYPE_IncLowIncHigh;
					else incType = TYPE_IncLowExcHigh;
				} else {
					if(incHigh) incType = TYPE_ExcLowIncHigh;
					else incType = TYPE_ExcLowExcHigh;
				}
				
				str = stringRep;
			} else {
				if(stringRep.startsWith("(") && stringRep.endsWith(")")) {
					throw new Exception("Empty interval (" + str + ")!");
				}
				stringRep = stringRep.substring(1, stringRep.length()-1); //trim the boundary characters
				double val = Double.parseDouble(stringRep);
				low = high = val;
				str = "[" + Double.toString(val) + "]"; //using Double.toString is important (standardization)
				incType = TYPE_IncLowIncHigh;
			}
		} else { //no low and high, just a value
			double val = Double.parseDouble(stringRep);
			low = high = val;
			str = "[" + Double.toString(val) + "]"; //using Double.toString is important (standardization)
			incType = TYPE_IncLowIncHigh;
		}
		
		if(low > high || (low == high && incType == TYPE_ExcLowExcHigh)) {
			throw new Exception("Empty interval (" + str + ")!");
		}
	}
	
	/**
	 * Compares numeric values to intervals
	 * @param val numeric value that can be parsed into a double
	 * @return true if the specified value is within the interval
	 */
	public boolean compareTo(String val){
		try { //if parse-able to double
			double d = Double.parseDouble(val);
			return compareTo(d);
		} catch(Exception e) { //else parse to an interval (e.g., "[35]")
			try {
				Interval i = new Interval(val);
				return compareTo(i.low);
			} catch(Exception e1) {
				e1.printStackTrace();
			}
		}
		return false; //just so the compiler shuts up
	}
	/**
	 * Compares numeric values to intervals
	 * @param d numeric value
	 * @return true if the specified value is within the interval
	 */
	public boolean compareTo(double d) {
		switch (incType) {
        	case TYPE_ExcLowExcHigh:
        		if(low < d && d < high)
        			return true;
        		else
        			return false;
        	case TYPE_ExcLowIncHigh:
        		if(low < d && d <= high)
        			return true;
        		else
        			return false;
        	case TYPE_IncLowExcHigh:
        		if(low <= d && d < high)
        			return true;
        		else
        			return false;
        	case TYPE_IncLowIncHigh:
        		if(low <= d && d <= high)
        			return true;
        		else
        			return false;
        	default:
        		return true;
		}
	}
	
	/**
	 * @return true if the lower bound is inclusive
	 */	
	private boolean incLowerBound() {
		return (incType == TYPE_IncLowExcHigh || incType == TYPE_IncLowIncHigh);
	}
	/**
	 * @return true if the upper bound is inclusive
	 */
	private boolean incUpperBound() {
		return (incType == TYPE_ExcLowIncHigh || incType == TYPE_IncLowIncHigh);
	}
	
	/**
	 * Checks whether one interval contains within itself another interval
	 * @param that an interval object
	 * @return true if this interval contains that interval
	 */
	public boolean contains(Interval that) {
		//general case (correct given all intervals are consistent (non-empty, low < high)
		if(this.low > that.low)
			return false;
		if(this.high < that.high)
			return false;
		//special cases pertaining to inclusion types
		if(this.low == that.low && that.incLowerBound() && !this.incLowerBound()) {
			return false;
		}
		if(this.high == that.high && that.incUpperBound() && !this.incUpperBound()) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Singleton intervals are categories assigned integer values (e.g., "[35]" or "[35)").
	 * @return true if singleton, false otherwise
	 */
	public boolean isSingleton() {
		if(!str.contains(":")) {
			return true;
		}
		return false;
	}
	
	/**
	 * Splits this interval into two new intervals such that all
	 * values &lt= to the parameter value reside in the first interval
	 * and &gt the parameter value reside in the second interval
	 * (e.g., given (3:5] to be split at 4, the result would be (3:4] and
	 * (4:5]).  
	 * @param value boundary of the split
	 * @return two interval resulting from the split
	 * @throws Exception
	 */
	public Interval[] splitInclusive(double value) throws Exception{
		Interval[] retVal = new Interval[2];
		if(incLowerBound()) { //determine left boundary condition (inc/exc)
			retVal[0] = new Interval("[" + low + ":" + value + "]");
		} else {
			retVal[0] = new Interval("(" + low + ":" + value + "]");
		}
		if(incUpperBound()) { //determine right boundary condition (inc/exc)
			retVal[1] = new Interval("(" + value + ":" + high + "]");
		} else {
			retVal[1] = new Interval("(" + value + ":" + high + ")");			
		}
		return retVal;
	}
	
	/**
	 * Builds a predicate that tests for inclusion into this interval.
	 * This function is useful to implement the compareTo function in SQL
	 * queries (e.g., given interval [30:40] and attribute name age as 
	 * comparator, returns "30 &lt= age, age &gt= 40")   
	 * @param comparator Name of the object being compared with the interval
	 * @return String representation of the predicate
	 */
	public String getPredicate(String comparator) {
		String retVal = "";
		if(incLowerBound()) { //check if lower bound is inclusive
			retVal += low + "<=" + comparator;
		} else {
			retVal += low + "<" + comparator;
		}
		retVal += " AND "; //separator
		if(incUpperBound()) { //check if upper bound is inclusive
			retVal += comparator + "<=" + high;
		} else {
			retVal += comparator + "<" + high;
		}
		return retVal;
	}
	
	/**
	 * String values are parsed to double and checked for inclusion in the 
	 * interval (returns true if within the interval). For Interval objects
	 * string representations are compared (returns true if similar).
	 */
	public boolean equals(Object o) {
		if(o instanceof String) {
			//will first parse the string to double
			// and then use the compareTo member function
			return str.compareTo((String) o) == 0;
		} else if(o instanceof Interval) {
			//both are intervals, just compare the string
			//representations
			return str.compareTo(((Interval) o).str) == 0;
		} else {
			throw new ClassCastException("Cannot compare with this object.");
		}
	}
	
	public String toString() {
		return str;
	}
}
