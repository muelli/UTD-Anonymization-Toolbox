package anatomy;

/**
 * This class manages the counts of sensitive attribute values. Associated with each object
 * is a double attribute value and the count of tuples in the dataset that have that value.
 */
public class AttValueCount implements Comparable{
	/** Attribute value */
	private double value;
	/** Number of associated tuples*/
	private int count;
	
	/**
	 * Class constructor
	 * @param value attribute value
	 * @param count number of associated tuples
	 */
	public AttValueCount(double value, int count) {
		this.value = value;
		this.count = count;
	}
	
	/**
	 * Decrement the count for this attribute value
	 * @return true if the count is larger than 0, false otherwise
	 */
	public boolean decrement() {
		if(count > 0) {
			count--;
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Used for sorting AttValueCounts in DESCENDING order
	 */
	public int compareTo(Object o) {
		AttValueCount that = (AttValueCount) o;
		if(this.count < that.count) {
			return 1;
		} else if(this.count == that.count) {
			return 0;
		} else {
			return -1;
		}
	}
	
	public String toString() {
		return "[" + value + ", " + count + "]";
	}
	
	/**
	 * Getter for the attribute value
	 * @return the attribute value
	 */
	public double getValue() {
		return value;
	}
	
	/**
	 * Getter for the tuple count
	 * @return count of tuples associated with the value
	 */
	public int getCount() {
		return count;
	}
}
