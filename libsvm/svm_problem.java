package libsvm;
public class svm_problem implements java.io.Serializable
{
	static final long serialVersionUID = -6152154127114895851L;
	public int l;
	public double[] y;
	public svm_node[][] x;
}
