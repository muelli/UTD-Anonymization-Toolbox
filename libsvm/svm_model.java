//
// svm_model
//
package libsvm;
public class svm_model implements java.io.Serializable
{
	static final long serialVersionUID = -6152154127304775851L;
	
	svm_parameter param;	// parameter
	int nr_class;		// number of classes, = 2 in regression/one class svm
	int l;			// total #SV
	svm_node[][] SV;	// SVs (SV[l])
	double[][] sv_coef;	// coefficients for SVs in decision functions (sv_coef[k-1][l])
	double[] rho;		// constants in decision functions (rho[k*(k-1)/2])
	double[] probA;         // pariwise probability information
	double[] probB;

	// for classification only

	int[] label;		// label of each class (label[k])
	int[] nSV;		// number of SVs for each class (nSV[k])
				// nSV[0] + nSV[1] + ... + nSV[k-1] = l
	public int numSVs() {
		return SV.length;
	}
	public String printSV(int index) {
		String result = "";
		if(index >= 0 && index < SV.length) {
			svm_node[] sv = SV[index];
			for(int i = 0; i < sv.length; i++) {
				result += sv[i].index+":"+sv[i].value+" "; 
			}
			return result;
		}
		else {
			return null;
		}
	}
};
