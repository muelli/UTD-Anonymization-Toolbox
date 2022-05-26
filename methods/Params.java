package methods;


public class Params {
	private String[] args;
	
	public static final int Num_RangeAsNewFeature = 1;
	public static final int Num_ReplaceWithMean = 2;
	public static final int Num_MinMaxFeatures = 3;
	public static final int Num_PDF = 4;
	
	public static final int Cat_GenAsNewFeature = 1;
	public static final int Cat_PathToRoot = 2;
	public static final int Cat_SetLeaves = 3;
	public static final int Cat_PDF = 4;
	
	public static final int Ker_LINEAR = 0;
	public static final int Ker_POLY = 1;
	public static final int Ker_RBF = 2;
	public static final int Ker_SIGMOID = 3;
	public static final int Ker_PRECOMPUTED = 4;
	
	public boolean usePDFExpected; //true or false
	public boolean useUniformExpected; //true or false
	public int numericRepOption;
	public int categoricalRepOption;
	public int kernelType;

	public Params(String[] args) throws Exception{
		this.args = args;
		//DEFAULTS		
		usePDFExpected = true;
		useUniformExpected = false;
		
		numericRepOption = Num_PDF;
		categoricalRepOption = Cat_PDF;
		kernelType = Ker_RBF;
		
		String value;
		if((value = isSet("-uni"))!=null) {
			useUniformExpected = Boolean.parseBoolean(value);
		}
		if((value = isSet("-pdf")) != null) {
			usePDFExpected = Boolean.parseBoolean(value);
			if(!usePDFExpected) {
				numericRepOption = Num_ReplaceWithMean;
				categoricalRepOption = Cat_SetLeaves;
			}
		}
		
		if((value = isSet("-catRep"))!= null) {
			categoricalRepOption = Integer.parseInt(value);
		}
		if((value = isSet("-numRep"))!= null) {
			numericRepOption = Integer.parseInt(value);
		}
		if((value = isSet("-kernel"))!= null) {
			kernelType = Integer.parseInt(value);
		}
		
		checkParameterValiditiy();
	}
	
	private void checkParameterValiditiy() throws Exception{
		if(useUniformExpected && usePDFExpected) {
			String message = "WARNING: You cannot set both pdf and uni! Resetting to pdf!";
			System.out.println(message);
			useUniformExpected = false;
		}
		if(usePDFExpected && (numericRepOption != Num_PDF || categoricalRepOption != Cat_PDF)) {
			System.out.println("WARNING: Overwriting numRep and catRep according to usePDFExpected!");
			numericRepOption = Num_PDF;
			categoricalRepOption = Cat_PDF;
		}
		if(useUniformExpected && (numericRepOption != Num_MinMaxFeatures || categoricalRepOption != Cat_SetLeaves )) {
			System.out.println("WARNING: Overwriting numRep and catRep according to useUniformExpected!!!");
			numericRepOption = Num_MinMaxFeatures;
			categoricalRepOption = Cat_SetLeaves;
		}
		if(numericRepOption < Num_RangeAsNewFeature || numericRepOption > Num_PDF) {
			throw new Exception("Invalid numeric representation parameter!");
		}
		if(categoricalRepOption < Cat_GenAsNewFeature || categoricalRepOption > Cat_PDF) {
			throw new Exception("Invalid categorical representation parameter!");
		}
		if(kernelType < Ker_LINEAR || kernelType > Ker_PRECOMPUTED) {
			throw new Exception("Invalid kernel type!");
		}
	}
	
	private String isSet(String optionKey) {
		for(int i=0; i< args.length; i++) {
			if(args[i].compareTo(optionKey) == 0) {
				return args[i+1]; 
			}
		}
		return null;
	}
}
