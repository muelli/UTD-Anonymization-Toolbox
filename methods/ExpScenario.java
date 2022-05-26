package methods;

import java.io.File;
import java.util.LinkedList;

import libsvm.mySvmUtil;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_parameter;
import libsvm.svm_problem;
import anonymizer.Anonymizer;
import anonymizer.Configuration;
import featRep.FeatureRepresentation;

public class ExpScenario {
	public static void runExp(Params p, String[] args) throws Exception{
		//check if outputFormat is set
		if(getOption("-outputFormat", args) >0 ) {
			System.out.println("WARNING: Parameter set by -outputFormat will be ignored!!!");
		}
		
		//test scenario params
		String testScenario = "11"; //00, 01, 10, 11
		String descriptor = null; //names file
		int crossVal = 2; //some integer value
		
		int index = -1;
		if( (index = getOption("-scenario", args)) >= 0) {
			if(args[index].compareTo("00") != 0 && args[index].compareTo("10") != 0 
				&& args[index].compareTo("01") != 0 && args[index].compareTo("11") != 0) {
				System.out.println("WARNING: Invalid test scenario, will assume 11");
			} else {
				testScenario = args[index];
			}
		}
		if( (index = getOption("-arff", args)) >= 0) { //not optional
			descriptor = args[index];
			File f = new File(descriptor);
			if(!f.exists()) {
				throw new Exception("Description file " + descriptor + " not found!!!");
			}
		} else {
			throw new Exception("Description file not specified (use option -arff)!!!");
		}
		if( (index = getOption("-crossval", args)) >= 0) {
			crossVal = (int) Double.parseDouble(args[index]);
		}
		if(crossVal < 2) {
			System.out.println("WARNING: At least 2 folds required for " +
					"cross-validation. Setting the value to 2");
			crossVal = 2;
		}
		
		Configuration conf = new Configuration(args);
		//validate some of the input
		if(p.usePDFExpected) {
			if(conf.outputFormat != Configuration.OUTPUT_FORMAT_GENVALSDIST) {
				System.out.println("WARNING: Resetting output format to GenValsDist!!!");
				conf.outputFormat = Configuration.OUTPUT_FORMAT_GENVALSDIST;
			}
		} else {
			if(conf.outputFormat == Configuration.OUTPUT_FORMAT_GENVALSDIST) {
				System.out.println("WARNING: Resetting output format to GenVals!!!");
				conf.outputFormat = Configuration.OUTPUT_FORMAT_GENVALS;
			}
		}
		if(conf.outputFormat == Configuration.OUTPUT_FORMAT_ANATOMY) {
			String message = "Anatomy output format is not supported (do not use program " +
				"arguments to set outputFormat)!!!";
			throw new Exception(message);
		}
		/* Mondrian can only be used in one of the following settings:
		 * (1) -pdf 
		 * (2) -uni
		 * (3) -numRep = 2 AND -catRep = 3
		 * (4) -numRep = 3 AND -catRep = 3
		*/
		if(conf.anonMethod == Configuration.METHOD_MONDRIAN) {
			if(p.numericRepOption == Params.Num_RangeAsNewFeature
					|| p.categoricalRepOption == Params.Cat_GenAsNewFeature
					|| p.categoricalRepOption == Params.Cat_PathToRoot) {
				String message = "Anonymization methods that generate their own " +
					"VGHs (e.g., Mondrian) cannot be used in this setting!!!";
				throw new Exception(message);
			}
		}
		
		//identifier attributes will be handled cautiously
		int[] idFields = null;
		if(conf.idAttributeIndices.size() > 0) {
			idFields = new int[conf.idAttributeIndices.size()];
			for(int i = 0; i < idFields.length; i++) {
				idFields[i] = conf.idAttributeIndices.get(i);
			}
		}
		
		//remove tuples with missing values
		FileIO.RemoveRecWithMissingValues(conf.inputFilename, "temp1.tmp");
		//remove identifier attributes manually
		LinkedList<Integer> idIndices = conf.idAttributeIndices;
		if(idFields != null) {
			//we should first re-adjust quasi-identifier indices
			for(int i = 0; i < conf.qidAtts.length; i++) {
				for(int j = 0; j < idFields.length; j++) {
					if(idFields[j] < conf.qidAtts[i].index) {
						conf.qidAtts[i].index--;
					}
				}
			}
			//then re-adjust sensitive indices
			for(int i = 0; i < conf.sensitiveAtts.length; i++) {
				for(int j = 0; j < idFields.length; j++) {
					if(idFields[j] < conf.sensitiveAtts[i].index) {
						conf.sensitiveAtts[i].index--;
					}
				}
			}
			//now remove the id attributes from the file
			FileIO.AttributeRemoval("temp1.tmp", "temp2.tmp", idFields);
			FileIO.RenameFile("temp2.tmp", "temp1.tmp");
			conf.idAttributeIndices = new LinkedList<Integer>();
		}
		conf.inputFilename = "temp1.tmp";
		
		//anonymize
		Anonymizer.anonymizeDataset(conf);		
		
		FeatureRepresentation fr;
		fr = new FeatureRepresentation(descriptor, conf.qidAtts, idIndices, p.numericRepOption,
				p.categoricalRepOption, p.usePDFExpected, p.useUniformExpected);
		fr.initialScan(conf.outputFilename);
		fr.assignFeatureIndices();
		fr.featurize(conf.outputFilename, "featAnon.rawdata");
		fr.featurize(conf.inputFilename, "featOrig.rawdata");
		
		for(int i = 1; i <= crossVal; i++){
			FileIO.PrepareCrossValidationData
					("featOrig.rawdata", "orig"+i+".data", "orig"+i+".test", crossVal, i);
			FileIO.PrepareCrossValidationData
					("featAnon.rawdata", "anon"+i+".data", "anon"+i+".test", crossVal, i);
		}
		
		String data, test;
		double avg_acc = 0;			
		for(int fold = 1; fold <= crossVal; fold++) {
			if(testScenario.compareTo("00")==0){
				data = test = "orig";
			}
			else if(testScenario.compareTo("11")==0){
				data = test = "anon";
			}
			else if(testScenario.compareTo("01")==0) {
				data = "orig";
				test = "anon";
			}
			else {
				data = "anon";
				test = "orig";
			}
			svm_parameter param = new svm_parameter();
			param.svm_type = svm_parameter.C_SVC;
			param.kernel_type = svm_parameter.RBF;
			param.degree = 3;
			param.gamma = 0;	// 1/k
			param.coef0 = 0;
			param.nu = 0.5;
			param.cache_size = 100;
			param.C = 1;
			param.eps = 1e-3;
			param.p = 0.1;
			param.shrinking = 1;
			param.probability = 0;
			param.nr_weight = 0;
			param.weight_label = new int[0];
			param.weight = new double[0];
			param.fr = fr;
			param.useExpectedValues = (p.useUniformExpected || p.usePDFExpected);
			param.kernel_type = p.kernelType; 
			svm_problem prob = mySvmUtil.readProblem(data+fold+".data", param);
			svm_model model = svm.svm_train(prob, param);
			double acc = mySvmUtil.predict(test+fold+".test", model);
			avg_acc += acc;
		}
		System.out.println();
		System.out.println("Average accuracy: " + avg_acc / crossVal);
		
		//delete files
		for(int i = 1; i <= crossVal; i++) {
			deleteFile("orig"+i+".data");
			deleteFile("orig"+i+".test");
			deleteFile("anon"+i+".data");
			deleteFile("anon"+i+".test");
		}
		deleteFile("featAnon.rawdata");
		deleteFile("featOrig.rawdata");
		deleteFile("temp1.tmp");
		
	}
	
	/**
	 * Deletes the file with the specified name, if exists
	 * @param fname filanem
	 */
	private static void deleteFile(String fname) {
		File f = new File(fname);
		if(f.exists()) {
			f.delete();
		}
	}
	
	/**
	 * Gets the specified option
	 * @param option Option value to be searched
	 * @param args List of arguments
	 * @return Option as specified, or null if not set
	 */
	private static int getOption(String option, String[] args) {
		for(int i = 0; i < args.length; i++) {
			if(args[i].compareToIgnoreCase(option) == 0 && i+1 < args.length) {
				return i+1;
			}
		}
		return -1;
	}
	
	public static void main(String[] args) {
//		String params = "-scenario 01 -outputFormat genValsDist " +
//				"-arff ../header.txt -method mondrian " +
//				"-config ../config.xml -input ../census-income_1K.data";
//		args = params.split(" ");
		try {
			Params p = new Params(args);
			ExpScenario.runExp(p, args);
		} catch(Exception e) {e.printStackTrace();}
	}
}