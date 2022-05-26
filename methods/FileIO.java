package methods;

import java.io.*;
import java.util.Random;

/**
 * This class contains a variety of file input/output functions
 * that are useful in building experiment data.
 */
public class FileIO {
	
	/**
	 * Reads the first numLines lines of an input file into outputFile
	 *
	 * @param input filename
	 * @param output filename
	 * @param number of lines to be read to output file
	 * @return 
	 */
	public static void Grip(String inputFile, String outputFile, int numLines) throws Exception {
		if(numLines > CountNonEmptyLines(inputFile)) {
			return;
		}
		
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
				
		FileWriter myOutputFile = new FileWriter(outputFile); //training
		BufferedWriter output = new BufferedWriter(myOutputFile);
		
		String line = null;
		String newline = System.getProperty("line.separator");
		int count = 0;
		while(count < numLines && (line = input.readLine()) != null && line.trim().compareTo("")!= 0) {
			output.write(line+newline);
			count++;
		}
		
		input.close();
		myInputFile.close();
		output.close();
		myOutputFile.close();
		Thread.sleep(100);
	}
	
	/**
	 * Outputs inputFile's projection on specified attribute indices to the outputFile
	 *
	 * @param input filename
	 * @param output filename
	 * @param int[] list of indices to be projected
	 * @return
	 */
	public static void AttributeSelection(String inputFile, String outputFile, int[] indices) throws Exception {
		if(indices == null || indices.length == 0) {
			return;
		}
		
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
				
		FileWriter myOutputFile = new FileWriter(outputFile); //training
		BufferedWriter output = new BufferedWriter(myOutputFile);
		
		//count the number of records first
		int count = CountNonEmptyLines(inputFile);
		
		String newline = System.getProperty("line.separator");
		String[] inLine;
		String outLine;
		for(int i = 0; i < count; i++) {
			inLine = input.readLine().split(",");
			outLine = inLine[indices[0]];
			for(int j = 1; j < indices.length; j++) {
				outLine += ","+inLine[indices[j]];
			}
			outLine+= newline;
			output.write(outLine);
			//System.out.print(outLine);
		}
		
		input.close();
		myInputFile.close();
		output.close();
		myOutputFile.close();
		Thread.sleep(100);
	}
	
	/**
	 * Removes the specified attributes from input and outputs the result
	 * to the output file
	 *
	 * @param input filename
	 * @param output filename
	 * @param int[] list of indices to be removed
	 * @return
	 */
	public static void AttributeRemoval(String inputFile, String outputFile, int[] indices) throws Exception {
		if(indices == null || indices.length == 0) {
			return;
		}
		
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
				
		FileWriter myOutputFile = new FileWriter(outputFile); //training
		BufferedWriter output = new BufferedWriter(myOutputFile);
		
		//count the number of records first
		int count = CountNonEmptyLines(inputFile);
		
		String newline = System.getProperty("line.separator");
		String[] inLine;
		String outLine;
		for(int i = 0; i < count; i++) {
			inLine = input.readLine().split(",");
			for(int j = 0; j < indices.length; j++) {
				inLine[indices[j]] = null;
			}
			outLine = "";
			for(int j = 0; j < inLine.length; j++) {
				if(inLine[j] != null) {
					outLine += ","+inLine[j];
				}
			}
			outLine = outLine.substring(1);
			outLine+= newline;
			output.write(outLine);
			//System.out.print(outLine);
		}
		
		input.close();
		myInputFile.close();
		output.close();
		myOutputFile.close();
		Thread.sleep(100);
	}

	/**
	 * Reads the first line of a header file and parses it as java.Integer
	 *
	 * @param header file for ARFF databases
	 * @return number of attributes
	 */
	public static int GetNumAttributes(String headerFile) throws Exception {
		FileReader inputFile = new FileReader(headerFile);
		BufferedReader input = new BufferedReader(inputFile);
		String line = input.readLine(); //number of attributes
		int numAtt = Integer.parseInt(line.trim());
		input.close();
		inputFile.close();
		Thread.sleep(100);
		return numAtt;
	}
	
	/**
	 * Reads the entire header file into a String, except for the first line
	 * that contains the number of attributes described in the header.
	 * Example header file: 
	 * "3
	 * \@RELATION relName
	 * \@Attribute numeric NUMERIC
	 * \@Attribute categorical {cat1,cat2}
	 * \@Attribute alpha STRING
	 * \@DATA"
	 *
	 * @param header file for ARFF databases 
	 * @return header information String in ARFF format
	 */
	public static String GetARFFHeader(String headerFile) throws Exception {
		String newline = System.getProperty("line.separator");
		FileReader inputFile = new FileReader(headerFile);
		BufferedReader input = new BufferedReader(inputFile);
		
		String line = input.readLine(); //number of attributes
		int numAtt = Integer.parseInt(line.trim());
		String result = input.readLine()+newline; //@RELATION relName
		for(int i = 0; i < numAtt; i++){
			line = input.readLine();
			result+=line+newline;
		}
		result+=input.readLine(); //@DATA
		
		input.close();
		inputFile.close();
		Thread.sleep(100);
		return result;
	}
	
	/**
	 * Reads the entire header file into a String, except for the first line
	 * that contains the number of attributes described in the header.
	 * Example header file: 
	 * "3
	 * \@RELATION relName
	 * \@Attribute numeric NUMERIC
	 * \@Attribute categorical {cat1,cat2}
	 * \@Attribute alpha STRING
	 * \@DATA"
	 * <p/>
	 * Among these attributes, the indices specified in exclude will be removed
	 * and the indices specified in string will be converted attributes of type
	 * String.
	 * @param header file for ARFF databases 
	 * @param exclude the attributes at the specified indices will be omitted
	 * @param string the attributes at the specified indices will be converted to string type
	 * @return header information String in ARFF format
	 */
	public static String GetARFFHeader(String headerFile, int[] exclude, int[] string) throws Exception {
		String newline = System.getProperty("line.separator");
		FileReader inputFile = new FileReader(headerFile);
		BufferedReader input = new BufferedReader(inputFile);
		
		int indexLine = 0;
		String line;
		String result = input.readLine()+newline; //@RELATION relName
		while((line = input.readLine()) != null && line.length() >=0) {
			boolean skip = false;
			for(int j = 0; exclude != null && j < exclude.length; j++) {
				if(exclude[j] == indexLine) {
					skip = true;
				}
			}
			if(skip) {
				indexLine++;
				continue; //continue without any output
			}
			for(int j = 0; string != null && j < string.length; j++) {
				if(string[j] == indexLine) {
					//compute index for numeric attributes
					int index = line.toLowerCase().lastIndexOf("numeric");
					if(index == -1) { //if not numeric, re-compute for categorical attributes
						index = line.lastIndexOf("{");
					}
					if(index != -1) {
						line = line.substring(0, index) + "STRING";
					} else {
						if(!line.toLowerCase().contains("string")) {
							throw new Exception("Cannot resolve attribute type!!!");
						}
					}
				}
			}
			result+=line+newline;
			indexLine++;
		}
		
		input.close();
		inputFile.close();
		Thread.sleep(100);
		return result;
	}
	
	/**
	 * Renames an input file to the given name, renFile. CAUTION: if there exists a
	 * file with the name "renFile", it will be deleted!!! 
	 *
	 * @param input filename, to be renames
	 * @param new filename
	 * @return
	 */
	public static void RenameFile(String inputFilename, String rename) throws Exception {
		File f = new File(rename);
		if(f.exists())
			f.delete();
		File f2 = new File(inputFilename);
		f2.renameTo(f);
		Thread.sleep(100);
	}
	
	/**
	 * Adds the second String parameter before the first line of a file
	 *
	 * @param file to be appended
	 * @param String to be appended
	 * @return
	 */
	public static void AddStringToFile(String inputFilename, String value) throws Exception{
		String newline = System.getProperty("line.separator");
		FileReader inputFile = new FileReader(inputFilename);
		BufferedReader input = new BufferedReader(inputFile);
		
		FileWriter outputFile = new FileWriter("temp");
		BufferedWriter output = new BufferedWriter(outputFile);
		
		String line;
		output.write(value + newline);
		while((line = input.readLine()) != null) {
			output.write(line+newline);
		}
		input.close();
		inputFile.close();
		output.close();
		outputFile.close();
		Thread.sleep(100);
		
		RenameFile("temp", inputFilename);
	}
	
	/**
	 * Merges file1 and file2 into outFile. Any line starting with "|" is omitted.
	 *
	 * @param file1 (will be the first part of outFile)
	 * @param file2
	 * @param outFile: output filename
	 * @return
	 */
	public static void MergeTwoInputs(String inputFile1, String inputFile2, String outputFile) throws Exception{
		FileReader myInputFile1 = new FileReader(inputFile1);
		BufferedReader input1 = new BufferedReader(myInputFile1);
		
		FileReader myInputFile2 = new FileReader(inputFile2);
		BufferedReader input2 = new BufferedReader(myInputFile2);
		
		FileWriter myOutputFile = new FileWriter(outputFile);
		BufferedWriter output = new BufferedWriter(myOutputFile);
		
		String line = null;
		String newline = System.getProperty("line.separator");
		while((line = input1.readLine()) != null && line.trim().compareTo("")!= 0) {
			if(line.charAt(0) != '|') { //for any comment lines (see first line of adult.test
				output.write(line+newline);
			}
		}
		while((line = input2.readLine()) != null && line.trim().compareTo("")!= 0) {
			if(line.charAt(0) != '|') { //for any comment lines (see first line of adult.test
				output.write(line+newline);
			}
		}
		
		input1.close();
		myInputFile1.close();
		input2.close();
		myInputFile2.close();
		output.close();
		myOutputFile.close();
		Thread.sleep(100);
	}
	
	/**
	 * Removes any records with '?' in inFile and outputs results to outFile.
	 * In the meanwhile, whitespace between attributes are removed in the format 
	 * (a_1,a_2,...,a_n) for n attributes.
	 *
	 * @param inFile: input filename
	 * @param outFile: output filename
	 * @return
	 */
	public static void RemoveRecWithMissingValues(String inputFile,	String outputFile) throws Exception{
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
		
		FileWriter myOutputFile = new FileWriter(outputFile);
		BufferedWriter output = new BufferedWriter(myOutputFile);
		
		int countOriginal = 0;
		int countNew = 0;
		String line = null;
		String newline = System.getProperty("line.separator");
		while((line = input.readLine()) != null) {
			countOriginal++;
			if(line.indexOf("?") == -1) {
				line = line.replaceAll(",\\s", ",");
				if(countNew == 0) {
					output.write(line);
				}
				else {
					output.write(newline+line);
				}
				countNew++;
			}
		}
		
		//System.out.println("Original file contains "+countOriginal+" tuples");
		//System.out.println("Output file contains "+countNew+" tuples");
		
		input.close();
		myInputFile.close();
		output.close();
		myOutputFile.close();
		Thread.sleep(100);
	}
	
	/**
	 * Shuffles records in inFile and outputs the result to outFile. Seed for random number
	 * number generator is fixed to 3345 (easily changable, but restricted for the purpose of
	 * repeatable experiments)
	 *
	 * @param inFile: input filename
	 * @param outFile: output filename
	 * @return
	 */
	public static void ShuffleRecords(String inputFile, String outputFile) throws Exception{		
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
				
		FileWriter myOutputFile = new FileWriter(outputFile); //training
		BufferedWriter output = new BufferedWriter(myOutputFile);
		
		//count the number of records first
		int count = CountNonEmptyLines(inputFile);
		
		String newline = System.getProperty("line.separator");
		String[] all = new String[count];
		for(int i = 0; i < count; i++) {
			all[i] = input.readLine();
		}
		Random rand = new Random(3345);
		for (int i = 0; i < count; i++) {
            int j = i + (int) (rand.nextDouble() * (count-i));
            String temp = all[i];
            all[i] = all[j];
            all[j] = temp;
        }
		for(int i = 0; i < count; i++) {
			output.write(all[i]+newline);
		}		
		input.close();
		myInputFile.close();
		output.close();
		myOutputFile.close();
		Thread.sleep(100);
	}
	
	/**
	 * Divides an input file into equi-length output files with given filenames.
	 * Primary purpose is multiple data holder experiments (no tuples discarded, last
	 * output file contains any remainder tuples)
	 *
	 * @param input filename
	 * @param array of output filenames
	 * @return
	 */
	public static void SplitIntoPartitions(String inputFile, String[] outputFiles) throws Exception{
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
		
		FileWriter[] myOutputFiles = new FileWriter[outputFiles.length];
		BufferedWriter[] outputs = new BufferedWriter[outputFiles.length];
		for(int i = 0; i < outputFiles.length; i++) {
			myOutputFiles[i] = new FileWriter(outputFiles[i]); //test
			outputs[i] = new BufferedWriter(myOutputFiles[i]);
		}
		
		//count the number of records first
		int count = CountNonEmptyLines(inputFile);
		int foldCount = count / outputFiles.length; //integer division, last computation should be through subtraction in order not to miss any records
				
		String newline = System.getProperty("line.separator");
		String line;
		for(int i = 0; i < outputFiles.length-1; i++) {
			count = 0; //first "initScanForTraining" lines into training file
			while(count < foldCount) {
				outputs[i].write(input.readLine()+newline);
				count++;
			}
		}
		while((line = input.readLine()) != null && line.trim().compareTo("")!= 0) {
			outputs[outputFiles.length-1].write(line+newline);
		}
		
		input.close();
		myInputFile.close();
		for(int i = 0; i < outputFiles.length; i++) {
			outputs[i].close();
			myOutputFiles[i].close();
		}
		Thread.sleep(100);
	}
	
	/**
	 * Partitions the input file into training and test datasets according to the 
	 * number of folds and current fold. Let the input file contain 101 records and
	 * the experiment is 4-fold.
	 * When currFold = 1, test dataset has first 25 tuples, training dataset has
	 * remaining 76 tuples.
	 * When currFold = 2, training dataset has first 25 tuples and all tuples after  
	 * the 50th tuple (76 in total), test dataset has tuples (25-50].
	 *
	 * @param input filename
	 * @param training data output filename
	 * @param test data output filename
	 * @param number of folds (>0)
	 * @param current fold: [1-numFolds]
	 * @return
	 */
	public static void PrepareCrossValidationData(String inputFile, String outputFile1,
			String outputFile2, int numFolds, int currFold) throws Exception{
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
		
		FileWriter myOutputFile2 = new FileWriter(outputFile2); //test
		BufferedWriter output2 = new BufferedWriter(myOutputFile2);
		
		FileWriter myOutputFile1 = new FileWriter(outputFile1); //training
		BufferedWriter output1 = new BufferedWriter(myOutputFile1);
		
		//count the number of records first
		int count = CountNonEmptyLines(inputFile);
		int foldCount = count / numFolds; //integer division, last computation should be through subtraction in order not to miss any records
		int initScanForTraining = foldCount * (currFold-1);
		
		String newline = System.getProperty("line.separator");
		String line;
		count = 0; //first "initScanForTraining" lines into training file
		while(count < initScanForTraining && (line = input.readLine()) != null && line.trim().compareTo("")!= 0) {
			output1.write(line+newline);
			count++;
		}
		count = 0; //then "foldCount" lines into test file 
		while(count < foldCount && (line = input.readLine()) != null && line.trim().compareTo("")!= 0) {
			output2.write(line+newline);
			count++;
		} //rest into training file (do not miss out any records!)
		while((line = input.readLine()) != null && line.trim().compareTo("")!= 0) {
			output1.write(line+newline);
		}
		
		input.close();
		myInputFile.close();
		output1.close();
		myOutputFile1.close();
		output2.close();
		myOutputFile2.close();
		Thread.sleep(100);
	}
	
	/**
	 * Counts and returns the number of non-empty lines
	 *
	 * @param input filename
	 * @return number of non empty lines in the input file
	 */
	public static int CountNonEmptyLines(String inputFile) throws Exception{
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
		
		int count = 0;
		String line = null;
		while((line = input.readLine()) != null && line.trim().compareTo("")!= 0) {
			count++;
		}
		
		input.close();
		myInputFile.close();
		Thread.sleep(100);
		return count;
	}
	
	/**
	 * Replaces all space characters within attribute values with the underscore character
	 *
	 * @param inputFile filename
	 * @param outputFile filename
	 */
	public static void ReplaceWSWithUnderScore(String inputFile, String outputFile) throws Exception{
		FileReader myInputFile = new FileReader(inputFile);
		BufferedReader input = new BufferedReader(myInputFile);
		
		FileWriter myOutputFile = new FileWriter(outputFile);
		BufferedWriter output = new BufferedWriter(myOutputFile);
		
		String line = null;
		String newline = System.getProperty("line.separator");
		while((line = input.readLine()) != null && line.trim().compareTo("")!= 0) {
			String[] vals = line.split(",");
			String lineOut = vals[0].trim().replaceAll(" ", "_");
			for(int i = 1; i < vals.length; i++) {
				vals[i] = vals[i].trim().replaceAll(" ", "_");
				lineOut += "," + vals[i];
			}
			output.write(lineOut + newline);
		}
		
		output.close();
		input.close();
		myInputFile.close();
		Thread.sleep(100);
	}
	
	/**
	 * Removes whitespace from the dataset (in between attribute values),
	 * Removes any record with unknown values ("?")
	 * Outputs the number of records in the final version
	 * If specified as parameter, shuffles the records and grips the first
	 * numLines records.
	 * 
	 * @param inputFilename
	 * @param outputFilename
	 * @param numLines (int) --optional
	 * @return
	 */
	public static void main(String[] args) throws Exception{
		if(args.length<2) {
			System.out.println("Insufficient arguments");
			System.exit(-1);
		}
		boolean grip = false;
		int numLines = -1;
		if(args.length==3) {
			try {
				numLines = Integer.parseInt(args[2]);
			} catch (Exception e) {
				System.out.println("3rd argument expected to be integer");
				System.exit(-1);
			}
			grip = true;
		} else {
			numLines = CountNonEmptyLines(args[0]);
		}
		
		String input = args[0];
		String output = args[1];
		int numRec = CountNonEmptyLines(input);
		if(numRec < numLines) {
			System.out.println("Input has "+numRec+" lines, gripping "+numLines+" records is not possible");
			numLines = numRec;
			grip = false;
		}
		
		RemoveRecWithMissingValues(input, output);
		RenameFile(output, input);
		ShuffleRecords(input, output);
		RenameFile(output,input);
		if(grip) {
			Grip(input, output, numLines);
			RenameFile(output, input);
		}
		
		//dataset specific tasks
		if(input.indexOf("abalone")!=-1) { //abalone
			//apply binning on class attribute ([1-9], [10-29])
			FileReader myInputFile = new FileReader(input);
			BufferedReader inputt = new BufferedReader(myInputFile);
			
			FileWriter myOutputFile = new FileWriter(output);
			BufferedWriter outputt = new BufferedWriter(myOutputFile);
			
			String newline = System.getProperty("line.separator");
			String line = null;
			for(int i = 0; i < numLines; i++) {
				line = inputt.readLine();
				String cols[] = line.split(",");
				int classVal = Integer.parseInt(cols[cols.length-1]);
				if(classVal < 10) {
					cols[cols.length-1] = "1-9";
				}
				else {
					cols[cols.length-1] = "10-29";
				}
				line = cols[0];
				for(int j = 1; j < cols.length; j++) {
					line+=","+cols[j];
				}
				outputt.write(line+newline);
			}
			inputt.close();
			myInputFile.close();
			outputt.close();
			myOutputFile.close();
		}
		else if(input.indexOf("mushroom")!=-1) { //mushroom
			//move class attribute from column 0 to column 22
			FileReader myInputFile = new FileReader(input);
			BufferedReader inputt = new BufferedReader(myInputFile);
			
			FileWriter myOutputFile = new FileWriter(output);
			BufferedWriter outputt = new BufferedWriter(myOutputFile);
			
			String newline = System.getProperty("line.separator");
			String line = null;
			for(int i = 0; i < numLines; i++) {
				line = inputt.readLine();
				String cols[] = line.split(",");
				line = cols[1];
				for(int j = 2; j < cols.length; j++) {
					line+=","+cols[j];
				}
				line+= ","+cols[0];
				outputt.write(line+newline);
			}
			inputt.close();
			myInputFile.close();
			outputt.close();
			myOutputFile.close();
		}
		else if(input.indexOf("german")!=-1) { //german
			//collect statistics for attributes 2, 5, 11, 13
			int[] indices = {1, 4, 10, 12};
			double[] max = new double[indices.length];
			double[] min = new double[indices.length];
			for(int i = 0; i < max.length; i++) {
				max[i] = Double.MIN_VALUE;
				min[i] = Double.MAX_VALUE;
			}
			FileReader myInputFile = new FileReader(input);
			BufferedReader inputt = new BufferedReader(myInputFile);
			
			String line = null;
			for(int i = 0; i < numLines; i++) {
				line = inputt.readLine();
				String cols[] = line.split(",");
				for(int j = 0; j < indices.length; j++) {
					double currVal = Double.parseDouble(cols[indices[j]]);
					if(currVal > max[j]) {
						max[j] = currVal;
					}
					if(currVal < min[j]) {
						min[j] = currVal;
					}
				}
			}
			for(int i = 0; i < indices.length; i++) {
				System.out.println("Att. "+(indices[i]+1)+" minVal = "+min[i]+", maxVal = "+max[i]);
			}
			inputt.close();
			myInputFile.close();

			RenameFile(input, output);
		}
	}
}
