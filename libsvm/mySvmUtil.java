package libsvm;

import java.io.*;
import java.util.*;

public class mySvmUtil {
	public static svm_problem readProblem(String input_file_name, svm_parameter param) throws Exception{
		BufferedReader fp = new BufferedReader(new FileReader(input_file_name));
		Vector<String> vy = new Vector<String>();
		Vector<svm_node[]> vx = new Vector<svm_node[]>();
		int max_index = 0;

		while(true)
		{
			String line = fp.readLine();
			if(line == null) break;

			StringTokenizer st = new StringTokenizer(line," \t\n\r\f:");

			vy.addElement(st.nextToken());
			int m = st.countTokens()/2;
			svm_node[] x = new svm_node[m];
			for(int j=0;j<m;j++)
			{
				x[j] = new svm_node();
				x[j].index = Integer.parseInt(st.nextToken());
				x[j].value = Double.parseDouble(st.nextToken());
			}
			if(m>0) max_index = Math.max(max_index, x[m-1].index);
			vx.addElement(x);
		}

		svm_problem prob = new svm_problem();
		prob.l = vy.size();
		prob.x = new svm_node[prob.l][];
		for(int i=0;i<prob.l;i++)
			prob.x[i] = (svm_node[])vx.elementAt(i);
		prob.y = new double[prob.l];
		for(int i=0;i<prob.l;i++)
			prob.y[i] = Double.parseDouble((String)vy.elementAt(i));

		if(param.gamma == 0)
			param.gamma = 1.0/max_index;

		if(param.kernel_type == svm_parameter.PRECOMPUTED)
			for(int i=0;i<prob.l;i++)
			{
				if (prob.x[i][0].index != 0)
				{
					System.err.print("Wrong kernel matrix: first column must be 0:sample_serial_number\n");
					System.exit(1);
				}
				if ((int)prob.x[i][0].value <= 0 || (int)prob.x[i][0].value > max_index)
				{
					System.err.print("Wrong input format: sample_serial_number out of range\n");
					System.exit(1);
				}
			}

		fp.close();
		return prob;
	}
	public static double predict(String inputFilename, svm_model model) throws IOException
	{
		FileReader myInputFile = new FileReader(inputFilename);
		BufferedReader input = new BufferedReader(myInputFile);
		int correct = 0;
		int total = 0;
		double error = 0;
		double sumv = 0, sumy = 0, sumvv = 0, sumyy = 0, sumvy = 0;
	
		String line = null;
		while((line = input.readLine()) != null)
		{
			StringTokenizer st = new StringTokenizer(line," \t\n\r\f:");	
			double target = Double.parseDouble(st.nextToken());
			int m = st.countTokens()/2;
			svm_node[] x = new svm_node[m];
			for(int j=0;j<m;j++)
			{
				x[j] = new svm_node();
				x[j].index = Integer.parseInt(st.nextToken());
				x[j].value = Double.parseDouble(st.nextToken());
			}

			double v = svm.svm_predict(model,x);
			if(svm.svm_predict(model,x) == target)
				++correct;
			error += (v-target)*(v-target);
			sumv += v;
			sumy += target;
			sumvv += v*v;
			sumyy += target*target;
			sumvy += v*target;
			++total;
		}
		input.close();
		myInputFile.close();
		
		//System.out.print("Accuracy = "
		//		+(double)correct/total*100+"% ("+correct+"/"+total+") (classification)\n");
		return (double)correct/total*100;
	}
}
