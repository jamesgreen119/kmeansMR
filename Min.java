import java.io.*;
import java.util.*;
import java.lang.Math.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Min extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	
    	private Text combo = new Text();
    	
		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}

		public void map(LongWritable key, 
					Text value, 
					OutputCollector<Text, 
					Text> output, 
					Reporter reporter) throws IOException {
		    String line = value.toString();
			
		    String[] tokens = line.split("\t");

		    Text mykey = new Text("Min: ");
		    try{

		    	double[] val = new double[9];
		    	double[] log = new double[9];
		    	String logstr = "";
		    	for(int i=0;i<9;i++){
		    		val[i] = Double.parseDouble(tokens[i+19]);
		    		log[i] = Math.log(val[i]+1);
		    	}
		    	for(int i=0;i<8;i++){
		    		logstr = logstr+Double.toString(log[i])+"\t";
		    	}
		    	logstr = logstr+Double.toString(log[8]);

			    Text mystring=new Text(logstr);
		    	output.collect(mykey,mystring);
			} catch(IndexOutOfBoundsException ex){}catch(NumberFormatException nfe){}


		    
		}


		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			double[] min=new double[9];
    		for(int i=0;i<9;i++){
    			min[i]=2e9;
    		}

    		while (values.hasNext()){
    			String line = values.next().toString();
    			String[] tokens = line.split("\t");
    			double[] dubs = new double[9];
    			try{
	    			for(int i=0;i<9;i++){
	    				dubs[i] = Double.parseDouble(tokens[i]);
	    			}

	    			for (int i=0; i<9; i++){
	    				if(dubs[i]<min[i]){
	    					min[i] = dubs[i];
	    				}
	    			}
    			} catch(NumberFormatException nfe){}
    		}

    		String[] strs = new String[9];

    		String myvalue = "";

    		for(int i=0;i<9;i++){
    			myvalue = myvalue + Double.toString(min[i])+ ",";
    		}


    		output.collect(key, new Text(myvalue));
		}

		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
    }

    public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Min.class);
		conf.setJobName("Min");

//		conf.setNumReduceTasks(3);

//		conf.setBoolean("mapred.output.compress", true);
//		conf.setBoolean("mapred.compress.map.output", true);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Min(), args);
		System.exit(res);
    }
}
