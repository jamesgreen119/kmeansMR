import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Minmax extends Configured implements Tool {

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
			double[] max = {15.342359156206584,13.367279379344113,14.644447890729538,10.68637098694278,10.607306175840998,11.493751004336053,11.016317897776185,10.44392125462084,10.371400709015463};
			double[] min = {1.547562508716013,2.4849066497880004,2.4849066497880004,1.0043645609038482E-4,0.0,1.0043645609038482E-4,0.0,0.0,0.0};
    		double[] dubs = new double[9];
    		double[] logdubs = new double[9];
    		double[] out = new double[9];
		    
		    try{
		    	Text mykey = new Text("x");

    			for(int i=0;i<9;i++){
    				dubs[i] = Double.parseDouble(tokens[i+19]);
    				logdubs[i] = Math.log(dubs[i]+1);
	    			out[i] = (logdubs[i]-min[i])/(max[i]-min[i]);    				
    			}
	    		String myvalue = "";

  
	    // 		for(int i=0;i<19;i++){
					// myvalue = myvalue + tokens[i]+"\t";
	    // 		}


	    		for(int i=0;i<8;i++){
	    			myvalue = myvalue + Double.toString(out[i])+ "\t";
	    		}
	    		myvalue = myvalue+Double.toString(out[8]);


		    	output.collect(mykey,new Text(myvalue));
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


    		// while (values.hasNext()){
    		// 	String line = values.next().toString();
    		// 	String[] tokens = line.split(",");

    		// 	try{
	    	// 		for(int i=0;i<9;i++){
	    	// 			dubs[i] = Double.parseDouble(tokens[i]);
	    	// 		}

	    	// 		for (int i=0; i<9; i++){
	    	// 			out[i] = (dubs[i]-min[i])/(max[i]-min[i]);
	    	// 		}

    		// 	} catch(NumberFormatException nfe){}
    		// }


    		// String myvalue = "";

    		// for(int i=0;i<8;i++){
    		// 	myvalue = myvalue + Double.toString(out[i])+ ",";
    		// }
    		// myvalue = myvalue+Double.toString(out[8]);

    		// output.collect(key, new Text(myvalue));
		}

		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
    }

    public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Minmax.class);
		conf.setJobName("Minmax");

		conf.setNumReduceTasks(0);

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
		int res = ToolRunner.run(new Configuration(), new Minmax(), args);
		System.exit(res);
    }
}
