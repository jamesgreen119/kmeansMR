import java.io.*;
import java.util.*;
import java.lang.Math.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;

public class kmeans extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
    	
//    	private Text combo = new Text();
    	private int[] vars = {1,2,4,6,8};
    	private int varCount = vars.length;

    	int numClusters;
    	double[][] centroids;
    	
		public void configure(JobConf job) {
			Path[] localFiles=new Path[0];
			try{
				numClusters = Integer.parseInt(job.get("numClusters")); //by default command line arguments are Strings
				double[][] tempCentroids = new double[numClusters][varCount];
				localFiles = DistributedCache.getLocalCacheFiles(job);
				BufferedReader fileIn = new BufferedReader(new FileReader(localFiles[0].toString()));
				String line;
				int i=0;
				while ((line=fileIn.readLine()) != null){
					String tokens[] = line.split("\t");
					for(int j=0;j<varCount;j++){
						tempCentroids[i][j] = Double.parseDouble(tokens[j+1]);
					}
					i++;
				} //while
				fileIn.close();
				centroids = tempCentroids;
			}//try
			catch (IOException ioe) {
		 		System.err.println("Caught exception while getting cached file: " + StringUtils.stringifyException(ioe));
			}//catch
		}//configure

		protected void setup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
		}

		public void map(LongWritable key, 
					Text value, 
					OutputCollector<IntWritable, 
					Text> output, 
					Reporter reporter) throws IOException {
		    String line = value.toString();
			
		    String[] tokens = line.split("\t");

			double min = 1000.0;
			int bestClust = 0;
			double[] data = new double[varCount];

			for(int i=0;i<varCount;i++){
				data[i] = Double.parseDouble(tokens[vars[i]]);
			}

			for(int i=0; i<numClusters;i++){
				double euDist = 0.0;
				for(int j=0;j<varCount;j++){
					euDist+=Math.pow(centroids[i][j]-data[j],2);
				}
				if(euDist<min){
					min=euDist;
					bestClust = i;
				}
			}

			output.collect(new IntWritable(bestClust),value);
		    


		    
		}


		protected void cleanup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
		}
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    	private int[] vars = {1,2,4,6,8};
    	private int varCount = vars.length;

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
		}
		
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

			//initialize sum[] and count
			double[] sum = new double[varCount];
			for(int i=0;i<varCount;i++){
				sum[i]=0.0;
			}
			double count = 0.0;

			//while has.next..., parse value into value[], add value[]  to sum[] and increment count
			while (values.hasNext()){
				String line = values.next().toString();
    			String[] tokens = line.split("\t");

    			double[] myvals = new double[varCount];
    			for(int i=0;i<varCount;i++){
    				myvals[i] = Double.parseDouble(tokens[vars[i]]);
    				sum[i] += myvals[i];
    			}
    			count=count+1;


			}

			double[] mean = new double[varCount];

			for(int i=0;i<varCount;i++){
				mean[i] = sum[i]/count;
			}

			String mymeans = "";
			for(int i=0;i<varCount-1;i++){
				mymeans = mymeans+Double.toString(mean[i])+"\t";
			}
			mymeans = mymeans+Double.toString(mean[varCount-1]);

			IntWritable mykey = new IntWritable((int) count);

			//output key, value=means
			output.collect(mykey,new Text(mymeans));

		}

		protected void cleanup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
		}
    }

    public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), kmeans.class);
		conf.setJobName("kmeans_jg");

//		conf.setNumReduceTasks(0);

//		conf.setBoolean("mapred.output.compress", true);
//		conf.setBoolean("mapred.compress.map.output", true);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.set("numClusters",args[2]);
		DistributedCache.addCacheFile(new Path("/user/jgreen/hw3/centroids/centroids.txt").toUri(),conf);

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new kmeans(), args);
		System.exit(res);
    }
}
