

import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Average extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int r = ToolRunner.run(new Average(), args);
		System.exit(r);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Path ip = new Path(arg0[0]);
		Path op = new Path(arg0[1]);
		
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		FileInputFormat.setInputPaths(job, ip);
		FileOutputFormat.setOutputPath(job,op);
		
		job.setJobName("WordCount");
		job.setJarByClass(Average.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Mapr.class);
		job.setReducerClass(Reducr.class);
		
		int cp = job.waitForCompletion(true)? 0:1;
		return cp;
	}
	
	public static class Mapr extends Mapper<LongWritable, Text, IntWritable,Text > {
	
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,IntWritable,Text>.Context context) throws java.io.IOException ,InterruptedException {
			//String v = value.toString();
			context.write(new IntWritable(1), value);
		};
	}
	
	public static class Reducr extends Reducer<IntWritable,Text,IntWritable, Text >{
		protected void reduce(IntWritable arg0, java.lang.Iterable<Text> arg1, org.apache.hadoop.mapreduce.Reducer<IntWritable,Text,IntWritable, Text>.Context arg2) throws java.io.IOException ,InterruptedException {
			int sum=0;
			int count=0;
			for(Text i: arg1){
				String s = i.toString();
				double v = Double.parseDouble(s);
				sum+=v;
				count++;
			}
			double avg = (double)sum/count;
			arg2.write(arg0, new Text("average: "+avg));
		};
	}

	
	
	
	
	
	
	
	
	
	
	
}
