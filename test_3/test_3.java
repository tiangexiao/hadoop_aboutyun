package aboutyun;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jboss.netty.handler.ipfilter.OneIpFilterHandler;



public class test_3 {
    
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
			String line = value.toString();

			StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
			while(tokenizerArticle.hasMoreElements()) {
				System.out.println("++++");
				String x = tokenizerArticle.nextToken();
				System.out.println(x);
				System.out.println("----");
			}
			while(tokenizerArticle.hasMoreElements()) {
				StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
				
				String strName = tokenizerLine.nextToken();
				String strScore = tokenizerLine.nextToken();
				
				Text name = new Text(strName);
				int scoreInt = Integer.parseInt(strScore);
				context.write(name, new IntWritable(scoreInt));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException,InterruptedException{
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()) {
				sum += iterator.next().get();
				count++;
			}
			int average = (int) sum /count;
			context.write(key, new IntWritable(average));
			
		}
	}
	
	public static class countmap extends Mapper<Object,Text,Text, IntWritable>{
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void mapper(Object key,Text value, Context context) throws IOException,InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while(tokenizer.hasMoreTokens()) {
				String tmp = tokenizer.nextToken();
				context.write(new Text(tmp), one);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		   
		   Configuration conf = new Configuration();
		   
		   conf.set("mapred.job.tracker", "10.10.11.191:9001");
		   conf.set("fs.default.name","hdfs://master:9000");
		   String[] ioArgs = new String[]{"aboutyun_3_in","dedup_out"};
		   
		   String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		   
		   if(otherArgs.length != 2) {
			   System.err.println("Usage:Data Dedupllication <in> <out>");
			   System.exit(2);
		   }
		   
		   Job job = new Job(conf, "score average");
		   job.setJarByClass(test_3.class);
		   
		   job.setMapperClass(Map.class);
		   job.setCombinerClass(Reduce.class);
		   job.setReducerClass(Reduce.class);
		   
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(IntWritable.class);
           
		   job.setInputFormatClass(TextInputFormat.class);
		   job.setOutputFormatClass(TextOutputFormat.class);
		   
		   FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		   FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		   
		   System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
