package aboutyun;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class test_1 {
   public static class Map extends Mapper<Object, Text, Text, Text>{
	   private static Text line = new Text();
	   
	   public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
		   
		   line = value;
		   System.out.println("#########");
		   System.out.println(line);
		   System.out.println("__________");
		   /*在这里增加测这个小小的测试，发现每次输出来的内容都是一个文本中的一行*/
		   context.write(line, new Text(""));
		   
		   
	   }
   }
   
   
   public static class Reduce extends Reducer<Text, Text, Text, Text>{
	   
	   public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,InterruptedException{
		   
		   context.write(key, new Text(""));
	   }
   }
   
   
   
   public static void main(String[] args) throws Exception{
	   
	   Configuration conf = new Configuration();
	   
	   conf.set("mapred.job.tracker", "10.10.11.191:9001");
	   /*不加这一行，会出现路径寻找错误，系统自动找到的是当前工程下的目录，而不故事hdfs的目录*/
	   conf.set("fs.default.name","hdfs://master:9000");
	   String[] ioArgs = new String[]{"aboutyun_1_in","dedup_out"};
	   
	   String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
	   
	   if(otherArgs.length != 2) {
		   System.err.println("Usage:Data Dedupllication <in> <out>");
		   System.exit(2);
	   }
	   
	   Job job = new Job(conf, "Data Deduplication");
	   job.setJarByClass(test_1.class);
	   
	   job.setMapperClass(Map.class);
	   job.setCombinerClass(Reduce.class);
	   job.setReducerClass(Reduce.class);
	   
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(Text.class);

	   FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	   FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	   
	   System.exit(job.waitForCompletion(true) ? 0 : 1);
	   
   }
   
   
   
   
   

   
   
   
   
}
