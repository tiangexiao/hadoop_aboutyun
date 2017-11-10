package phone_stream;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Data_MP {

        public static void main(String[] args) throws Exception {
                Configuration configuration = new Configuration();
                Job job = new Job(configuration);
                job.setJarByClass(Data_MP.class);
                job.setMapperClass(DataTotalMapper.class);
                job.setReducerClass(DataTotalReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DataWritable.class);
                job.setCombinerClass(DataTotalReducer.class);
                Path inputDir = new Path("hdfs://master:9000/user/hive/warehouse/hive.db/phone_stream/stream_data.txt");
                FileInputFormat.addInputPath(job, inputDir);
                Path outputDir = new Path("hdfs://master:9000/user/hive/output");
                FileOutputFormat.setOutputPath(job, outputDir);
                job.waitForCompletion(true);
        }

}



class DataTotalMapper extends Mapper<LongWritable, Text, Text, DataWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                        throws IOException, InterruptedException {
                String lineStr = value.toString();
                String[] strArr = lineStr.split("\t");
                String phpone = strArr[1];
                String upPackNum = strArr[6];
                String downPackNum = strArr[7];
                String upPayLoad = strArr[8];
                String downPayLoad = strArr[9];
                System.out.println("phpone is "+phpone);
                context.write(
                                new Text(phpone),
                                new DataWritable(Integer.parseInt(upPackNum), Integer
                                                .parseInt(downPackNum), Integer.parseInt(upPayLoad),
                                                Integer.parseInt(downPayLoad)));
        }

}

class DataTotalReducer extends Reducer<Text, DataWritable, Text, DataWritable> {

        @Override
        protected void reduce(Text k2, Iterable<DataWritable> v2, Context context)
                        throws IOException, InterruptedException {
                int upPackNumSum = 0;
                int downPackNumSum = 0;
                int upPayLoadSum = 0;
                int downPayLoadSum = 0;
                for (DataWritable dataWritable : v2) {
                        upPackNumSum += dataWritable.getUpPackNum();
                        downPackNumSum += dataWritable.getDownPackNum();
                        upPayLoadSum += dataWritable.getUpPayLoad();
                        downPayLoadSum += dataWritable.getDownPayLoad();
                }
                context.write(k2, new DataWritable(upPackNumSum, downPackNumSum, upPayLoadSum, downPayLoadSum));
        }

}
