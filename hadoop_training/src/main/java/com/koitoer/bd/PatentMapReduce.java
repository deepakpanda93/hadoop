package com.koitoer.bd;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by mauricio.mena on 02/09/2016.
 */
public class PatentMapReduce extends Configured implements Tool{


    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "PatentJob");
        job.setJarByClass(PatentMapReduce.class);
        job.setJobName("Patent aggregation job");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapPatent.class);
        //job.setCombinerClass(ReducePatent.class);
        job.setReducerClass(ReducePatent.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper the list into the memory
     */
    public static class MapPatent extends Mapper<LongWritable,Text, Text, Text>{

        Text k =  new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line =  value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " ");

            while (tokenizer.hasMoreTokens()){
                String token1 = tokenizer.nextToken();
                k.set(token1);
                String token2 = tokenizer.nextToken();
                v.set(token2);
                context.write(k, v);
            }
        }
    }


    /**
     * Reducer that takes
     * (ID_doctor, ID_client1 - ID_client2)
     */
    public static class ReducePatent extends Reducer<Text, Text, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(Text text : values) {
                sum++;
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PatentMapReduce(), args);
        System.exit(exitCode);
    }
}
