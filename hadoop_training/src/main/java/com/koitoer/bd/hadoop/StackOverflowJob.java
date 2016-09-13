package com.koitoer.bd.hadoop;

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

/**
 * Created by koitoer on 9/12/16.
 */
public class StackOverflowJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(StackMap.class);
        job.setReducerClass(StackReduce.class);

        //For the initial input of the mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //What will be the input type and what the output type
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1 ;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new StackOverflowJob(), args);
        System.exit(exitCode);
    }

    public static class StackMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String [] values = line.split("\t");
            if(key.get() != 0  && values.length > 6){
                String listOfTags = values[5];
                String [] tags = listOfTags.split(",");
                for(String tag : tags){
                    context.write(new Text(tag), new IntWritable(1));
                }
            }
        }
    }

    public static class StackReduce extends Reducer <Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable intWritable : values){
                sum++;
            }
            context.write(key,new IntWritable(sum));
        }
    }
}
