package com.koitoer.bd.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * hadoop jar hadoop-training.jar com.koitoer.sl.BasicJoinExample /user/cloudera/datasets/custs /user/cloudera/datasets/txns /user/cloudera/out/join
 * Join in the reducer example use the custs and txns data sets
 * Created by mauricio.mena on 06/09/2016.
 */
public class BasicJoinExample {

    /**
     * Mapper for the customer data
     * 4000001,Mauricio,Mena,30,Developer
     */
    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("customer\t" + parts[1]));
        }
    }


    /**
     * Mapper for the transactions data
     * 001,01-01-2011,4000001,100.00,Gym,Mexico,credit
     */
    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[2]), new Text("tx\t" + parts[3]));
        }
    }


    /**
     * Join in the reducer phase
     */
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "";
            double total = 0.00;
            int count = 0;
            for(Text text : values){
                String parts[] = text.toString().split("\t");
                if (parts[0].equals("tx")) {
                    count++;
                    total += Float.parseFloat(parts[1]);
                }else if(parts[0].equals("customer")){
                    name = parts[1];
                }
            }

            String str = String.format("%d\t%f", count, total);
            context.write(new Text(name), new Text(str));
        }
    }

    /**
     * Entry point for the logic program
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Reduce-side join example");
        job.setJarByClass(BasicJoinExample.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TransactionMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(configuration).delete(outputPath,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
