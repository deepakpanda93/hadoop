package com.koitoer.bd.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Using counter API in hadoop
 * one, 1386023259550
 * two, 1389523259558
 * Created by mauricio.mena on 06/09/2016.
 */
public class CounterExample {

    public enum MONTH{
        DEC, JAN, FEB
    }

    /**
     * Entry point for hadoop map reduce
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(CounterExample.class);
        job.setNumReduceTasks(0);
        job.setJobName("CounterTest");
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Counters counters = job.getCounters();
        Counter c1 = counters.findCounter(MONTH.DEC);
        System.out.println(c1.getDisplayName() + " " + c1.getValue());

        c1 = counters.findCounter(MONTH.JAN);
        System.out.println(c1.getDisplayName() + " " + c1.getValue());

        c1 = counters.findCounter(MONTH.FEB);
        System.out.println(c1.getDisplayName() + " " + c1.getValue());

    }

    /**
     * Mapper who convert each linea and increment the count of the months.
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] strs = line.split(",");
            long lts = Long.parseLong(strs[1]);
            DateTime dateTime = new DateTime(lts);
            int m = dateTime.getMonthOfYear();

            switch (m){
                case 11:
                    context.getCounter(MONTH.DEC).increment(1);
                    break;
                case 0:
                    context.getCounter(MONTH.JAN).increment(1);
                    break;
                case 1:
                    context.getCounter(MONTH.FEB).increment(1);
                    break;
            }

            Text text = new Text("success");
            context.write(text,text);
        }
    }



}
