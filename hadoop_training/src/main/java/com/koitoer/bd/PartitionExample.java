package com.koitoer.bd;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by mauricio.mena on 06/09/2016.
 */
public class PartitionExample {


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                value.set(tokenizer.nextToken());
                output.collect(value, new IntWritable(1));

            }

        }
    }

    // Output types of Mapper should be same as arguments of Partitioner
    public static class MyPartitioner implements Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {

            String myKey = key.toString().toLowerCase();

            if (myKey.equals("hadoop")) {
                return 0;
            }
            if (myKey.equals("data")) {
                return 1;
            } else {
                return 2;
            }
        }

        @Override
        public void configure(JobConf arg0) {
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {

            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(PartitionExample.class);
        conf.setJobName("partition example job");

        // Forcing program to run 3 reducers
        conf.setNumReduceTasks(3);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setPartitionerClass(MyPartitioner.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
