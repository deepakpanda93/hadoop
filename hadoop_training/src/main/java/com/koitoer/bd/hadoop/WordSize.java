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
import java.util.StringTokenizer;

/**
 * hadoop jar hadoop-training.jar com.koitoer.sl.WordSize /user/cloudera/wordcountproblem /user/cloudera/wordsize_out
 * Created by mauricio.mena on 02/09/2016.
 */
public class WordSize extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = new Job(configuration, "WordSize");
        job.setJarByClass(WordSize.class);
        job.setJobName("Word size count job");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordSizeMap.class);
        job.setReducerClass(WordSizeReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WordSize(), args);
        System.exit(exitCode);
    }

    /**
     * The input is in the form of (offset, line)
     * The output is in the form of (1, I-O) - (2, do-of-in) - (3, the-all-not)
     */
    private static class WordSizeMap extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()){
                String word = tokenizer.nextToken();
                int count = word.length();
                context.write(new IntWritable(count), new Text(word));
            }
        }
    }

    /**
     * The input is in the form of (1, I-O) - (2, do-of-in) - (3, the-all-not)
     * The output in the form of (1,2) - (2-3) - (3,3)
     */
    private static class WordSizeReducer extends Reducer <IntWritable, Text, IntWritable, IntWritable>{

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(Text text : values){
                sum++;
            }
            context.write(key,new IntWritable(sum));
        }
    }
}
