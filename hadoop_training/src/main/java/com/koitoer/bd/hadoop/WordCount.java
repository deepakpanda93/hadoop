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
 * Run using $hadoop jar hadoop-training-1.0.jar com.koitoer.sl.WordCount inputPath outputPath
 * Created by mauricio.mena on 31/08/2016.
 */
public class WordCount extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        //configuration.set("mapred.job.tracker", "local");
        //configuration.set("hadoop.tmp.dir","/home/koitoer/tmp");
        //configuration.set("os.name","Windows");
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

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

    /**
     * Input is in the format of {offset, line} -- >  (key, value)
     * For word count example the output will be (apple, 1) - (I, 1) - (banana, 1)
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()){
                value.set(tokenizer.nextToken());
                context.write(value, new IntWritable(1));
            }
        }
    }

    /**
     * Input will be defined by (apple, 1,1,1,1,1) - (I, 1)
     * Output will be (apple,5) - (I,1)
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable x : values){
                sum += x.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
