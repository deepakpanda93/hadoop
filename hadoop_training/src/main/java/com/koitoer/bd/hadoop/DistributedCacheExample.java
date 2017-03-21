package com.koitoer.bd.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * hadoop jar hadoop-training.jar com.koitoer.sl.DistributedCacheExample /user/cloudera/datasets/dcinput /user/cloudera/out/dc
 * Distributed Cache example is join in the mapper function use the abc.dat + dcinput datasets
 * This is the new version the latest API
 * Created by mauricio.mena on 06/09/2016.
 */
public class DistributedCacheExample {

    public static class DistributedMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Map<String, String> abMap = new HashMap<String, String>();
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        /**
         * abc.dat
         * up   utah
         * ma   manhattan
         * br   broomfield
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles();
            for(URI file : files){
                if(file.getPath().contains("datasets/abc.dat")){
                    Path path = new Path(file);
                    BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
                    String line = reader.readLine();
                    while(line != null){
                        String [] tokens = line.split("\t");
                        abMap.put(tokens[0], tokens[1]);
                        line = reader.readLine();
                    }
                }
            }

            if(abMap.isEmpty()){
                throw new IOException("File not found");
            }
        }

        /**
         * Mapper output
         * Utah up xxxx
         * Manhattan ma xxxx
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String row = value.toString();
            String [] tokens = row.split("\t");
            outputKey.set(abMap.get(tokens[0]));
            outputValue.set(row);
            context.write(outputKey, outputValue);
        }
    }


    /**
     * Entry point for hadoop job
     * @param args
     * @throws IOException
     * @throws URISyntaxException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(DistributedCacheExample.class);
        job.setJobName("Distributed cache example");
        job.setNumReduceTasks(0);

        job.addCacheFile(new Path("/user/cloudera/datasets/abc.dat").toUri());
        job.setMapperClass(DistributedMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }



}
