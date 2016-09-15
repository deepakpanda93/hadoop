package com.koitoer.bd.hadoop;

import com.google.common.collect.Lists;
import com.koitoer.bd.hadoop.mapjoin.CombineValuesMapper;
import com.koitoer.bd.hadoop.mapjoin.SortByKeyMapper;
import com.koitoer.bd.hadoop.mapjoin.SortByKeyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Map-side joins offer substantial gains in performance since we are
 *                  avoiding the cost of sending data across the network
 * The datasets to be joined are already sorted by the same key and have the same number of partitions
 * Of the two datasets to be joined, one is small enough to fit into memory
 * Created by mauricio.mena on 14/09/2016. forked from bbejeck repository
 */
public class MapJoinExample {


    /**
     * https://github.com/bbejeck/hadoop-algorithms/blob/master/src/bbejeck/mapred/joins/reduce/ManyToManyReduceSideJoinDriver.java
     * http://codingjunkie.net/mapreduce-reduce-joins/
     * src/main/resources/join/dataset1 src/main/resources/join/dataset2 mje_out
     * Hadoop sorts all keys and guarantees that keys with the same value are sent to the same reducer
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String separator = ",";
        String keyIndex = "0";
        int numReducers = 10;
        String jobOneInputPath = args[0];
        String jobTwoInputPath = args[1];
        String joinJobOutPath = args[2];

        String jobOneSortedPath = jobOneInputPath + "_sorted";
        String jobTwoSortedPath = jobTwoInputPath + "_sorted";

        Job firstSort = Job.getInstance(getConfiguration(keyIndex, separator));
        configureJob(firstSort, "firstSort", numReducers, jobOneInputPath, jobOneSortedPath, SortByKeyMapper.class, SortByKeyReducer.class);

        Job secondSort = Job.getInstance(getConfiguration(keyIndex, separator));
        configureJob(secondSort, "secondSort", numReducers, jobTwoInputPath, jobTwoSortedPath, SortByKeyMapper.class, SortByKeyReducer.class);

        Job mapJoin = Job.getInstance(getMapJoinConfiguration(separator, jobOneSortedPath, jobTwoSortedPath));
        configureJob(mapJoin, "mapJoin", 0, jobOneSortedPath + "," + jobTwoSortedPath, joinJobOutPath, CombineValuesMapper.class, Reducer.class);
        mapJoin.setInputFormatClass(CompositeInputFormat.class);

        //Run the previous configured jobs
        List<Job> jobs = Lists.newArrayList(firstSort, secondSort, mapJoin);
        int exitStatus = 0;
        for (Job job : jobs) {
            boolean jobSuccessful = job.waitForCompletion(true);
            if (!jobSuccessful) {
                System.out.println("Error with job " + job.getJobName() + "  " + job.getStatus().getFailureInfo());
                exitStatus = 1;
                break;
            }
        }
        System.exit(exitStatus);
    }

    private static void configureJob(Job job,
                                     String jobName,
                                     int numReducers,
                                     String inPath,
                                     String outPath,
                                     Class<? extends Mapper> mapper,
                                     Class<? extends Reducer> reducer) throws java.io.IOException {
        job.setNumReduceTasks(numReducers);
        job.setJobName(jobName);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setMapOutputKeyClass(Text.class);
        job.setJarByClass(MapJoinExample.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
    }


    private static Configuration getMapJoinConfiguration(String separator, String... paths) {
        Configuration config = new Configuration();
        config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", separator);
        //Will use the separator character to set the first value as the key and the rest will be used for the value.
        String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, paths);
        config.set("mapred.join.expr", joinExpression);
        config.set("separator", separator);
        return config;
    }

    private static Configuration getConfiguration(String keyIndex, String separator) {
        Configuration config = new Configuration();
        config.set("keyIndex", keyIndex);
        config.set("separator", separator);
        return config;
    }
}
