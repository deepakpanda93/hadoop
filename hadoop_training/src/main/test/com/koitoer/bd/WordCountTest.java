package com.koitoer.bd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mauricio.mena on 12/09/2016.
 */
public class WordCountTest {

    /**
     * Test method for the mapper
     * @throws IOException
     */
    @Test
    public void testMapper() throws IOException {
        WordCount.Map mapper = new WordCount.Map();
        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.withInput(new LongWritable(0), new Text("Hello World World"));
        List<Pair<Text, IntWritable>> resultMap = mapDriver.run();
        Assertions.assertThat(resultMap.get(0).getFirst()).isEqualTo(new Text("Hello"));
        Assertions.assertThat(resultMap.get(0).getSecond()).isEqualTo(new IntWritable(1));
        Assertions.assertThat(resultMap.get(1).getFirst()).isEqualTo(new Text("World"));
        Assertions.assertThat(resultMap.get(1).getSecond()).isEqualTo(new IntWritable(1));
        Assertions.assertThat(resultMap.get(2).getFirst()).isEqualTo(new Text("World"));
        Assertions.assertThat(resultMap.get(2).getSecond()).isEqualTo(new IntWritable(1));
        Assertions.assertThat(resultMap.size()).isEqualTo(3);
    }

    /**
     * Test method for the reducer
     * @throws IOException
     */
    @Test
    public void testReducer () throws IOException {
        WordCount.Reduce reducer = new WordCount.Reduce();
        ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = ReduceDriver.newReduceDriver(reducer);
        reduceDriver.withInput(new Text("Hello"), Arrays.asList(new IntWritable(1), new IntWritable(1)));
        List<Pair<Text, IntWritable>> resultList = reduceDriver.run();
        Assertions.assertThat(resultList.get(0).getFirst()).isEqualTo(new Text("Hello"));
        Assertions.assertThat(resultList.get(0).getSecond()).isEqualTo(new IntWritable(2));
        Assertions.assertThat(resultList.size()).isEqualTo(1);


    }

}