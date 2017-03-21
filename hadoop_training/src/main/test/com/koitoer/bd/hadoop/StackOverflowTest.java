package com.koitoer.bd.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by koitoer on 9/12/16.
 */
public class StackOverflowTest {

    @Test
    public void testMapper() throws IOException {
        String line1 = "1\t563355\t62701\t0\t1235000081\tphp,error,gd,image-processing\t220\t2   563372  67183   2       1235000501\n";
        StackOverflowJob.StackMap stackMap = new StackOverflowJob.StackMap();
        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = MapDriver.newMapDriver(stackMap);
        mapDriver.withInput(new LongWritable(1), new Text(line1));
        List<Pair<Text, IntWritable>> result = mapDriver.run();
        Assertions.assertThat(result.size()).isEqualTo(4);
        Assertions.assertThat(result.get(0).getFirst()).isEqualTo(new Text("php"));
        Assertions.assertThat(result.get(1).getFirst()).isEqualTo(new Text("error"));
        Assertions.assertThat(result.get(2).getFirst()).isEqualTo(new Text("gd"));
        Assertions.assertThat(result.get(3).getFirst()).isEqualTo(new Text("image-processing"));
    }


    @Test
    public void testReducer() throws IOException {
        StackOverflowJob.StackReduce stackReduce = new StackOverflowJob.StackReduce();
        ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(stackReduce);
        reduceDriver.withInput(new Text("php"), Arrays.asList(new IntWritable(1), new IntWritable(1)));
        List<Pair<Text, IntWritable>> redPairList = reduceDriver.run();
        Assertions.assertThat(redPairList.get(0).getFirst()).isEqualTo(new Text("php"));
        Assertions.assertThat(redPairList.get(0).getSecond()).isEqualTo(new IntWritable(2));
    }
}
