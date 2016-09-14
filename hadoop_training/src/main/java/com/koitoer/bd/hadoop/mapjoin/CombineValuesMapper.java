package com.koitoer.bd.hadoop.mapjoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

/**
 * Take the previous ordered values from the other values and use to put in the same file
 * Created by mauricio.mena on 14/09/2016. Forked from bbejeck repository
 */
public class CombineValuesMapper extends Mapper<Text, TupleWritable, NullWritable, Text> {

    private static final NullWritable nullKey = NullWritable.get();
    private Text outValue = new Text();
    private StringBuilder valueBuilder = new StringBuilder();
    private String separator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        separator = context.getConfiguration().get("separator");
    }

    @Override
    protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
        valueBuilder.append(key).append(separator);
        for (Writable writable : value) {
            valueBuilder.append(writable.toString()).append(separator);
        }
        valueBuilder.setLength(valueBuilder.length() - 1);
        outValue.set(valueBuilder.toString());
        context.write(nullKey, outValue);
        valueBuilder.setLength(0);
    }
}
