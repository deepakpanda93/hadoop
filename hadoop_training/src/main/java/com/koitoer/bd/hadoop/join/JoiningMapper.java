package com.koitoer.bd.hadoop.join;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;

/**
 * Created by mauricio.mena on 13/09/2016.
 */
public class JoiningMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

    private int keyIndex;
    private Splitter splitter;
    private Joiner joiner;
    private TaggedKey taggedKey = new TaggedKey();
    private Text data = new Text();
    private int joinOrder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex"));
        String separator = context.getConfiguration().get("separator");
        splitter = Splitter.on(separator).trimResults();
        joiner = Joiner.on(separator);
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> values = Lists.newArrayList(splitter.split(value.toString()));
        //Take the uid out of the array
        String joinKey = values.remove(keyIndex);
        //Take the remain string and put in another string separated by comma
        String valuesWithOutKey = joiner.join(values);
        //Save in the tagged key the joinOrder integer
        taggedKey.set(joinKey, joinOrder);
        data.set(valuesWithOutKey);
        //Write in the form (UID: value1, value2)
        context.write(taggedKey, data);
    }

}