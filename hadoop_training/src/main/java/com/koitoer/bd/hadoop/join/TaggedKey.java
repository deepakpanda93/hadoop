package com.koitoer.bd.hadoop.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Make the TaggedKey as a Writable object and implements the Comparable
 * Created by mauricio.mena on 13/09/2016 forked from bbejeck repository
 */
public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();

    public void set(String key, int tag){
        this.joinKey.set(key);
        this.tag.set(tag);
    }

    public static TaggedKey read(DataInput in) throws IOException {
        TaggedKey taggedKey = new TaggedKey();
        taggedKey.readFields(in);
        return taggedKey;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        tag.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        tag.readFields(in);
    }

    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0 ){
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
        return compareValue;
    }

    public Text getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(Text joinKey) {
        this.joinKey = joinKey;
    }

    public IntWritable getTag() {
        return tag;
    }

    public void setTag(IntWritable tag) {
        this.tag = tag;
    }
}
