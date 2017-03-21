package com.koitoer.bd.hadoop.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by mauricio.mena on 13/09/2016.
 */
public class TaggedJoiningPartitioner extends Partitioner<TaggedKey, Text> {

    @Override
    public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
        return taggedKey.getJoinKey().hashCode() % numPartitions;
    }
}
