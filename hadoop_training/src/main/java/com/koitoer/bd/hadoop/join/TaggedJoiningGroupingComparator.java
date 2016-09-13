package com.koitoer.bd.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Class used to Compare the internal values from two Writable objects
 * Created by mauricio.mena on 13/09/2016 forked from bbejeck repository
 */
public class TaggedJoiningGroupingComparator extends WritableComparator {

    public TaggedJoiningGroupingComparator() {
        super(TaggedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TaggedKey taggedKey1 = (TaggedKey)a;
        TaggedKey taggedKey2 = (TaggedKey)b;
        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}