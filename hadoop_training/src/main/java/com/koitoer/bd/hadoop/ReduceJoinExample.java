package com.koitoer.bd.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * hadoop jar hadoop-training.jar com.koitoer.sl.BasicJoinExample /user/cloudera/datasets/custs /user/cloudera/datasets/txns /user/cloudera/out/join
 * src/resources/custs src/resources/txns out_rje_out
 * Join in the reducer example use the custs and txns data sets
 * Created by mauricio.mena on 06/09/2016.
 */
public class ReduceJoinExample {

    public static class TransactionWritable implements Writable, WritableComparable<TransactionWritable>{

        private Text transactionId;
        private Text information;

        public TransactionWritable(Text transactionId, Text information) {
            this.transactionId = transactionId;
            this.information = information;
        }

        @Override
        public int compareTo(TransactionWritable o) {
            return this.transactionId.compareTo(o.transactionId);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            transactionId.write(dataOutput);
            information.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            transactionId.readFields(dataInput);
            information.readFields(dataInput);
        }
    }

    /**
     * Customer Writable Class
     */
    public static class CustomerWritable implements Writable, WritableComparable<CustomerWritable>{

        private Text customerKey = new Text();
        private Text customerName = new Text();

        public CustomerWritable() {
        }

        public CustomerWritable(String customerKey) {
            this.customerKey.set(customerKey);
        }

        public CustomerWritable(String customerKey, String customerName) {
            this.customerKey.set(customerKey);
            this.customerName.set(customerName);
        }

        public static CustomerWritable read(DataInput in) throws IOException {
            CustomerWritable customerWritable = new CustomerWritable();
            customerWritable.readFields(in);
            return customerWritable;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            customerKey.write(dataOutput);
            customerName.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            customerKey.readFields(dataInput);
            customerName.readFields(dataInput);
        }

        @Override
        public int compareTo(CustomerWritable o) {
            int compareValue = this.customerKey.compareTo(o.customerKey);
            if(compareValue == 0 ){
                compareValue = this.customerName.compareTo(o.customerName);
            }
            return compareValue;
        }
    }

    /**
     * Mapper for the customer data
     * 4000001,Mauricio,Mena,30,Developer
     */
    public static class CustomerMapper extends Mapper<LongWritable, Text, CustomerWritable, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new CustomerWritable(parts[0], parts[1]), new DoubleWritable(0));
        }
    }


    /**
     * Mapper for the transactions data
     * 001,01-01-2011,4000001,100.00,Gym,Mexico,credit
     */
    public static class TransactionMapper extends Mapper<LongWritable, Text, CustomerWritable, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            TransactionWritable transactionWritable = new TransactionWritable(new Text(parts[0]),new Text(parts[3]));
            context.write(new CustomerWritable(parts[2]), new DoubleWritable(Double.valueOf(parts[3])));

        }
    }


    /**
     * Join in the reducer phase
     */
    public static class ReduceJoinReducer extends Reducer<CustomerWritable, DoubleWritable, Text, Text>{

        @Override
        protected void reduce(CustomerWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double total = 0.00;
            int count = -1;
            for(DoubleWritable doubleWritable : values){
                count++;
                total += doubleWritable.get();
            }
            String str = String.format("%d\t%f", count, total);
            context.write(new Text(key.customerName), new Text(str));
        }
    }

    /**
     * Entry point for the logic program
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ReduceJoinExample.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(CustomerWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setGroupingComparatorClass(CustomerWritableJoiningGroupingComparator.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TransactionMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(configuration).delete(outputPath,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     *
     */
    private static class CustomerWritableJoiningGroupingComparator extends WritableComparator {
        public CustomerWritableJoiningGroupingComparator() {
            super(CustomerWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CustomerWritable customerKey1 = (CustomerWritable)a;
            CustomerWritable customerKey2 = (CustomerWritable)b;
            return customerKey1.customerKey.compareTo(customerKey2.customerKey);
        }
    }
}
