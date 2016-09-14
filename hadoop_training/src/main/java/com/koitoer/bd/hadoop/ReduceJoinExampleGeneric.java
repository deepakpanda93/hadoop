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
import java.util.Arrays;

/**
 * src/main/resources/custs src/main/resources/txns out_rjeg
 * Join in the reducer example using writables to transport the information from mappers to reducers
 * Created by mauricio.mena on 06/09/2016.
 */
public class ReduceJoinExampleGeneric {

    /**
     *
     */
        public static class MyGenericWritable extends GenericWritable {

            private static Class<? extends Writable>[] CLASSES = null;

            static {
                CLASSES = (Class<? extends Writable>[]) new Class[]{
                        CustomerWritable.class,
                        TransactionWritable.class,
                };
            }

            public MyGenericWritable() {
            }

            public MyGenericWritable(Writable instance) {
                set(instance);
            }

            @Override
            protected Class<? extends Writable>[] getTypes() {
                return CLASSES;
            }

            @Override
            public String toString() {
                return "MyGenericWritable [getTypes()=" + Arrays.toString(getTypes()) + "]";
            }

        }


        public static class TransactionWritable implements Writable, WritableComparable<TransactionWritable>{

            private Text transactionId = new Text();
            private DoubleWritable information = new DoubleWritable();

            public TransactionWritable() {
            }

            public TransactionWritable(String transactionId, Double cost) {
                this.transactionId.set(transactionId);
                this.information.set(cost);
            }

            public static TransactionWritable read(DataInput in) throws IOException {
                TransactionWritable transactionWritable = new TransactionWritable();
                transactionWritable.readFields(in);
                return transactionWritable;
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
    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, MyGenericWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            CustomerWritable customerWritable = new CustomerWritable(parts[0], parts[1]);
            context.write(new Text(parts[0]), new MyGenericWritable(customerWritable));
        }
    }


    /**
     * Mapper for the transactions data
     * 001,01-01-2011,4000001,100.00,Gym,Mexico,credit
     */
    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, MyGenericWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            TransactionWritable transactionWritable = new TransactionWritable(parts[0], Double.parseDouble(parts[3]));
            context.write(new Text(parts[2]), new MyGenericWritable(transactionWritable));

        }
    }


    /**
     * Join in the reducer phase
     */
    public static class ReduceJoinReducer2 extends Reducer<Text, MyGenericWritable, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<MyGenericWritable> values, Context context) throws IOException, InterruptedException {
            double total = 0.00;
            int count = 0;
            String customerName = "";
            for(MyGenericWritable genericWritable : values){
                Writable rawValue = genericWritable.get();
                if(rawValue instanceof CustomerWritable){
                    customerName = ((CustomerWritable)rawValue).customerName.toString();
                }else if(rawValue instanceof TransactionWritable){
                    count++;
                    total += Float.parseFloat(((TransactionWritable) rawValue).information.toString());
                }
            }

            String str = String.format("%d\t%f", count, total);
            context.write(new Text(customerName), new Text(str));
        }
    }

    /**
     * Entry point for the logic program
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ReduceJoinExampleGeneric.class);
        job.setReducerClass(ReduceJoinReducer2.class);
        job.setMapOutputValueClass(MyGenericWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyGenericWritable.class);
        //job.setGroupingComparatorClass(CustomerWritableJoiningGroupingComparator.class);

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
