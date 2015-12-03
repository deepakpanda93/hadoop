package com.koitoer.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by koitoer on 12/1/15.
 *
 */
public class MaxTemperature {

    /**
     * First we need to start dfs. start-dfs.sh
     * Then we need to put the files into the hdfs with hdfs dfs -put fileXXX outputDIR
     * After put the file in dfs we need to start hadoop job: hadoop com.koitoer.hadoop.MaxTemperature fileXXX ouput
     * When job is completed you could se the results with : hdfs dfs -cat outputDIR/part-r-00000
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output patth>");
            System.exit(-1);
        }

        Configuration configuration = new Configuration();

        configuration.addResource(new Path("/HADOOP_HOME/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/HADOOP_HOME/etc/hadoop/hdfs-site.xml"));

        Job job = new Job(configuration);
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //Some errors is due to the port, you need to setup the core-site.xml and add the configuration to the client.
        //java.io.IOException: NameNode is not formatted.  SOLUTION: Run hdfs namenode -format to format the namenode
        //PATH should be set with this configuration
        //core-site.xml, this will set the env and the port.
        /*
          <configuration>
            <property>
            <name>fs.defaultFS</name>
            <value>hdfs://localhost:9000</value>
            </property>
            </configuration>
        */
        //hdfs-site.xml, this will set the env and the port.
        /*
            <configuration>
            <property>
              <name>dfs.namenode.name.dir</name>
              <value>/home/koitoer/mydata/hdfs/namenode</value>
              </property>


              <property>
              <name>dfs.datanode.data.dir</name>
                <value>/home/koitoer/mydata/hdfs/datanode</value>
              </property>
            </configuration>
        */
    }
}