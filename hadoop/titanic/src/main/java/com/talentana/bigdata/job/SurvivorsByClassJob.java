package com.talentana.bigdata.job;


import com.talentana.bigdata.titanic.byClass.SurvivedByClassMapper;
import com.talentana.bigdata.titanic.byClass.SurvivedByClassReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SurvivorsByClassJob {
    public static void main( String[] args ) throws Exception
    {
        Configuration configuration = new Configuration();
        Job theJob = Job.getInstance(configuration);

        theJob.setJobName("Survivors By Class");
        theJob.setJarByClass(SurvivorsByClassJob.class);
        theJob.setMapperClass(SurvivedByClassMapper.class);
        theJob.setReducerClass(SurvivedByClassReducer.class);

        theJob.setOutputKeyClass(Text.class);
        theJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(theJob,new Path(args[0]));
        FileOutputFormat.setOutputPath(theJob,new Path(args[1]));

        theJob.waitForCompletion(true);
    }
}
