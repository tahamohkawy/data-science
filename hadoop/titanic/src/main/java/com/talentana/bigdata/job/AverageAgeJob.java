package com.talentana.bigdata.job;


import com.talentana.bigdata.titanic.age.TitanicAgeMapper;
import com.talentana.bigdata.titanic.age.TitanicAgeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageAgeJob
{
    public static void main( String[] args ) throws Exception
    {
        Configuration configruation = new Configuration();
        Job avgAgeJob = Job.getInstance(configruation);

        avgAgeJob.setJobName("Average age for dead in Titanic");
        avgAgeJob.setJarByClass(AverageAgeJob.class);
        avgAgeJob.setMapperClass(TitanicAgeMapper.class);
        avgAgeJob.setReducerClass(TitanicAgeReducer.class);

        avgAgeJob.setOutputKeyClass(Text.class);
        avgAgeJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(avgAgeJob,new Path(args[0]));
        FileOutputFormat.setOutputPath(avgAgeJob,new Path(args[1]));

        avgAgeJob.waitForCompletion(true);
    }
}
