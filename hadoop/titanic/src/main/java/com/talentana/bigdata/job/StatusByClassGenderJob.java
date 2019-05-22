package com.talentana.bigdata.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderMapper;
import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderReducer;



public class StatusByClassGenderJob {
	
	public static void main( String[] args ) throws Exception
    {
        Configuration configuration = new Configuration();
        Job theJob = Job.getInstance(configuration);

        theJob.setJarByClass(StatusByClassGenderJob.class);

        theJob.setJobName("Status By Class and Gender");
        theJob.setJarByClass(StatusByClassGenderJob.class);
        theJob.setMapperClass(StatusByClassGenderMapper.class);
        theJob.setReducerClass(StatusByClassGenderReducer.class);


        theJob.setOutputKeyClass(Text.class);
        theJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(theJob,new Path(args[0]));
        FileOutputFormat.setOutputPath(theJob,new Path(args[1]));

        theJob.waitForCompletion(true);
    }

}
