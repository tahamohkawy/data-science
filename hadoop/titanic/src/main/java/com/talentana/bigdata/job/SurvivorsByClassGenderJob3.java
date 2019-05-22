package com.talentana.bigdata.job;

import com.talentana.bigdata.model.TitanicValue;
import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderMapper2;
import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderMapper3;
import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderReducer2;
import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderReducer3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SurvivorsByClassGenderJob3 {

    public static void main(String[] args ) throws Exception{
        Configuration configuration = new Configuration();
        Job theJob = Job.getInstance(configuration);

        theJob.setJobName("Survivors By Class Gender Job3");
        theJob.setJarByClass(SurvivorsByClassGenderJob3.class);
        theJob.setMapperClass(StatusByClassGenderMapper3.class);
        theJob.setReducerClass(StatusByClassGenderReducer3.class);

        theJob.setOutputKeyClass(Text.class);
        theJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(theJob,new Path(args[0]));
        FileOutputFormat.setOutputPath(theJob,new Path(args[1]));

        theJob.waitForCompletion(true);

    }



}
