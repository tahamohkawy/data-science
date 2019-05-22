package com.talentana.bigdata.job;

import com.talentana.bigdata.model.TitanicValue;
import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderMapper2;
import com.talentana.bigdata.titanic.byComposite.StatusByClassGenderReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SurvivorsByClassGenderJob2 {

    public static void main(String[] args ) throws Exception{
        Configuration configuration = new Configuration();
        Job theJob = Job.getInstance(configuration);

        theJob.setJobName("Survivors By Class Gender Job2");
        theJob.setJarByClass(SurvivorsByClassGenderJob2.class);
        theJob.setMapperClass(StatusByClassGenderMapper2.class);
        theJob.setReducerClass(StatusByClassGenderReducer2.class);

        theJob.setOutputKeyClass(Text.class);
        theJob.setOutputValueClass(TitanicValue.class);

        FileInputFormat.addInputPath(theJob,new Path(args[0]));
        FileOutputFormat.setOutputPath(theJob,new Path(args[1]));


        theJob.waitForCompletion(true);

    }



}
