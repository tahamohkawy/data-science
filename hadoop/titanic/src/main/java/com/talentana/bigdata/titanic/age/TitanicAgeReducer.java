package com.talentana.bigdata.titanic.age;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TitanicAgeReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //We need to get the average age of people who died by gender
        int sum = 0;
        int count = 0;
        Iterator<IntWritable> iter = values.iterator();
        while (iter.hasNext()){
            sum += iter.next().get();

            count += 1;
        }

        double avg = sum/count;

        context.write(key,new DoubleWritable(avg));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
