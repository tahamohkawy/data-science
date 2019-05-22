package com.talentana.bigdata.titanic.byComposite;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class StatusByClassGenderReducer3 extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        IntWritable theSum = new IntWritable();

        int sum = 0;
        Iterator<IntWritable> iter = values.iterator();

        while (iter.hasNext()){
            sum += iter.next().get();
        }

        theSum.set(sum);
        context.write(key, theSum);
    }
}
