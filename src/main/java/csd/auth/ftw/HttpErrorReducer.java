package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HttpErrorReducer extends Reducer<IntWritable, Text, Text, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // value string
        int size = 0;
        StringBuilder listStr = new StringBuilder("[");
        for (Text value : values) {
            size++;
            listStr.append(value.toString() + ",");
        }

        // remove last comma
        listStr.deleteCharAt(listStr.length() - 1);
        listStr.append("]");

        // key string
        String keyStr = String.format("%3s\t%4d\t", key.toString(), size);

        context.write(new Text(keyStr), new Text(listStr.toString()));
    }
}
