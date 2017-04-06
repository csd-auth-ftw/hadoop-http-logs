package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HttpLogAnalyser {
    public static final String URL_JOB_NAME = "URL_JOB_NAME";
    public static final String TIMESTAMP_JOB_NAME = "TIMESTAMP_JOB_NAME";

    private Path inputPath;
    private Path outputUrlPath;
    private Path outputTimestampPath;

    public HttpLogAnalyser(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        if (args.length != 3) {
            System.out.println("Please enter input and two output paths");
            System.exit(1);
        }

        inputPath = new Path(args[0]);
        outputUrlPath = new Path(args[1]);
        outputTimestampPath = new Path(args[2]);

        int exitCodeUrl = executeJob(URL_JOB_NAME);
        int exitCodeTimestamp = executeJob(TIMESTAMP_JOB_NAME);

        System.exit(Math.max(exitCodeUrl, exitCodeTimestamp));
    }

    private int executeJob(String name) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(HttpLogAnalyser.class);

        job.setMapperClass(HttpErrorMapper.class);
        job.setReducerClass(HttpErrorReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setJar("http.jar");

        Path outputPath = name.equals(URL_JOB_NAME) ? outputUrlPath: outputTimestampPath;
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;        
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        new HttpLogAnalyser(args);
    }
}
