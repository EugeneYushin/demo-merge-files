package demo.jobs;

import demo.formats.LineNumGroupComparator;
import demo.formats.LineWritable;
import demo.formats.NonSplittableTextInputFormat;
import demo.mappers.FileMergeMapper;
import demo.reducers.FileMergeReducer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;


/**
 * Merge lines from Input Files<br>
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * </ol>
 */
public class FileMergeJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new ConfigurationException("Usage: [input dir] [output dir]");
        }
        System.exit(ToolRunner.run(new Configuration(), new FileMergeJob(), args));
    }

    //final static Logger LOG = Logger.getLogger(FileMergeJob.class);

    public int getReducersCount(String inputDir) throws IOException {
        int reducerCount = 0;
        Set filePartition = new HashSet<String>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(inputDir), conf);

        FileStatus[] ls = fs.listStatus(new Path(inputDir));

        for (FileStatus file : ls) {
            if (!filePartition.contains(file.getPath().getName().split("_")[0])) {
                reducerCount++;
                filePartition.add(file.getPath().getName().split("_")[0]);
            }
        }

        return reducerCount == 0 ? 1 : reducerCount;
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));

        Job job = Job.getInstance(conf);
        job.setJobName("Merge Files");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //FileInputFormat.addInputPath(job, new Path("/user/root/demo_logs"));
        //FileOutputFormat.setOutputPath(job, new Path("/user/root/demo_logs_out"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Prevent splitting to provide correct order of lines
        job.setInputFormatClass(NonSplittableTextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // Prevent create zero-sized default output, eg part-00000
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, NullWritable.class);

        job.setMapperClass(FileMergeMapper.class);
        job.setReducerClass(FileMergeReducer.class);
        job.setNumReduceTasks(this.getReducersCount(args[0]));
        //job.setNumReduceTasks(this.getReducersCount("/user/root/demo_logs"));

        // Identity HashPartitioner

        job.setMapOutputKeyClass(LineWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setGroupingComparatorClass(LineNumGroupComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
