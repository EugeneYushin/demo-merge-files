package demo.mappers;

import demo.formats.LineWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * FileMergeMapper
 * <p>
 * Parses file and outputs <LineWritable, Text>.
 * <p>
 * The LineWritable key is used by the Partitioner to map lines from batch of files with the same number
 * to particular Reducer, and place lines in correct order.
 */
public class FileMergeMapper extends Mapper<LongWritable, Text, LineWritable, Text> {

    long lineN;
    String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //final String groupName = "LineCounter";
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
        lineN = 0;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LineWritable lw = new LineWritable(lineN, fileName);

        context.write(lw, value);

        //TODO Use counter for calculating Line Numbers
        lineN++;
    }
}
