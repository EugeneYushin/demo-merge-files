package demo.reducers;

import demo.formats.LineWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;


/**
 * FileMergeReducer
 * <p>
 * Concatenates relevant lines from splitted files.
 * <p>
 * Use MultipleOutputs against Context to flush data.
 */
public class FileMergeReducer extends Reducer<LineWritable, Text, Text, NullWritable> {
    //final static Logger LOG = Logger.getLogger(FileMergeReducer.class);

    private MultipleOutputs<Text, NullWritable> multipleOutputs;
    private Text outLine = new Text();

    public String generateFileName(LineWritable key) {
        String partition = key.fileName.split("_")[0];
        return partition + "_abc.log";
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void reduce(LineWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> textIterator = values.iterator();
        String fullLine = new String();

        while (textIterator.hasNext()) {
            fullLine += textIterator.next() + " ";
        }

        outLine.set(fullLine);

        multipleOutputs.write(outLine, NullWritable.get(), generateFileName(key));
        //context.write(new Text(fullLine), NullWritable.get());
        //context.write(key, outLine);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
