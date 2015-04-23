
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Efi Kaltirimidou
 */
public class InvertedIndexReduce extends
        Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text text, Iterable<IntWritable> values, Context context)
    {
    }
}
