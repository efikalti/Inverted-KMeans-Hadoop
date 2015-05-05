
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Efi Kaltirimidou
 */
public class InvertedIndexReduce extends
        Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text text, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        String output = "[";
        for (Text filename : values)
        {
            output = output + ", " + filename.toString();
        }
        output = output + "]";
        context.write(text, new Text(output)); 
    }
}
