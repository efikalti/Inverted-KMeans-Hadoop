package job;


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
    public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        String filename = "";
        String output = "[";
        
        for (Text value : values)
        {
            filename = value.toString();
            //add filename to inverted index if it does not exists already
            if (!output.contains(filename))
            {
                output = output + filename + ",";
            }
        }
        
        output = output + "]";
        context.write(word, new Text(output));
    }
}
