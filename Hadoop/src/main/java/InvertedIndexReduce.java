
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
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
        HashMap<String, Integer> appearences = new HashMap<String, Integer>();
        int num;
        String filename = "";
        String output = "[";
        
        for (Text value : values)
        {
            filename = value.toString();
            //count appearences of each work in each file
            if (!appearences.containsKey(filename))
            {
                 appearences.put(filename, 1);
            }
            else
            {
                num = appearences.get(filename);
                num++;
                appearences.put(filename, num);
            }
            //add filename to inverted index if it does not exists already
            if (!output.contains(filename))
            {
                output = output + filename + ",";
            }
        }
        
        //if this word is important it will appear a lot of times in one file but
        //but not in other files
        
        //find max in values
        boolean important = true;
        int max = 0;
        for (Entry<String,Integer> e : appearences.entrySet())
        {
            if (max < e.getValue())
            {
                max = e.getValue();
                filename = e.getKey();
            }
        }
        //compare max with every other
        for (Entry<String,Integer> e : appearences.entrySet())
        {
            if ((max - e.getValue() == 0) || max - e.getValue() < 3)
            {
                if (!filename.equals(e.getKey()))
                {
                    important = false;
                }
            }
        }
        
        output = output + "]";
        context.write(word, new Text(output)); 
    }
}
