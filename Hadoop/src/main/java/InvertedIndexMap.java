
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Efi Kaltirimidou
 */
public class InvertedIndexMap extends
        Mapper<Object, Text, Text, IntWritable>{
    
     @Override
    public void map(Object key, Text value, Context context) throws IOException {
        
        //Get from configuration the path to the stopwords file and create a StopWords object for filtering
        Configuration conf = context.getConfiguration();
        Path[] filename = DistributedCache.getLocalCacheFiles(conf);
        StopWords stopwords = new StopWords("stopwords.txt");
    }
    
}
