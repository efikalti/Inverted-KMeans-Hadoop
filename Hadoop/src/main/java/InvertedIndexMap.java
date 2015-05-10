
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author Efi Kaltirimidou
 */
public class InvertedIndexMap extends
        Mapper<Object, Text, Text, Text>{
    
    private Text id;
    private Text final_word;
    
     @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        //Get from configuration the path to the stopwords file and create a StopWords object for filtering
        Configuration conf = context.getConfiguration();
        Path[] filename = DistributedCache.getLocalCacheFiles(conf);
        StopWords stopwords = new StopWords(filename[0].toString());
        
        //get filename of the current input and add it to Text variable id
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileId = fileSplit.getPath().getName();
        id = new Text(fileId);
        
        //remove xml tags
        String new_value = value.toString().replaceAll("<.*?>", "");
                
        //split the line into words,remove punctuation and separate each word
        String[] words = new_value.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split(" ");
        
        //check each word if it is a stopword and emit to reducer those who are not
        for (String str : words) { //filter each word
            if(!stopwords.contains(str))
            {//str is not a stopword 
                final_word = new Text(str);
                //emit the word and the id of the file it belongs to
                context.write(final_word, id); 
            }
        }
    }
    
}
