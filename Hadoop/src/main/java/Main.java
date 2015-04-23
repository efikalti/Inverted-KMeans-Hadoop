
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author Efi Kaltirimidou
 */
public class Main {
	
	public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ClassNotFoundException
	{
            if (args.length < 2) {
                System.out.println("Proper use: Main <Input path> <Output path>");
            } else {
                
                // Paths of input and output directory
                Path input = new Path(args[0]);    //input path
                Path output = new Path(args[1]);    //output path
                
                // Create configuration
                Configuration conf = new Configuration(true);
                // Create connector with the hdfs system
                FileSystem hdfs = FileSystem.get(conf);
                // Add the file stopwords to the Distributed Cache so it is accessible from every process
                DistributedCache.addCacheFile(new URI("stopwords.txt"), conf); 

                // Delete output if it exists to avoid error
                if (hdfs.exists(output)) {
                    hdfs.delete(output, true);
                }
                
                // Commands for the Inverted Index job
                Job InvertedIndex = new Job(conf, "Inverted Index");
                // Assign Map and Reduce class
                InvertedIndex.setJarByClass(InvertedIndexMap.class);
                InvertedIndex.setMapperClass(InvertedIndexMap.class);
                InvertedIndex.setReducerClass(InvertedIndexReduce.class);
                // Define the data type of keys and values
                InvertedIndex.setMapOutputKeyClass(Text.class); //key from map
                InvertedIndex.setMapOutputValueClass(IntWritable.class);//value from map
                InvertedIndex.setOutputKeyClass(Text.class);    //key from reduce
                InvertedIndex.setOutputValueClass(Text.class); //value from reduce
                // Set input path 
                FileInputFormat.addInputPath(InvertedIndex, input);
                InvertedIndex.setInputFormatClass(TextInputFormat.class);
                // Set output path
                FileOutputFormat.setOutputPath(InvertedIndex, output);
                InvertedIndex.setOutputFormatClass(SequenceFileOutputFormat.class);

                //Execute InvertedIndex 
                int code = InvertedIndex.waitForCompletion(true) ? 0 : 1;

                if (code == 0) //InvertedIndex completed successfully
                {
                    
                }
            }
            
	}
	
}
