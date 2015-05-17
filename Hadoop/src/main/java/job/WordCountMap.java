package job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author efi
 */
public class WordCountMap extends
        Mapper<Object, Text, Text, Text>{
    
    
     @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        
        String[] words = value.toString().replaceAll("[^a-zA-Z, ]", "").split(",");
        String new_key = words[0];
        String filename = "";
        
        HashMap<String, Integer> appearences = new HashMap<>();
        int num;
        
        for (int i=1; i<words.length-1; i++)
        {
            //count appearences of each work in each file
            if (!appearences.containsKey(words[i]))
            {
                 appearences.put(words[i], 1);
            }
            else
            {
                num = appearences.get(words[i]);
                num++;
                appearences.put(words[i], num);
            }
        }
        
        //if this word is important it will appear a lot of times in one file
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
    }
    
}