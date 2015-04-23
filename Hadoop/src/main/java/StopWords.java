import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Opens the file given in the constructor and creates an ArrayList of the stopwords the 
 * file contains
 */
public class StopWords {
    
   ArrayList <String> stopwords = new ArrayList<String>();
    
    public StopWords(String filepath)
    {
        // The name of the file to open.
        String fileName = filepath;

        // This will reference one line at a time
        String line;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader
                    = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader;
            bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                this.stopwords.add(line);
            }

            // Always close files.
            bufferedReader.close();
        } catch (FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '"
                    + fileName + "'");
        } catch (IOException ex) {
            System.out.println(
                    "Error reading file '"
                    + fileName + "'");
            // Or we could just do this: 
            // ex.printStackTrace();
        }

    }
    
    /**
     * 
     * @param word
     * @return true if the word is contained in the stopwords otherwise false
     */
    public boolean contains(String word)
    {
        if(this.stopwords.contains(word))
        {
            return true;
        }
        return false;
    }
}