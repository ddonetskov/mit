import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class Task2MultiOutputFormat extends MultipleTextOutputFormat<Text, Text> {
    
    /*
      Using the key to form the output file's name
    */
    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String leaf) {
        return new Path(key.toString(), leaf).toString();
    }

    /*
      Discard the key as we don't need it any longer
    */
    @Override
    protected Text generateActualKey(Text key, Text value) {
        return null;
    }
}
