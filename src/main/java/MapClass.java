import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// A mapper class converting each line of input into a key/value pair
// Each character is turned to a key with value as 1
public class MapClass extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /*String v = value.toString();
        for (int i = 0; i < v.length(); i++) {
            word.set(v.substring(i, i + 1));
            context.write(word, one);
        }*/
        Pattern pattern = Pattern.compile("(.*?(person).*)");
        String string = value.toString();
        /*string.replace('<', '_');
        string.replace('>', '_');
        string.replace(':', '_');*/
        Matcher matcher = pattern.matcher(string);

        //if (matcher.matches())
        while(matcher.find())
        {
            String[] first = matcher.group().split("\t");
            String[] second = first[0].split("/");
            second[4] = second[4].substring(0, second[4].length() - 1);
            //System.out.println(matcher.group());
            word.set(second[4]);
            context.write(word, one);
        }


        /*StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
          while (itr.hasMoreTokens()) {
              if(itr.nextToken().matches("(.*?(person))")) {
                  word.set(itr.nextToken());
                  context.write(word, one);
              }else {
                  word.set("islooo else");
                  context.write(word, one);
              }
        }*/

    }
}