import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdMapClass extends Mapper<LongWritable, Text, Text, Text> {
    private Text id = new Text();
    private Text value = new Text();

    @Override
    public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
        String filename = "person_id.txt";
        Writer out = new OutputStreamWriter(new FileOutputStream(filename, true), "UTF-8");

        Pattern pattern = Pattern.compile("(.*?(people.person).*)");
        String line = input_line.toString();
        Matcher matcher = pattern.matcher(line);

        if(matcher.matches()) {
            String[] tmp_triplet = line.split("\t");
            String person_id = getId(tmp_triplet[0]);

            BufferedReader br = new BufferedReader(new FileReader(filename));
            boolean find_line = false;

            String current_line;
            while ((current_line = br.readLine()) != null) {
                if(current_line.equals(person_id)){
                    find_line = true;
                    break;
                }
            }

            if(!find_line){
                out.write(person_id + "\n");
                out.close();

                value.set("person_id");
                id.set(person_id);
                context.write(value, id);
            }
        }
    }

    public String getId(String base_triplet){
        String[] id = base_triplet.split("/");
        id[4] = id[4].substring(0, id[4].length() - 1);
        return id[4];
    }
}
