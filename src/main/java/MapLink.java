import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapLink extends Mapper<LongWritable, Text, Text, Text> {
    private Text link_id = new Text();
    private Text value = new Text();
    String current = "";
    Boolean person = false;

    @Override
    public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
        String line = input_line.toString();

        Pattern link_pattern = Pattern.compile(".*name.*");
        Matcher link_matcher = link_pattern.matcher(line);

        if(!link_matcher.find()){
            String[] tmp_triplet = line.split("\t");

            link_id.set(tmp_triplet[0]);
            value.set(tmp_triplet[1]);
            context.write(link_id, value);
        }

        /*Configuration conf = context.getConfiguration();
        File idfile = new File(conf.get("idfile"));
        String line = input_line.toString();

        String[] tmp_triplet = line.split("\t");
        String id = getId(tmp_triplet[0]);
        Pattern link_pattern = Pattern.compile(".*(notable_for)(.display|.object).*");
        Matcher link_matcher = link_pattern.matcher(line);

        if(!current.equals(id)) {
            person = false;
            current = id;
            String current_line;
            try (BufferedReader br = new BufferedReader(new FileReader(idfile))) {
                while ((current_line = br.readLine()) != null) {
                    if (current_line.contains(id)) {
                        person = true;
                        if (link_matcher.matches()) {
                            if (tmp_triplet[2].contains("\"")) {
                                link_id.set(id);
                                value.set(tmp_triplet[2]);
                                context.write(link_id, value);
                            } else {
                                String[] link_value = tmp_triplet[2].split("/");
                                link_value[4] = link_value[4].substring(0, link_value[4].length() - 1);

                                link_id.set(id);
                                value.set(link_value[4]);
                                context.write(link_id, value);
                            }
                        }
                    }
                }
            } catch (Exception e) {

            }
        }
        else if(person && link_matcher.matches()){
            if (tmp_triplet[2].contains("\"")) {
                link_id.set(id);
                value.set(tmp_triplet[2]);
                context.write(link_id, value);
            } else {
                String[] link_value = tmp_triplet[2].split("/");
                link_value[4] = link_value[4].substring(0, link_value[4].length() - 1);

                link_id.set(id);
                value.set(link_value[4]);
                context.write(link_id, value);
            }
        }*/
    }

    public String getId(String base_triplet){
        String[] id = base_triplet.split("/");
        id[4] = id[4].substring(0, id[4].length() - 1);
        return id[4];
    }
}
