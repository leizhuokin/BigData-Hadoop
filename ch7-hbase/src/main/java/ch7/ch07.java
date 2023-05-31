package ch7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ch07 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String hadoop_home = "D:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(ch07.class);
        job.setReducerClass(ch07.MyReducer.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        FileOutputFormat.setOutputPath(job, new Path("/birthdayname"));

        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("students"), scan, ch07.MyMapper.class, Text.class, Text.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String birthday= Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
            String name = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("name"))));
            Set<String> tokSet = new HashSet<>();
            String[] tok = birthday.toString().trim().split("-");
            tokSet.add(name);
            context.write(new Text(tok[1]+"-"+tok[2]), new Text(String.join(",", tokSet)));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> locs = new ArrayList<>();
            for (Text value : values) {
                locs.add(value.toString());
            }
            context.write(key, new Text(String.join(",", locs)));
        }
    }
}
