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

public class ch08 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String hadoop_home = "D:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(ch08.class);
        job.setReducerClass(ch08.MyReducer.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        FileOutputFormat.setOutputPath(job, new Path("/agename"));

        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("students"), scan, ch08.MyMapper.class, Text.class, Text.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String birthday= Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
            String name = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("name"))));
            String[] tok = birthday.trim().split("-");
            context.write(new Text(""), new Text(name + "," + tok[0]));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String empName;
            String firstName = "";
            String secondName = "";
            String thirdName = "";
            long empAge = 0;
            long firstAge = 2022;
            long secondAge = 2022;
            long thirdAge = 2022;
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empAge = Long.parseLong(val.toString().split(",")[1]);
                if (empAge < firstAge) {
                    thirdName = secondName;
                    thirdAge = secondAge;
                    secondName = firstName;
                    secondAge = firstAge;
                    firstName = empName;
                    firstAge = empAge;
                } else if (empAge < secondAge) {
                    thirdName = secondName;
                    thirdAge = secondAge;
                    secondName = empName;
                    secondAge = empAge;
                } else if (empAge < thirdAge) {
                    thirdName = empName;
                    thirdAge = empAge;
                }
            }
            context.write(new Text(firstName), new Text("出生年份"+String.valueOf(firstAge)));
            context.write(new Text(secondName), new Text("出生年份"+String.valueOf(secondAge)));
            context.write(new Text(thirdName), new Text("出生年份"+String.valueOf(thirdAge)));
        }
    }
}
