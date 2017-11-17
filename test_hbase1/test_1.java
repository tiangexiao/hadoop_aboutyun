import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
/*原本想用maven来进行项目的构建，但是在使用maven的时候出现了很多的问题*/
public class test_1 {
         public static void main(String[] args) throws IOException {
         
         Configuration conf = HBaseConfiguration.create();
         conf.set("hbase.zookeeper.quorum", "master");//使用eclipse时必须添加这个，否则无法定位
         //conf.set("hbase.zookeeper.property.clientPort", "4397");
         HBaseAdmin admin = new HBaseAdmin(conf);// 新建一个数据库管理员//新api
         HTableDescriptor desc=new HTableDescriptor(TableName.valueOf("blog"));
         //HTableDescriptor desc = new HTableDescriptor("blog");
         desc.addFamily(new HColumnDescriptor("article"));
         desc.addFamily(new HColumnDescriptor("author"));
         admin.createTable(desc );
         admin.close();
         //admin.disableTable("blog");
         //admin.deleteTable("blog");
         //assertThat(admin.tableExists("blog"),is(false));
   }
}