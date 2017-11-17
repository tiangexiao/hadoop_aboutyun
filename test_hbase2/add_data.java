
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class add_data {
    private static Configuration conf = null;
    static {
    	conf = HBaseConfiguration.create();
    	conf.set("hbase.zookeeper.quorum", "master");
    	conf.set("hbase.zookeeper.property.clientPort", "2181");
    	
    }
    
    public static void main(String[] args) throws IOException {
		String cols[] = new String[1];
		String colsvalue[] = new String[1];
		cols[0] = "title";
		colsvalue[0] = "aboutyun";
		createTable();
		addData("www.xiaotiange.com","blog",cols,colsvalue);
	}
    
    private static void createTable() throws MasterNotRunningException,ZooKeeperConnectionException,IOException{
    	HBaseAdmin admin = new HBaseAdmin(conf);
    	if(admin.tableExists(TableName.valueOf("LogTable"))) {
    		System.out.println("table is exist");
    		System.exit(0);
    	} else {
    		HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("blog"));
    		descriptor.addFamily(new HColumnDescriptor("article"));
    		admin.createTable(descriptor);
    		admin.close();
    		System.out.println("create table successful!");
    		
    	}
    	
    }
    
    
    private static void addData(String rowKey,String tableName, 
    		String[] column1, String[] value1) throws IOException{
    	Put put = new Put(Bytes.toBytes(rowKey));
    	HTable table = new HTable(conf, Bytes.toBytes(tableName));
    	HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
    	
    	for(int i = 0; i < columnFamilies.length; i ++) {
    		String familyName = columnFamilies[i].getNameAsString();
    		if(familyName.equals("article")) {
    			for(int j = 0; j < column1.length; j++) {
    				put.add(Bytes.toBytes(familyName),Bytes.toBytes(column1[j]),Bytes.toBytes(value1[j]));
    			}
    		}
    	}
    	table.put(put);
    	System.out.println("add successfaul");
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
}
