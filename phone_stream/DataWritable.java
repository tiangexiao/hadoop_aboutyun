package phone_stream;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataWritable implements Writable{
    private int upPackNum;
    private int downPackNum;
    private int upPayLoad;
    private int downPayLoad;
    
    public DataWritable() {
    	super();
    }
    
    public DataWritable(int upPackNum, int downPackeNum, int upPayLoad, int downPayLoad) {
    	super();
    	this.upPackNum = upPackNum;
    	this.downPackNum = downPackeNum;
    	this.upPayLoad = upPayLoad;
    	this.downPayLoad = downPayLoad;
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(upPackNum);
        out.writeInt(downPackNum);
        out.writeInt(upPayLoad);
        out.writeInt(downPayLoad);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
            upPackNum = in.readInt();
            downPackNum = in.readInt();
            upPayLoad = in.readInt();
            downPayLoad =in.readInt();
    }
	public int getUpPackNum() {
		return upPackNum;
	}
	public void setUpPackNum(int upPackNum) {
		this.upPackNum = upPackNum;
	}
	public int getDownPackNum() {
		return downPackNum;
	}
	public void setDownPackNum(int downPackNum) {
		this.downPackNum = downPackNum;
	}
	public int getUpPayLoad() {
		return upPayLoad;
	}
	public void setUpPayLoad(int upPayLoad) {
		this.upPayLoad = upPayLoad;
	}
	public int getDownPayLoad() {
		return downPayLoad;
	}
	public void setDownPayLoad(int downPayLoad) {
		this.downPayLoad = downPayLoad;
	}
	
    @Override
    public String toString() {
            return "        " + upPackNum + "        "
                            + downPackNum + "        " + upPayLoad + "        "
                            + downPayLoad;
    }
    
}
