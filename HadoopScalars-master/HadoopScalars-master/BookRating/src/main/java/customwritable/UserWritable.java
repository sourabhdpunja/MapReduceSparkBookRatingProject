package customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserWritable implements Writable {

    private IntWritable userId;
    private Text location;
    private org.apache.hadoop.io.IntWritable age;

    public UserWritable() {
        this.userId = new IntWritable();
        this.location = new Text();
        this.age = new IntWritable();
    }

    public UserWritable(IntWritable userId, Text location, IntWritable age) {
        this.userId = userId;
        this.location = location;
        this.age = age;
    }

    public IntWritable getUserId() {
        return userId;
    }

    public void setUserId(IntWritable userId) {
        this.userId = userId;
    }

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public IntWritable getAge() {
        return age;
    }

    public void setAge(IntWritable age) {
        this.age = age;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        userId.write(dataOutput);
        location.write(dataOutput);
        age.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId.readFields(dataInput);
        location.readFields(dataInput);
        age.readFields(dataInput);
    }

    @Override
    public String toString(){
        return location.toString() + "," + age.toString();
    }
}
