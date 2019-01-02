package customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RatingWritable implements Writable {

    private IntWritable userId;
    private Text isbn;
    private IntWritable rating;

    public RatingWritable(){
        this.userId = new IntWritable();
        this.isbn = new Text();
        this.rating = new IntWritable();
    }

    public RatingWritable(IntWritable userId, Text isbn, IntWritable rating) {
        this.userId = userId;
        this.isbn = isbn;
        this.rating = rating;
    }

    public IntWritable getUserId() {
        return userId;
    }

    public void setUserId(IntWritable userId) {
        this.userId = userId;
    }

    public Text getIsbn() {
        return isbn;
    }

    public void setIsbn(Text isbn) {
        this.isbn = isbn;
    }

    public IntWritable getRating() {
        return rating;
    }

    public void setRating(IntWritable rating) {
        this.rating = rating;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        userId.write(dataOutput);
        isbn.write(dataOutput);
        rating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId.readFields(dataInput);
        isbn.readFields(dataInput);
        rating.readFields(dataInput);
    }

    @Override
    public String toString(){
        return rating.toString();
    }
}
