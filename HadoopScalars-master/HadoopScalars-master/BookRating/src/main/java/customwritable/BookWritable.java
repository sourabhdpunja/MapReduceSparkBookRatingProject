package customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BookWritable implements Writable {

    private Text isbn;
    private Text title;
    private Text author;
    private IntWritable yearOfPublication;
    private Text publisher;

    public BookWritable() {
        this.isbn = new Text();
        this.title = new Text();
        this.author = new Text();
        this.yearOfPublication = new IntWritable();
        this.publisher = new Text();
    }

    public BookWritable(Text isbn, Text title, Text author, IntWritable yearOfPublication, Text publisher) {
        this.isbn = isbn;
        this.title = title;
        this.author = author;
        this.yearOfPublication = yearOfPublication;
        this.publisher = publisher;
    }

    public BookWritable(BookWritable bookWritable){
        this.isbn = new Text(bookWritable.getIsbn().toString());
        this.title = new Text(bookWritable.getTitle().toString());
        this.author = new Text(bookWritable.getAuthor().toString());
        this.yearOfPublication = new IntWritable(bookWritable.getYearOfPublication().get());
        this.publisher = new Text(bookWritable.getPublisher().toString());
    }

    public Text getIsbn() {
        return isbn;
    }

    public void setIsbn(Text isbn) {
        this.isbn = isbn;
    }

    public Text getTitle() {
        return title;
    }

    public void setTitle(Text title) {
        this.title = title;
    }

    public Text getAuthor() {
        return author;
    }

    public void setAuthor(Text author) {
        this.author = author;
    }

    public IntWritable getYearOfPublication() {
        return yearOfPublication;
    }

    public void setYearOfPublication(IntWritable yearOfPublication) {
        this.yearOfPublication = yearOfPublication;
    }

    public Text getPublisher() {
        return publisher;
    }

    public void setPublisher(Text publisher) {
        this.publisher = publisher;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        isbn.write(dataOutput);
        title.write(dataOutput);
        author.write(dataOutput);
        yearOfPublication.write(dataOutput);
        publisher.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isbn.readFields(dataInput);
        title.readFields(dataInput);
        author.readFields(dataInput);
        yearOfPublication.readFields(dataInput);
        publisher.readFields(dataInput);
    }

    @Override
    public String toString(){
        return title.toString() + "," + author.toString() + "," + yearOfPublication.toString()
                + "," + publisher.toString();
    }
}
