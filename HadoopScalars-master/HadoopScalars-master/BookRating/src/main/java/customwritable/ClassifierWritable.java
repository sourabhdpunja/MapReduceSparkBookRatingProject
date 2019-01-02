// Ref: https://www.mkyong.com/java/how-to-read-and-write-java-object-to-a-file/
// Ref: https://waikato.github.io/weka-wiki/serialization/
package customwritable;

import exceptions.BookRatingException;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import weka.classifiers.Classifier;

import java.io.*;

public class ClassifierWritable implements Writable {

    private static final Logger logger = LogManager.getLogger(ClassifierWritable.class);

    private Classifier classifier;

    public ClassifierWritable() {
        super();
    }

    public ClassifierWritable(Classifier classifier) {
        super();
        this.classifier = classifier;
    }

    public Classifier getClassifier() {
        return classifier;
    }

    public void setClassifier(Classifier classifier) {
        this.classifier = classifier;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(classifier);
        dataOutput.writeInt(b.toByteArray().length);
        dataOutput.write(b.toByteArray());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        byte[] data = new byte[size];
        dataInput.readFully(data);
        ByteArrayInputStream b = new ByteArrayInputStream(data);
        ObjectInputStream o = new ObjectInputStream(b);
        try {
            classifier = (Classifier) o.readObject();
        } catch (ClassNotFoundException e) {
            logger.error(e);
            throw new BookRatingException("Exception occurred while reading Classifier object.", e);
        }
    }
}
