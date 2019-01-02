//Ref: http://www.lichun.cc/blog/2012/05/hadoop-genericwritable-sample-usage/
package customwritable;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;

public class MyGenericWritable extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
                BookWritable.class,
                RatingWritable.class
        };
    }

    public MyGenericWritable() {
    }

    public MyGenericWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }

    @Override
    public String toString() {
        return "MyGenericWritable [getTypes()=" + Arrays.toString(getTypes()) + "]";
    }
}
