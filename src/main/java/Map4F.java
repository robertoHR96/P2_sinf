import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map4F extends Mapper<Object, Text, Text, DoubleWritable> {

    private static final String SEPARATOR = ",";
    private static final String SEPARATOR2 = " ";
    private static final String SEPARATOR3 = ":";

    /**
     * orderId,product,cantidad,precio,(04/07/23 14:32),direccion("calle, ciudad, codPostal")
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        if (values.length > 4) {
            // final String date = format(values[0]);
            final String fech = format(values[4]);
            // "992 Sunset St; San Francisco; CA 94016"
            if (fech != null) {
                final String[] fehc2 = fech.toString().split(SEPARATOR2);
                if (fehc2.length > 1) {
                    final String[] hora = format(fehc2[1]).toString().split(SEPARATOR3);
                    if (hora.length > 1) {
                        final String hora2 = format(hora[0]);
                        // pasamos las cantidades a tipo de dato procesable
                        final DoubleWritable multiplicaicon = new DoubleWritable((NumberUtils.toInt(values[2])));
                        // lo preparamos para pasarselo al reduc
                        context.write(new Text(hora2), multiplicaicon);
                    }
                }
            }
        }
    }

    private String format(String value) {
        return value.trim();
    }
}
