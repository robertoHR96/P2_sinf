import java.io.IOException;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Esta clase implementa el primer paso de un job MapReduce en Hadoop.
 * El propósito de este Mapper es procesar datos de entrada y generar pares clave-valor
 * donde la clave es la fecha y el valor es el resultado de la multiplicación de la cantidad
 * y el precio de un producto en un pedido.
 */
public class Map1 extends Mapper<Object, Text, Text, DoubleWritable> {

    // Separador utilizado para dividir los campos en la entrada
    private static final String SEPARATOR = ",";

    // Order ID,Product,Quantity Ordered,Price Each,Order Date,Purchase Address

    /**
     * Método map que procesa cada línea de entrada y emite un par clave-valor.
     *
     * @param key     La clave de entrada, no utilizada en este caso.
     * @param value   La línea de texto de entrada.
     * @param context El contexto del MapReduce que se utiliza para escribir la salida.
     * @throws IOException          Si hay un error de lectura/escritura al procesar la entrada o escribir la salida.
     * @throws InterruptedException Si el proceso del MapReduce se interrumpe.
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Dividir la línea de entrada en campos utilizando el separador
        final String[] values = value.toString().split(SEPARATOR);

        if (values.length > 0) {
            final String co = format(values[4]);
            if (co != null) {
                final String f1 = format(co.split(" ")[0]);
                if (f1 != null) {
                    final String[] fechas = f1.split("/");
                    if (fechas.length > 0) {
                        // Extraer el componente de la fecha
                        final String fecha = fechas[0];

                        // Formatear y validar la cantidad y el precio
                        values[2] = format(values[2]);
                        values[3] = format(values[3]);

                        // Calcular la multiplicación de la cantidad y el precio
                        final DoubleWritable multiplicacion = new DoubleWritable((NumberUtils.toInt(values[2]) * NumberUtils.toDouble(values[3])));

                        // Emitir el par clave-valor (fecha, multiplicacion)
                        context.write(new Text(fecha), multiplicacion);
                    }
                }
            }
        }
    }

    /**
     * Método de utilidad para formatear una cadena, eliminando espacios en blanco alrededor.
     *
     * @param value La cadena a formatear.
     * @return La cadena formateada.
     */
    private String format(String value) {
        return value.trim();
    }
}
