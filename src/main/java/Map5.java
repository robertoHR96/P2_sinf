import java.io.IOException;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Esta clase implementa el primer paso de un job MapReduce en Hadoop.
 * El propósito de este Mapper es procesar datos de entrada y generar pares clave-valor
 * donde la clave es el nombre del producto y el valor es la cantidad de productos en un pedido.
 */
public class Map5 extends Mapper<Object, Text, Text, DoubleWritable> {

    // Separador utilizado para dividir los campos en la entrada
    private static final String SEPARATOR = ",";

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
            // Obtener la fecha y validarla
            final String fecha = format(values[4]);
            if (fecha != null) {
                // Crear un DoubleWritable con la cantidad de productos
                final DoubleWritable cantidadProductos = new DoubleWritable(NumberUtils.toDouble(values[2]));

                // Emitir el par clave-valor (producto, cantidadProductos)
                context.write(new Text(values[1]), cantidadProductos);
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
