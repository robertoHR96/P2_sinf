import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Esta clase implementa el primer paso de un job MapReduce en Hadoop.
 * El propósito de este Mapper es procesar datos de entrada y generar pares clave-valor
 * donde la clave es la ciudad y el valor es la cantidad de productos en un pedido.
 */
public class Map6 extends Mapper<Text, Text, Text, Text> {

    // Separadores utilizados para dividir los campos en la entrada
    private static final String SEPARATOR = ",";
    private static final String SEPARATOR2 = ";";

    /**
     * Método map que procesa cada línea de entrada y emite un par clave-valor.
     *
     * @param key     La clave de entrada, no utilizada en este caso.
     * @param value   La línea de texto de entrada.
     * @param context El contexto del MapReduce que se utiliza para escribir la salida.
     * @throws IOException          Si hay un error de lectura/escritura al procesar la entrada o escribir la salida.
     * @throws InterruptedException Si el proceso del MapReduce se interrumpe.
     */


    // Order ID,Product,Quantity Ordered,Price Each,Order Date,Purchase Address

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Dividir la línea de entrada en campos utilizando el primer separador
        final String[] values = value.toString().split(":");

        if (values.length == 2) {

            context.write(key, value);
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
