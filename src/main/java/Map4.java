import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Esta clase implementa el primer paso de un job MapReduce en Hadoop.
 * El propósito de este Mapper es procesar datos de entrada y generar pares clave-valor
 * donde la clave es una combinación de producto y hora, y el valor es la cantidad de productos en un pedido.
 */
public class Map4 extends Mapper<Object, Text, Text, DoubleWritable> {

    // Separadores utilizados para dividir los campos en la entrada
    private static final String SEPARATOR = ",";
    private static final String SEPARATOR2 = " ";
    private static final String SEPARATOR3 = ":";

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
        // Dividir la línea de entrada en campos utilizando el primer separador
        final String[] values = value.toString().split(SEPARATOR);

        if (values.length > 4) {
            // Obtener el nombre del producto y la fecha
            final String product = format(values[1]);
            final String fecha = format(values[4]);

            // Verificar si el producto y la fecha son válidos
            if (product != null && fecha != null) {
                // Dividir la fecha para obtener la hora
                final String[] fechaHora = fecha.split(SEPARATOR2);
                if (fechaHora.length > 1) {
                    // Obtener la parte de la hora y formatearla
                    final String[] horaMinuto = format(fechaHora[1]).split(SEPARATOR3);
                    if (horaMinuto.length > 1) {
                        final String hora = format(horaMinuto[0]);

                        // Formatear y validar la cantidad
                        final DoubleWritable cantidadProductos = new DoubleWritable((NumberUtils.toInt(values[2])));

                        // Emitir el par clave-valor (producto + hora, cantidadProductos)
                        context.write(new Text(product + " " + hora), cantidadProductos);
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