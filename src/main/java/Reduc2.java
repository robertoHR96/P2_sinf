import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Esta clase implementa el primer paso del proceso de reducción en un job MapReduce en Hadoop.
 * El propósito de este Reducer es sumar las cantidades de productos para cada clave, donde la clave
 * es una combinación de producto y hora, y emitir el resultado agrupado por producto y hora.
 */
public class Reduc2 extends Reducer<Text, DoubleWritable, Text, Text> {

    // Formato decimal para los totales de ventas
    private final DecimalFormat decimalFormat = new DecimalFormat("#.##");

    /**
     * Método reduce que procesa la lista de valores asociados a una clave y emite el resultado agrupado.
     *
     * @param key       La clave de entrada.
     * @param coValues  La lista de cantidades de productos asociadas a la clave.
     * @param context   El contexto del MapReduce que se utiliza para escribir la salida.
     * @throws IOException          Si hay un error de lectura/escritura al procesar la entrada o escribir la salida.
     * @throws InterruptedException Si el proceso del MapReduce se interrumpe.
     */
    public void reduce(Text key, Iterable<DoubleWritable> coValues, Context context) throws IOException, InterruptedException {
        Map<String, Double> hourSalesMap = new HashMap<>();

        // Iterar sobre las cantidades de productos asociadas a la clave
        for (DoubleWritable coValue : coValues) {
            double totalCo = coValue.get();
            String[] keyParts = key.toString().split(" ");
            StringBuilder productBuilder = new StringBuilder();

            // Concatenar todos los elementos excepto el último
            for (int i = 0; i < keyParts.length - 1; i++) {
                productBuilder.append(keyParts[i]);
                // Agregar un espacio entre los elementos si es necesario
                if (i < keyParts.length - 2) {
                    productBuilder.append(" ");
                }
            }
            String product = productBuilder.toString();
            String hour = keyParts[keyParts.length - 1];

            // Actualizar el mapa de ventas por producto y hora
            if (!hourSalesMap.containsKey(product + " hora: " + hour)) {
                hourSalesMap.put(product + " hora: " + hour, totalCo);
            } else {
                double currentTotal = hourSalesMap.get(product + " hora: " + hour);
                hourSalesMap.put(product + " hora: " + hour, currentTotal + totalCo);
            }
        }

        // Emitir los resultados agrupados por producto y hora
        for (Map.Entry<String, Double> entry : hourSalesMap.entrySet()) {
            String product = entry.getKey();
            Double totalSales = entry.getValue();
            context.write(new Text(product), new Text(" total: " + decimalFormat.format(totalSales)));
        }
    }
}
