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
public class Reduc3 extends Reducer<Text, Text, Text, Text> {

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
    public void reduce(Text key, Iterable<Text> coValues, Context context) throws IOException, InterruptedException {
        double totalCo = 0.0;
        Integer total = 0;
        String masVendido = "";
        // Sumar las cantidades de productos asociadas a la clave
        for (Text coValue : coValues) {
            String ciuProd [] = key.toString().split(":");
            if(ciuProd.length == 2){
                Integer auxTotal = Integer.parseInt(ciuProd[0]);
                if(auxTotal > total){
                    total = auxTotal;
                    masVendido = ciuProd[1]+ " con "+new String(total.toString());
                }

            }
        }
        context.write(new Text(key), new Text(masVendido));
    }
}
