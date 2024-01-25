import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.HashMap; // import the HashMap class

/**
 * Esta clase implementa el primer paso del proceso de reducción en un job MapReduce en Hadoop.
 * El propósito de este Reducer es sumar las cantidades de productos para cada clave y emitir
 * la clave junto con el total de cantidades.
 */
public class Reduc1 extends Reducer<Text, Text, Text, Text> {

    // Formato decimal para el total de cantidades
    private final DecimalFormat decimalFormat = new DecimalFormat("#.##");

    /**
     * Método reduce que procesa la lista de valores asociados a una clave y emite el resultado.
     *
     * @param key      La clave de entrada.
     * @param coValues La lista de cantidades de productos asociadas a la clave.
     * @param context  El contexto del MapReduce que se utiliza para escribir la salida.
     * @throws IOException          Si hay un error de lectura/escritura al procesar la entrada o escribir la salida.
     * @throws InterruptedException Si el proceso del MapReduce se interrumpe.
     */
    public void reduce(Text key, Iterable<Text> coValues, Context context) throws IOException, InterruptedException {
        double totalCo = 0.0;
        Integer tot =0;
        String val="";
        HashMap<String, Integer> mapa = new HashMap<String, Integer>();

        final String ciuProd[] = key.toString().split(":");

        // Sumar las cantidades de productos asociadas a la clave
        for (Text coValue : coValues) {
            String ss[] = coValue.toString().split(":");
            if (ss.length > 2) {
                if (mapa.containsKey(ss[0])) {
                    mapa.put(ss[0], (mapa.get(ss[0]) + Integer.parseInt(ss[1]) ));
                }else{

                    mapa.put(ss[0], Integer.parseInt(ss[1]) );
                }
            }
        }
        for (String i : mapa.keySet()) {
            if(mapa.get(i) > tot ){
                val = i;
                tot = mapa.get(i);
            }
        }

        // Emitir la clave junto con el total de cantidades si es mayor que cero
        if (totalCo > 0 && (ciuProd.length == 2)) {
            context.write(new Text(ciuProd[0]), new Text( val+ ":" +tot ));
        }
    }
}
