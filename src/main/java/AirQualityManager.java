import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Esta clase representa el punto de entrada principal para el job MapReduce llamado "AirQualityManager" en Hadoop.
 * Configura y ejecuta un trabajo MapReduce que utiliza diferentes clases de mappers y reducers según las necesidades
 * del análisis de calidad del aire.
 */
public class AirQualityManager extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("AirQualityManager required params: {input file} {output dir}");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf());
		job.setJarByClass(AirQualityManager.class); // clase contiene el javar
		job.setInputFormatClass(TextInputFormat.class); // texto de entrada y salida
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * Seleccionar el mapper y reducer correspondientes
		 * Los Map1, Map2, Map3 y Map5 van con el Reduc1
		 * y el Map4 va con el Reduc2
		 */
		job.setMapperClass(Map3.class); // Clase del mapper
		job.setReducerClass(Reduc1.class); // Clase del reducer



		job.setMapOutputKeyClass(Text.class); // Tipo de clave de salida del mapper
		job.setMapOutputValueClass(Text.class); // Tipo de valor de salida del mapper

		job.setOutputKeyClass(Text.class); // Tipo de clave de salida del reducer
		job.setOutputValueClass(Text.class); // Tipo de valor de salida del reducer

		/*
		job.setMapperClass(Map6.class); // Clase del mapper
		job.setReducerClass(Reduc3.class); // Clase del reducer

		job.setMapOutputKeyClass(Text.class); // Tipo de clave de salida del mapper
		job.setMapOutputValueClass(Text.class); // Tipo de valor de salida del mapper

		job.setOutputKeyClass(Text.class); // Tipo de clave de salida del reducer
		job.setOutputValueClass(Text.class); // Tipo de valor de salida del reducer

		 */

		FileInputFormat.addInputPath(job, new Path(args[0])); // Ruta de entrada
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Ruta de salida

		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * Elimina el directorio de salida si ya existe para evitar errores.
	 *
	 * @param args Los argumentos de línea de comandos, se espera que el segundo argumento sea la ruta de salida.
	 * @throws IOException Si hay un error al acceder al sistema de archivos.
	 */
	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	/**
	 * Método principal que configura y ejecuta el job MapReduce utilizando la clase AirQualityManager.
	 *
	 * @param args Los argumentos de línea de comandos, se espera que el primer argumento sea la ruta de entrada
	 *             y el segundo argumento sea la ruta de salida.
	 * @throws Exception Si hay un error al ejecutar el job MapReduce.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AirQualityManager(), args);
	}

}
