package sparkcompression;

import fastdoop.FASTAlongInputFileFormat;
import fastdoop.PartialSequence;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;

public class LzwRDD extends JavaPairRDD<String[], byte[]> {

	public LzwRDD(RDD<Tuple2<String[], byte[]>> parent) {
		super(parent, ClassManifestFactory.fromClass(String[].class), ClassManifestFactory.fromClass(byte[].class));
	}

	public static LzwRDD toLzwRDD(JavaRDD<String> fromRDD) {
		return new LzwRDD(fromRDD.map(LZWEncoding::encode).rdd());
	}

	public static LzwRDD read(String path, JavaSparkContext sc, int partitions) {
		JavaRDD<String> sequence = sc.newAPIHadoopFile(path, FASTAlongInputFileFormat.class,
				Text.class, PartialSequence.class, sc.hadoopConfiguration()).
				map(t -> t._2.getValue().toUpperCase().replaceAll("[^ACGT]+", ""))
				.repartition(partitions);

		return toLzwRDD(sequence);
	}

	public JavaRDD<Integer[]> search(String pattern) {
		return map(text -> BMTypeAlgorithm.run(pattern, text));
	}

	@Override
	public LzwRDD cache() {
		return new LzwRDD(super.cache().rdd());
	}

	@Override
	public LzwRDD persist(StorageLevel level) {
		return new LzwRDD(super.persist(level).rdd());
	}
}
