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

import java.util.Arrays;

public class BpeRDD extends JavaPairRDD<String[], byte[]> {

	public BpeRDD(RDD<Tuple2<String[], byte[]>> parent) {
		super(parent, ClassManifestFactory.fromClass(String[].class), ClassManifestFactory.fromClass(byte[].class));
	}

	public static BpeRDD toBpeRDD(JavaRDD<String> fromRDD) {
		return new BpeRDD(fromRDD.map(BPEncoding::encode).rdd());
	}

	public static BpeRDD read(String path, JavaSparkContext sc, int partitions) {
		JavaRDD<String> sequence = sc.newAPIHadoopFile(path, FASTAlongInputFileFormat.class,
				Text.class, PartialSequence.class, sc.hadoopConfiguration()).
				map(t -> t._2.getValue().toUpperCase().replaceAll("[^ACGT]+", ""))
				.repartition(partitions);

		return toBpeRDD(sequence);
	}

	public JavaRDD<Integer[]> search(String pattern) {
		return map(text -> BMTypeAlgorithm.run(pattern, text));
	}

	@Override
	public BpeRDD cache() {
		return new BpeRDD(super.cache().rdd());
	}

	@Override
	public BpeRDD persist(StorageLevel level) {
		return new BpeRDD(super.persist(level).rdd());
	}
}
