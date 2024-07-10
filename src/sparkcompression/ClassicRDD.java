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
import scala.reflect.ClassTag;

public class ClassicRDD extends JavaRDD<byte[]> {

	public ClassicRDD(RDD<byte[]> rdd) {
		super(rdd, ClassManifestFactory.fromClass(byte[].class));
	}

	public static ClassicRDD toClassicRDD(JavaRDD<byte[]> fromRDD) {
		return new ClassicRDD(fromRDD.rdd());
	}

	public static ClassicRDD read(String path, JavaSparkContext sc, int partitions) {
		JavaRDD<byte[]> sequence = sc.newAPIHadoopFile(path, FASTAlongInputFileFormat.class,
				Text.class, PartialSequence.class, sc.hadoopConfiguration()).
				map(t -> t._2.getValue().toUpperCase().replaceAll("[^ACGT]+", "").getBytes())
				.repartition(partitions);

		return toClassicRDD(sequence);
	}

	@Override
	public ClassicRDD cache() {
		return new ClassicRDD(super.cache().rdd());
	}

	@Override
	public ClassicRDD persist(StorageLevel level) {
		return new ClassicRDD(super.persist(level).rdd());
	}

	public JavaRDD<Integer[]> search(String pattern) {
		return map(read -> BMAlgorithm.bm_algo_multi(read, pattern.getBytes()));
	}
}
