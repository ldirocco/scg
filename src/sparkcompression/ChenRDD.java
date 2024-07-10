package sparkcompression;

import fastdoop.FASTAlongInputFileFormat;
import fastdoop.PartialSequence;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassManifestFactory;

import java.util.Arrays;

public class ChenRDD extends JavaRDD<TextTuple> {

	private final int k;

	public ChenRDD(RDD<TextTuple> parent, int k) {
		super(parent, ClassManifestFactory.fromClass(TextTuple.class));
		this.k = k;
	}

	public static ChenRDD toChenRDD(JavaRDD<byte[]> fromRDD, int maxPatternLength) {
		return new ChenRDD(fromRDD.map(ChenEncoding::encode).rdd(), maxPatternLength);
	}

	public static ChenRDD read(String path, JavaSparkContext sc, int maxPatternLength, int partitions) {
		JavaRDD<byte[]> sequence = sc.newAPIHadoopFile(path, FASTAlongInputFileFormat.class,
				Text.class, PartialSequence.class, sc.hadoopConfiguration()).
				map(t -> t._2.getValue().toUpperCase().replaceAll("[^ACGT]+", "").getBytes())
				.repartition(partitions);

		return toChenRDD(sequence, maxPatternLength);
	}

	public JavaRDD<Integer[]> search(String pattern) {
		PatternTuple[] Pi_A1_A2_list = ChenEncoding.encodePatterns(pattern);

		return map(text -> BMChenAlgorithm.run(text, Pi_A1_A2_list, false));
	}

	@Override
	public ChenRDD cache() {
		return new ChenRDD(super.cache().rdd(), k);
	}

	@Override
	public ChenRDD persist(StorageLevel level) {
		return new ChenRDD(super.persist(level).rdd(), k);
	}
}
