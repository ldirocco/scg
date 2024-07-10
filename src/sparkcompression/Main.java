package sparkcompression;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setMaster("yarn");
//        SparkConf conf = new SparkConf().setMaster("local");
//        args = new String[] {"data/536.fasta", "", "bpe", "false", "false", "32"};

        String input_file = args[0];
        int partitions = Integer.parseInt(args[5]);

        JavaSparkContext sc;

        if (args[2].equals("classic"))
            conf.setAppName("CMP (Clear)");
        else if (args[2].equals("chen"))
            conf.setAppName("CMP (Chen)");
        else if (args[2].equals("bpe"))
            conf.setAppName("CMP (BPE)");
        else if (args[2].equals("lzw"))
            conf.setAppName("CMP (LZW)");

        sc = new JavaSparkContext(conf);

//        List<String> Ps = sc.newAPIHadoopFile(args[1], FASTAshortInputFileFormat.class,
//                Text.class, Record.class, sc.hadoopConfiguration()).
//                map(t -> t._1.getValue().toUpperCase().replaceAll("[^ACGT]", "")).
//                collect();

        List<String> Ps = new ArrayList<>();
        Ps.add("TTCCTTAGGAAAAGGGGAAGACCACCAATC");
        Ps.add("AGAGGATTATGTACATCAGCACAGGATGCA");
        Ps.add("GAAGGACTTAGGGGAGTCCTCATGAAAAAT");
        Ps.add("GTATTAGTACAGTAGAGCCTTCACCGGCAT");
        Ps.add("TCTGTTTATTAAGTTATTTCTACAGCAAAA");
        Ps.add("CGATCATATGCAGATCCGCAGTGCGCGGTA");

        long time = System.currentTimeMillis();

        if (args[2].equals("classic")) {

            ClassicRDD sequence;

            // load
            if (Boolean.parseBoolean(args[3])) {
                JavaRDD<byte[]> rdd = sc.objectFile("classic_rdd", partitions);
                sequence = new ClassicRDD(rdd.rdd());
            }
            else
                sequence = ClassicRDD.read(input_file, sc, partitions);

            // save
            if (Boolean.parseBoolean(args[4]))
                sequence.saveAsObjectFile("classic_rdd");

            sequence = sequence.persist(StorageLevel.MEMORY_AND_DISK());

            long found = 0;

            for (String P : Ps)
                found += sequence
                        .search(P)
                        .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);

            System.out.println("Sequence length: " + sequence.aggregate(0L, (v, text) -> text.length + v, Long::sum));
            System.out.println("Found: " + found);
        }
        else if (args[2].equals("chen")) {
            ChenRDD sequence;

            int maxK = Ps.get(0).length();

            // load
            if (Boolean.parseBoolean(args[3])) {
                JavaRDD<TextTuple> rdd = sc.objectFile("chen_rdd", partitions);
                sequence = new ChenRDD(rdd.rdd(), maxK);
            }
            else
                sequence = ChenRDD.read(input_file, sc, maxK, partitions);

            // save
            if (Boolean.parseBoolean(args[4]))
                sequence.saveAsObjectFile("chen_rdd");

            sequence = sequence.persist(StorageLevel.MEMORY_AND_DISK());

            long found = 0;

            for (String P : Ps)
                found += sequence
                        .search(P)
                        .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);

            System.out.println("Sequence length: " + sequence.aggregate(0L, (v, text) -> text.getT().length + v, Long::sum));
            System.out.println("Found: " + found);
        }
        else if (args[2].equals("bpe")) {
            BpeRDD sequence;

            // load
            if (Boolean.parseBoolean(args[3])) {
                JavaRDD<Tuple2<String[], byte[]>> rdd = sc.objectFile("bpe_rdd", partitions);
                sequence = new BpeRDD(rdd.rdd());
            }
            else
                sequence = BpeRDD.read(input_file, sc, partitions);

            // save
            if (Boolean.parseBoolean(args[4]))
                sequence.saveAsObjectFile("bpe_rdd");

            sequence = sequence.persist(StorageLevel.MEMORY_AND_DISK());

            long found = 0;

            for (String P : Ps)
                found += sequence
                        .search(P)
                        .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);

            System.out.println("Sequence length: " + sequence.aggregate(0L, (v, text) -> text._2.length + v, Long::sum));
            System.out.println("Found: " + found);
        }
        else if (args[2].equals("lzw")) {
            LzwRDD sequence;

            // load
            if (Boolean.parseBoolean(args[3])) {
                JavaRDD<Tuple2<String[], byte[]>> rdd = sc.objectFile("lzw_rdd", partitions);
                sequence = new LzwRDD(rdd.rdd());
            }
            else
                sequence = LzwRDD.read(input_file, sc, partitions);

            // save
            if (Boolean.parseBoolean(args[4]))
                sequence.saveAsObjectFile("lzw_rdd");

            sequence = sequence.persist(StorageLevel.MEMORY_AND_DISK());

            long found = 0;

            for (String P : Ps)
                found += sequence
                        .search(P)
                        .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);

            System.out.println("Sequence length: " + sequence.aggregate(0L, (v, text) -> text._2.length + v, Long::sum));
            System.out.println("Found: " + found);
        }

        sc.stop();

        System.out.println("TIME (s): " + ((System.currentTimeMillis() - time) / 1000.0));
    }
}
