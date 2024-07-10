package sparkcompression;

import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import scala.Tuple2;

import java.util.*;

public class BPEncoding {
    public static final int R = 256;

    public static Tuple2<String[], byte[]> encode(String text) {
        Char2ObjectOpenHashMap<String> D = new Char2ObjectOpenHashMap<>();
        byte[] S = text.getBytes();

        char idx_newchar = 0;
        boolean[] characters = new boolean[R];

        characters['A'] = true;
        characters['C'] = true;
        characters['G'] = true;
        characters['T'] = true;

        Int2LongAVLTreeMap counts = new Int2LongAVLTreeMap();

        for (int i = 0; i < text.length()-1; i++) {
            int pair = encodePair(text.charAt(i), text.charAt(i+1));
            counts.merge(pair, 1L, Long::sum);
        }

        do {
            Integer max_pair = findMaxPair(counts);

            if (max_pair == null)
                break;

            while (idx_newchar < characters.length && characters[idx_newchar++]) ;

            if (idx_newchar == characters.length)
                break; // throw new RuntimeException("No more character to use for the encoding");

            char c = (char) (idx_newchar - 1);
            characters[c] = true;

            String pair = decodePair(max_pair);
            S = replace(S, pair, c, counts);
            D.put(c, pair);
        } while(true);

        String[] D_array = new String[R];

        D_array['A'] = "A";
        D_array['C'] = "C";
        D_array['G'] = "G";
        D_array['T'] = "T";

//        System.out.println(text.length() * 1.0 / S.length());

        for (char token : D.keySet())
            D_array[token] = phrase(token, D);

        return new Tuple2<>(D_array, S);
    }

    private static byte[] replace(byte[] S, String pair, char c, Int2LongAVLTreeMap counts) {
        byte[] S1 = new byte[S.length];
        char c1, c2;
        char c0, c3;
        char p1, p2;

        p1 = pair.charAt(0);
        p2 = pair.charAt(1);

        counts.remove(encodePair(p1, p2));

        int k = 0;

        for (int i = 0; i < S.length; i++) {
            c1 = (char) S[i];

            if (i == S.length-1) {
                S1[k++] = (byte) c1;
                continue;
            }

            c2 = (char) S[i+1];

            if (c1 == p1 && c2 == p2) {
                S1[k++] = (byte) c;

                if (i > 0) {
                    c0 = (char) S[i-1];
                    counts.merge(encodePair(c0, c1), -1, Long::sum);
                    counts.merge(encodePair(c0, c), 1, Long::sum);
                }
                if (i+1 < S.length) {
                    c3 = (char) S[i+1];
                    counts.merge(encodePair(c2, c3), -1, Long::sum);
                    counts.merge(encodePair(c, c3), 1, Long::sum);
                }

                i++;
            }
            else
                S1[k++] = (byte) c1;
        }

        return Arrays.copyOf(S1, k);
    }

    private static int encodePair(char c1, char c2) {
        return (c1 << 16) + c2;
    }

    private static Integer findMaxPair(Int2LongAVLTreeMap counts) {
        Integer max_pair = null;
        long max_count = 1;

        for (Int2LongMap.Entry el: counts.int2LongEntrySet()) {
            long count = el.getLongValue();
            if (count > max_count) {
                max_pair = el.getIntKey();
                max_count = count;
            }
        }

        return max_pair;
    }

    private static String decodePair(int pair) {
        return (char)(pair >>> 16) + "" + (char)(pair & 0xFFFF);
    }

    private static String phrase(char token, Char2ObjectOpenHashMap<String> D) {
        String in_phrase = D.get(token);

        if (in_phrase == null)
            return token + "";

        StringBuilder out_phrase;
        String p;

        while (true) {
            boolean token_in_phrase = false;
            out_phrase = new StringBuilder();

            for (char c: in_phrase.toCharArray()) {
                p = D.get(c);

                if (p != null) {
                    out_phrase.append(p);
                    token_in_phrase = true;
                }
                else
                    out_phrase.append(c);
            }

            in_phrase = out_phrase.toString();

            if (!token_in_phrase)
                break;
        }

        return in_phrase;
    }
}
