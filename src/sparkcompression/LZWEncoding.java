package sparkcompression;

import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;

public class LZWEncoding {
    public static final int R = 256;

    public static Tuple2<String[], byte[]> encode(String text) {
        HashMap<String, Character> dict = new HashMap<>();
        dict.put("A", 'A');
        dict.put("C", 'C');
        dict.put("G", 'G');
        dict.put("T", 'T');

        HashMap<Character, String> rev_dict = new HashMap<>();
        rev_dict.put('A', null);
        rev_dict.put('C', null);
        rev_dict.put('G', null);
        rev_dict.put('T', null);

        boolean[] characters = new boolean[R];

        characters['A'] = true;
        characters['C'] = true;
        characters['G'] = true;
        characters['T'] = true;

        char idx_newchar = 0;

        String buffer = "";
        byte[] output = new byte[text.length()];
        int len = 0;

        for (char c: text.toCharArray()) {
            if (dict.containsKey(buffer + c))
                buffer += c;
            else {
                output[len++] = (byte) dict.get(buffer).charValue();

                while (idx_newchar < characters.length && characters[idx_newchar++]) ;

                if (idx_newchar != characters.length) {
                    char new_char = (char) (idx_newchar - 1);
                    characters[c] = true;

                    dict.put(buffer + c, new_char);
                    rev_dict.put(new_char, buffer + c);
                }

                buffer = "" + c;
            }
        }

        String[] D_array = new String[R];

        D_array['A'] = "A";
        D_array['C'] = "C";
        D_array['G'] = "G";
        D_array['T'] = "T";

        for (char token : rev_dict.keySet())
            D_array[token] = phrase(token, rev_dict);

        return new Tuple2<>(D_array, Arrays.copyOf(output, len));
    }

    private static String phrase(char token, HashMap<Character, String> D) {
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
