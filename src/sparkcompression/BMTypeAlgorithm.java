package sparkcompression;

import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class BMTypeAlgorithm {
    /*
        //Preprocessing for computing JumpBM(j, t), OutputBM(j, t), and Occ(t)
        Preprocess the pattern π and the dictionary D;
        // Main routine
        focus := an appropriate value;
        while focus ≤ n do begin
            Step 1: Report all pattern occurrences that are contained in the phrase S[focus].u by using Occ(t);
            Step 2: Find all pattern occurrences that end within the phrase S[focus].u by using JumpBM(j, t) and OutputBM(j, t);
            Step 3: Compute a possible shift ∆ based on information gathered in Step 2;
            focus := focus + ∆
        end
    */
    public static Integer[] run(String pattern, Tuple2<String[], byte[]> D_S) {
        ArrayList<Integer> occurrences = new ArrayList<>();

        String[] D = D_S._1;
        byte[] S = D_S._2;

        int[][] dfa = dfa(pattern, D.length);
        int n = S.length;
        int focus = init_focus(S, D, pattern);

        // only to consider real indexing
        int offset = 0;
        int prev_focus = 0;

        while (focus < n) {

            // Step 1 - Report all pattern occurrences that are contained in the phrase S[focus].u
            occurrences.addAll(occ(D[ubyte(S[focus])], pattern, offset));

            // Step 2 - Find all pattern occurrences that end within the phrase S[focus].u
            Tuple3<ArrayList<Integer>, Integer, Integer> occurrences_state_l = find_pattern_occurrences(focus, S, dfa, pattern, D, offset);

            // Step 3 - Compute a possible shift ∆ based on information gathered in Step 2
            if (occurrences_state_l != null) {
                int state = occurrences_state_l._2();
                int l = occurrences_state_l._3();

                occurrences.addAll(occurrences_state_l._1());
                focus += delta(state, D[ubyte(S[focus - l])], S, focus, pattern, D);
            }
            else {
                focus += delta(S, focus, pattern, D);
            }

            // only to consider real indexing
            for (int i = prev_focus; i < focus; i++)
                offset += D[ubyte(S[i])].length();

            prev_focus = focus;
        }

        return occurrences.toArray(new Integer[0]);
    }

    private static int init_focus(byte[] S, String[] D, String pattern) {
        int i = 0;
        int off = D[ubyte(S[i])].length();

        while (off < pattern.length())
            off += D[ubyte(S[++i])].length();

        return i;
    }

    private static ArrayList<Integer> occ(String tu, String p, int offset) {
        ArrayList<Integer> occurrences = new ArrayList<>();

        int i = 0;
        int occ = tu.indexOf(p, i);

        while(occ != -1) {
            occurrences.add(occ + offset);
            i = occ+1;
            occ = tu.indexOf(p, i);
        }

        return occurrences;
    }

    private static int delta(byte[] S, int focus, String p, String[] D) {
        int k = 1;
        int i = 1;

        if (focus + i >= S.length)
            return k;

        int shift_0 = shift(0, D[ubyte(S[focus])], p);
        int v1 = D[ubyte(S[focus + i])].length();

        while (focus + i + 1 < S.length && shift_0 > v1) {
            i++; k++;
            v1 += D[ubyte(S[focus + i])].length();
        }

        return k;
    }

    private static int delta(int j, String tu, byte[] S, int focus, String p, String[] D) {
        int k = 1;
        int i = 1;

        if (focus + i + 1 >= S.length)
            return k;

        int v1 = D[ubyte(S[focus + i])].length();
        int v2 = D[ubyte(S[focus + i+1])].length() +
                D[ubyte(S[focus + i])].length() -
                lpps(D[ubyte(S[focus])], p).length();

        int shift_0 = shift(0, D[ubyte(S[focus])], p);
        int shift_j = shift(j, tu, p);

        // todo (check): shift_0 and/or shift_j?
        while (focus + i + 1 < S.length && (shift_0 > v1 && shift_j > v2)) {
            i++; k++;
            v1 += D[ubyte(S[focus + i])].length();
            v2 += D[ubyte(S[focus + i])].length();
        }

        return k;
    }

    private static int shift(int j, String tu, String p) {
        return rightmost_occ(p, tu + p.substring(p.length() - j));
    }

    private static int rightmost_occ(String p, String tu) {
        return p.length() - (p.lastIndexOf(tu) + tu.length());
    }

    private static int output(int j, String p, String tu, int[][] dfa) {
        for (int i = 0; i < tu.length(); i++) {  // todo: i should be 1
            String w = tu.substring(i);

            if (g(j, w, dfa) == p.length())
                return w.length();
        }

        return 0;
    }

    private static Tuple3<ArrayList<Integer>, Integer, Integer> find_pattern_occurrences(int focus, byte[] S, int[][] dfa, String p, String[] D, int offset) {
        ArrayList<Integer> occurrences = new ArrayList<>();

        if (focus == 0)  // cannot be a partial matching
            return null;

        int state = jump(0, D[ubyte(S[focus])], dfa, p);

        // if a partial matching is not found or it is the first token, return
        if (state == -1)
            return null;

        int d = state;
        int l = 1;

        int final_state = -1;
        int final_l = -1;
        int suff_len = 0;

        int k = 0;

        while (d > 0) {

            // until partial matchings are found, analyze tokens backward
            // if partial matching in entirely in a token, manage it
            try {
                while (jump(state, D[ubyte(S[focus - l])], dfa, p) != -1) {
                    state = jump(state, D[ubyte(S[focus - l])], dfa, p);

                    if (state == p.length())
                        suff_len = D[ubyte(S[focus - l])].length();

                    l++;
                }
            }
            catch (StringIndexOutOfBoundsException exc) {
                throw new RuntimeException(focus + " " + l + " " + k + " " + D[ubyte(S[focus])]);
            }

            k++;

            // if the first token (last one analyzed) has the suffix equals to the pattern prefix, return
            if (suff_len == 0) {
                suff_len = output(state, p, D[ubyte(S[focus - l])], dfa);
            }
            else
                l--;

            if (suff_len > 0) {
                for (int i = focus - l; i < focus; i++)
                    offset -= D[ubyte(S[i])].length();

                occurrences.add(offset + (D[ubyte(S[focus - l])].length() - suff_len));
            }

            // todo (check): before or after setting the final_x?
            d -= state - f(state, p);
            state = f(state, p);

            // todo (check): set only at first iteration?
            if (final_l == -1) {
                final_state = state;
                final_l = l;
            }
        }

        return new Tuple3<>(occurrences, final_state, final_l);
    }

    private static int jump(int j, String tu, int[][] dfa, String pattern) {
        if (j != 0)
            return g(j, tu, dfa);

        String lpps = lpps(tu, pattern);
        if(!lpps.equals(""))
            return g(j, lpps, dfa);

        return -1;  // undefined
    }

    private static String lpps(String tu, String pattern) {
        String patt_suff = pattern.substring(1);

        while (!tu.startsWith(patt_suff)) {
            if (patt_suff.length() == 1)
                return "";

            patt_suff = patt_suff.substring(1);
        }

        return patt_suff;
    }

    private static int f(int j, String p) {
        if (j <= 1)
            return 0;

        int k = j-1;
        int m = p.length();
        String j_suffix = p.substring(m-j);
        String k_suffix = p.substring(m-k);

        while (k > 0 && !j_suffix.startsWith(k_suffix))
            k--;

        return k;
    }

    private static int g(int j, String tu, int[][] dfa) {
        return g(j, tu, 0, dfa);
    }

    private static int g(int j, String tu, int i, int[][] dfa) {
        if (i == tu.length())
            return j;

        int j1 = g(j, tu, i+1, dfa);

        if (j1 == -1)
            return j1;

        return g(j1, tu.charAt(i), dfa);
    }

    private static int g(int j, char c, int[][] dfa) {
        return dfa[c][j];
    }

    // DFA accepts the reverse pattern
    private static int[][] dfa(String pattern, int R) {
        int m = pattern.length();

        // build DFA from pattern
        int[][] dfa = new int[R][m+1]; // todo: check m+1

        for (int j = 0; j < m+1; j++)
            for (int c = 0; c < R; c++)
                dfa[c][j] = -1;

        dfa[pattern.charAt(m-1)][0] = 1;

        for (int j = 1; j < m; j++)
            dfa[pattern.charAt(m-1-j)][j] = j + 1;

        return dfa;
    }

    private static int ubyte(byte b) {
        return b & 0xFF;
    }
}
