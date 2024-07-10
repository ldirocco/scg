package sparkcompression;

import java.util.ArrayList;

import static sparkcompression.BMAlgorithm.bm_algo_multi;

public class BMChenAlgorithm {
    public static Integer[] run(TextTuple T1_A, PatternTuple[] Pi_A1_A2_list, boolean verbose) {
        ArrayList<Integer> indices_list = new ArrayList<>();

        for (PatternTuple Pi_A1_A2 : Pi_A1_A2_list) {
            byte[] Pm = Pi_A1_A2.getPm();
            byte[] Tf = T1_A.getTf();

            byte maskA1 = (byte) (0b11111111 >>> 2*Pi_A1_A2.getA1());
            byte maskA2 = (byte) (0b11111111 << 2*Pi_A1_A2.getA2());

            int m = Pm.length;
            int n = Tf.length;

            if (m == 0) { // the pattern is shorter than 9 characters (there is only Pf, Pi[0], and Pb, Pi[1])
                byte[] T1 = T1_A.getT();
                byte[] Pi = Pi_A1_A2.getP();

                for (int i = 0; i < T1.length-1; i++)
                    if ((T1[i] & maskA1) == Pi[0] && (T1[i+1] & maskA2) == Pi[1]) {
                        indices_list.add(4*i + Pi_A1_A2.getA1());
                    }

                continue;
            }

            Integer[] indices = bm_algo_multi(Tf, Pm);

            if (indices.length == 0) continue;

            if (verbose) System.out.println("Pm found");

            byte Pf = Pi_A1_A2.getPf();
            byte Pb = Pi_A1_A2.getPb();
            byte Tb = T1_A.getTb();

            for (int index : indices)
                // case 1: occurrence at the end of Tf
                if (index == n - m) {
                    if (Pf == (Tf[index-1] & maskA1)) {
                        if (verbose) System.out.println("Pf found (case 1)");

                        if(Pb == (Tb & maskA2)) {
                            if (verbose) System.out.println("Pb found (case 1)");
                            indices_list.add(4*(index-1) + Pi_A1_A2.getA1());
                        }
                    }
                }
                // case 2: occurrence in middle of Tf
                else if (index > 0 && index < n - m)
                    if (Pf == (Tf[index-1] & maskA1)) {
                        if (verbose) System.out.println("Pf found (case 2), Pf="+Pf+" Tf="+Tf[index-1]+" Tf_masked="+(Tf[index-1] & maskA1));

                        if(Pb == (Tf[index + m] & maskA2)) {
                            if (verbose) System.out.println("Pb found (case 2), Pb="+Pb+" Tf="+Tf[index+m]+" Tf_masked="+(Tf[index+m] & maskA2));
                            indices_list.add(4*(index-1) + Pi_A1_A2.getA1());
                        }
                    }
        }

        return indices_list.toArray(new Integer[0]);
    }
}
