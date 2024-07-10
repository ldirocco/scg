package sparkcompression;

import java.util.ArrayList;

public class BMAlgorithm {

	public static int bm_algo(String T, String P)
    {
		int n = T.length();
		int m = P.length();

        int i = m - 1;
        int j = m - 1;

        while(i < n) {

        	char t = T.charAt(i);
        	char p = P.charAt(j);

            if (t == p)
            {
                if (j == 0)
                	return i; // a match!
                else
                {
                    i--;
                    j--;
                }
            }
            else
            {
                i += m - Math.min(j, 1 + P.lastIndexOf(t));
                j = m - 1;
            }
        }

        return -1;
    }

	/*
	 * http://www.personal.kent.edu/~rmuhamma/Algorithms/MyAlgorithms/StringMatch/boyerMoore.htm
	 */
	public static int bm_algo(byte[] T, byte[] P)
    {
		int n = T.length;
		int m = P.length;

        int i = m - 1;
        int j = m - 1;

        while(i < n) {
        	byte t = T[i];
        	byte p = P[j];

			if (t == p) {
				if (j == 0)
					return i; // a match!
				else {
					i--;
					j--;
				}
			} else {
				i += m - Math.min(j, 1 + lastIndexOf(P, t));
				j = m - 1;
			}
		}

        return -1;
    }

	public static Integer[] bm_algo_multi(String T, String P)
	{
		int n = T.length();
		int m = P.length();

		int i = m - 1;
		int j = m - 1;

		ArrayList<Integer> indices = new ArrayList<>();

		while(i < n) {
			char t = T.charAt(i);
			char p = P.charAt(j);

			if (t == p) {
				if (j == 0) {
					// return i; // a match!
					indices.add(i);
					i += m - Math.min(j, 1 + P.lastIndexOf(t));
					j = m - 1;

				} else {
					i--;
					j--;
				}
			} else {
				i += m - Math.min(j, 1 + P.lastIndexOf(t));
				j = m - 1;
			}
		}

		return indices.toArray(new Integer[0]);
	}

	public static Integer[] bm_algo_multi(byte[] T, byte[] P)
	{
		int n = T.length;
		int m = P.length;

		int i = m - 1;
		int j = m - 1;
		ArrayList<Integer> indices = new ArrayList<>();

		while(i < n) {
			byte t = T[i];
			byte p = P[j];

			if (t == p) {
				if (j == 0) {
					// return i; // a match!
					indices.add(i);
					i += m - Math.min(j, 1 + lastIndexOf(P, t));
					j = m - 1;

				} else {
					i--;
					j--;
				}
			} else {
				i += m - Math.min(j, 1 + lastIndexOf(P, t));
				j = m - 1;
			}
		}

		return indices.toArray(new Integer[0]);
	}

	private static int nextCharIndex(String T, int i) {
		for (i += 1; i < T.length(); i++)
			if (Character.isAlphabetic(T.charAt(i)))
				return i;

		return -1;
	}

	private static int prevCharIndex(String T, int i) {
		for (i -= 1; i > -1; i--)
			if (Character.isAlphabetic(T.charAt(i)))
				return i;

		return -1;
	}

	private static int lastIndexOf(byte[] P, byte b) {
		for (int i = P.length-1; i > -1; i--)
			if (P[i] == b)
				return i;

		return -1;
	}
}
