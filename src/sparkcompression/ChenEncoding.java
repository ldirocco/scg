package sparkcompression;

public class ChenEncoding
{
	public static TextTuple encode(byte[] T) {
		int n = T.length;
		byte A = (byte) (n % 4);
		byte[] T1 = new byte[(int) Math.ceil(n/4.0)];

		if (A > 0)
			A = (byte) (4 - A);

		for (int i = 0; i < n; i+=4)
			try { T1[i/4] = dnaToByte(T, i, Math.min(i+4, n)); }
			catch (Exception e) { e.printStackTrace(); }

		return new TextTuple(T1, A);
	}

	public static TextTuple encode(String T) {
		int n = T.length();
		byte A = (byte) (n % 4);
		byte[] T1 = new byte[(int) Math.ceil(n/4.0)];

		if (A > 0)
			A = (byte) (4 - A);

		for (int i = 0; i < n; i+=4)
			try { T1[i/4] = dnaToByte(T.substring(i, Math.min(i+4, n))); }
			catch (Exception e) { e.printStackTrace(); }

		return new TextTuple(T1, A);
	}

	public static String decode(TextTuple T1_A) {
		String T = "";
		byte[] T1 = T1_A.getT();

		for (int i = 0; i < T1.length-1; i++)
			T += byteToDna(T1[i], (byte) 0);

		T += byteToDna(T1[T1.length-1], T1_A.getA());

		return T;
	}

	private static byte dnaToByte(byte[] text, int from, int to) throws Exception {
		int value = 0;

		for (int i = from; i < to; i++) {
			switch (text[i]) {
				case 'A':
					value = (value << 2) + 0b00;
					break;
				case 'C':
					value = (value << 2) + 0b01;
					break;
				case 'T':
					value = (value << 2) + 0b10;
					break;
				case 'G':
					value = (value << 2) + 0b11;
					break;
				default:
					throw new Exception("No accepted value to encode: " + text[i]);
			}
		}

		for (int i = 0; i < 4 - (to - from); i++)
			value <<= 2;

		return (byte) value;
	}
	
	public static PatternTuple[] encodePatterns(String P) {
	    byte A1 = 0;
	    TextTuple P1_A2 = encode(P);

	    PatternTuple P1_A1_A2 = new PatternTuple(P1_A2.getP(), A1, P1_A2.getA2());
	    PatternTuple P2_A1_A2 = rightShiftPattern(P1_A1_A2);
	    PatternTuple P3_A1_A2 = rightShiftPattern(P2_A1_A2);
	    PatternTuple P4_A1_A2 = rightShiftPattern(P3_A1_A2);

	    PatternTuple[] patterns = new PatternTuple[] { P1_A1_A2, P2_A1_A2, P3_A1_A2, P4_A1_A2 };

	    return patterns;
	}

	private static PatternTuple rightShiftPattern(PatternTuple Pi_A1_A2) {
		byte[] Pi = Pi_A1_A2.getP();
		byte[] Pj;

		byte A1 = (byte) (Pi_A1_A2.getA1() + 1);
		byte A2 = (byte) (Pi_A1_A2.getA2() - 1);

		if (A2 < 0) {
			A2 = 3;
			Pj = new byte[Pi.length + 1];
			Pj[Pi.length] = (byte) ((Pi[Pi.length - 1] & 0b11) << 6);
		} else
			Pj = new byte[Pi.length];

		for (int i = Pi.length - 1; i > 0; i--)
			Pj[i] = (byte) ((Pi[i] >> 2 & 0b111111) + ((Pi[i - 1] & 0b11) << 6));

		Pj[0] = (byte) (Pi[0] >> 2 & 0b111111);

		return new PatternTuple(Pj, A1, A2);
	}
	
	private static byte dnaToByte(String quartet) throws Exception {
		int value = 0;
		
		for (byte c : quartet.getBytes())
			switch(c) {
				case 'A': value = (value << 2) + 0b00; break;
				case 'C': value = (value << 2) + 0b01; break;
				case 'T': value = (value << 2) + 0b10; break;
				case 'G': value = (value << 2) + 0b11; break;
				default: throw new Exception("No accepted value to encode: " + c);
			}
		
		for (int i = 0; i < 4 - quartet.length(); i++)
	        value <<= 2;
		
		return (byte) value;
	}

	private static String byteToDna(byte b, byte A) {
		String quartet = "";
		
		b >>>= 2*A;
		
		for (int i = 0; i < 4-A; i++)
			switch (b & 0b11) {
				case 0b00: quartet = "A" + quartet; b >>>= 2; break;
				case 0b01: quartet = "C" + quartet; b >>>= 2; break;
				case 0b10: quartet = "T" + quartet; b >>>= 2; break;
				case 0b11: quartet = "G" + quartet; b >>>= 2; break;
			}
		
		return quartet;
	}
}
