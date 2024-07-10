package sparkcompression;

import java.util.Arrays;

public class TextTuple extends PatternTuple {

	public TextTuple(byte[] T1, byte A) {
		super(T1, (byte) 0, A);
	}

	public byte[] getT() {
		return getP();
	}
	
	public byte[] getTf() {
		return Arrays.copyOfRange(getContent(), 0, getContent().length-3);
	}
	
	public byte getTb() {
		return getPb();
	}
	
	public byte getA() {
		return getA2();
	}
}
