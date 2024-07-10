package sparkcompression;

import java.util.Arrays;

import scala.Serializable;

public class PatternTuple implements Serializable {

	private static final long serialVersionUID = -5568032122265611212L;

	private byte[] content;
	
	public PatternTuple(byte[] P1, byte A1, byte A2) {
		content = Arrays.copyOf(P1, P1.length + 2);
		
		content[P1.length] = A1;
		content[P1.length+1] = A2;
	}
	
	public byte[] getP() {
		return Arrays.copyOfRange(content, 0, content.length-2);
	}
	
	public byte getPf() {
		return content[0];
	}
	
	public byte[] getPm() {
		return Arrays.copyOfRange(content, 1, content.length-3);
	}
	
	public byte getPb() {
		return content[content.length-3];
	}
	
	public byte getA1() {
		return content[content.length-2];
	}
	
	public byte getA2() {
		return content[content.length-1];
	}
	
	@Override
	public String toString() {
		int[] pos = new int[content.length-2];
		
		for (int i = 0; i < content.length-2; i++)
			pos[i] = content[i] < 0 ? content[i] + 256 : content[i];
		
		return "(" + Arrays.toString(pos) + ", A1=" + getA1() + ", A2=" + getA2() + ")";
	}

	public byte[] getContent() {
		return content;
	}
}
