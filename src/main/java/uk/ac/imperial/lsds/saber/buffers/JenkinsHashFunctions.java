package uk.ac.imperial.lsds.saber.buffers;

public class JenkinsHashFunctions {
	
	private static final int rotate (int val, int bits) {
		
		return (val << bits) | (val >>> (32 - bits));
	}
	
	public static int hash (byte [] key, int initValue) {
		
		return hash (key, 0, key.length, initValue);
	}
	
	public static int hash (byte [] key, int offset, int length, int initValue) {
		
		int a, b, c;
		
		a = b = c = (0xdeadbeef + (length << 2) + initValue);
		
		/* Handle most of the key */
		
		int index = offset;
		
		while (length > 12) {
			
			a += key[index +  0];
			a += key[index +  1] <<  8;
			a += key[index +  2] << 16;
			a += key[index +  3] << 24;
			b += key[index +  4];
			b += key[index +  5] <<  8;
			b += key[index +  6] << 16;
			b += key[index +  7] << 24;
			c += key[index +  8];
			c += key[index +  9] <<  8;
			c += key[index + 10] << 16;
			c += key[index + 11] << 24;
			
			{ 
				/* mix(a, b, c); */
				a -= c; a ^= rotate(c,  4); c += b;
				b -= a; b ^= rotate(a,  6); a += c;
				c -= b; c ^= rotate(b,  8); b += a;
				a -= c; a ^= rotate(c, 16); c += b;
				b -= a; b ^= rotate(a, 19); a += c;
				c -= b; c ^= rotate(b,  4); b += a;
			}
			
			index  += 12;
			length -= 12;
		}
		
		switch (length) {
			case 12: c += key[index + 11] <<  24;
			case 11: c += key[index + 10] <<  16;
			case 10: c += key[index +  9] <<   8;
			case  9: c += key[index +  8];
			case  8: b += key[index +  7] <<  24;
			case  7: b += key[index +  6] <<  16;
			case  6: b += key[index +  5] <<   8;
			case  5: b += key[index +  4];
			case  4: a += key[index +  3] <<  24;
			case  3: a += key[index +  2] <<  16;
			case  2: a += key[index +  1] <<   8;
			case  1: a += key[index +  0]; break;
			case  0: return c;
		}
		
		{
			/* final(a, b, c); */
			c ^= b; c -= rotate(b,14);
			a ^= c; a -= rotate(c,11);
			b ^= a; b -= rotate(a,25);
			c ^= b; c -= rotate(b,16);
			a ^= c; a -= rotate(c, 4);
			b ^= a; b -= rotate(a,14);
			c ^= b; c -= rotate(b,24);
		}
		
		return c;
	}
}
