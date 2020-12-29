package sparkannoy.spark;

import java.math.BigInteger;
import java.util.Random;

/**
 * validation purpose
 */
public class Kiss32Random extends Random {

    private long x;
    private long y;
    private long z;
    private long c;

    private static long uint32Mask = 0xffffffffL;
    private static BigInteger uint32MaskBigInteger = BigInteger.valueOf(0xffffffffL);
    private static BigInteger bigNumber = BigInteger.valueOf(698769069);

    public Kiss32Random() {
        this(123456789);
    }

    // seed must be != 0
    public Kiss32Random(long seed) {
        x = seed;
        y = 362436000;
        z = 521288629;
        c = 7654321;
    }

    @Override
    public int nextInt(int bound) {
        return (int)(kiss() % bound);
    }

    @Override
    public boolean nextBoolean() {
        return (kiss() & 1) == 1;
    }

    public long kiss() {
        // Linear congruence generator
        x = (69069 * x + 12345) & uint32Mask;

        // Xor shift
        y ^= (y << 13) & uint32Mask;
        y ^= (y >> 17) & uint32Mask;
        y ^= (y << 5) & uint32Mask;

        // Multiply-with-carry
        BigInteger t = bigNumber
                .multiply(BigInteger.valueOf(z))
                .add(BigInteger.valueOf(c));

        c = t.shiftRight(32).and(uint32MaskBigInteger).longValue();
        z = t.and(uint32MaskBigInteger).longValue();

        return (x + y + z) & uint32Mask;
    }

}






