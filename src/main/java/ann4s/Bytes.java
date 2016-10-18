package ann4s;

public class Bytes {

    public static int bytes2int(byte[] b, int o) {
        return (b[o    ] & 0xff) << 24 |
               (b[o + 1] & 0xff) << 16 |
               (b[o + 2] & 0xff) <<  8 |
               (b[o + 3] & 0xff);
    }

    public static byte[] int2bytes(int i) {
        byte[] b = new byte[4];
        b[0] = (byte) ((i >> 24) & 0xff);
        b[1] = (byte) ((i >> 16) & 0xff);
        b[2] = (byte) ((i >>  8) & 0xff);
        b[3] = (byte) ( i        & 0xff);
        return b;
    }

    public static float bytes2float(byte[] b, int o) {
        return Float.intBitsToFloat(bytes2int(b, o));
    }

    public static byte[] float2bytes(float f) {
        return int2bytes(Float.floatToIntBits(f));
    }

    public static float[] bytes2floats(byte[] b, int o, int s) {
        float[] floats = new float[s];
        for (int j = 0; j < s; j ++) {
            floats[j] = bytes2float(b, o + j * 4);
        }
        return floats;
    }

    public static float[] bytes2floats(byte[] b, int o, float[] floats) {
        for (int j = 0; j < floats.length; j ++) {
            floats[j] = bytes2float(b, o + j * 4);
        }
        return floats;
    }

    public static byte[] floats2bytes(float[] f) {

        byte[] bytes = new byte[f.length * 4];
        for (int j = 0; j < f.length; j ++) {
            int i = Float.floatToIntBits(f[j]);
            int o = j * 4;
            bytes[o    ] = (byte) ((i >> 24) & 0xff);
            bytes[o + 1] = (byte) ((i >> 16) & 0xff);
            bytes[o + 2] = (byte) ((i >>  8) & 0xff);
            bytes[o + 3] = (byte) ( i        & 0xff);
        }
        return bytes;
    }

    public static byte[] ints2bytes(int[] i) {
        byte[] bytes = new byte[i.length * 4];
        for (int j = 0; j < i.length; j ++) {
            int o = j * 4;
            bytes[o    ] = (byte) ((i[j] >> 24) & 0xff);
            bytes[o + 1] = (byte) ((i[j] >> 16) & 0xff);
            bytes[o + 2] = (byte) ((i[j] >>  8) & 0xff);
            bytes[o + 3] = (byte) ( i[j]        & 0xff);
        }
        return bytes;
    }

    public static int[] bytes2ints(byte[] b, int o, int s) {
        if (s < 0) s = b.length / 4;
        int[] ints = new int[s];
        for (int j = 0; j < ints.length; j ++) {
            ints[j] = bytes2int(b, o + j * 4);
        }
        return ints;
    }

    public static float dot(byte[] a, int oa, byte[] b, int ob, int d) {
        int s = d * 4;
        float r = 0;
        for (int j = 0; j < s; j += 4) {
            r += Float.intBitsToFloat(((a[oa + j]) << 24) | ((a[oa + j + 1] & 0xff) << 16) | ((a[oa + j + 2] & 0xff) <<  8) | ((a[oa + j + 3] & 0xff))) *
                    Float.intBitsToFloat(((b[ob + j]) << 24) | ((b[ob + j + 1] & 0xff) << 16) | ((b[ob + j + 2] & 0xff) <<  8) | ((b[ob + j + 3] & 0xff)));
        }
        return r;
    }

//    public static void main(String[] args) {
//        float[] values = {0.1f, 0.2f, 0.3f};
//
//        float d = 0;
//        for (float v : values) {
//            d += v * v;
//        }
//        System.out.println(d);
//
//        byte[] bytes = floats2bytes(values);
//
//        System.out.println(dot(bytes, 0, bytes, 0, 3));
//
//        float[] converted = bytes2floats(bytes, 0, values.length);
//
//        for (float aConverted : converted) {
//            System.out.println(aConverted);
//        }
//    }
}
