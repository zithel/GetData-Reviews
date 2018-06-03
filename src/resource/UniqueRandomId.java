package resource;

import java.security.SecureRandom;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;


public class UniqueRandomId
{
    public String Generate()
    {
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }

    static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    static final String lower = upper.toLowerCase(Locale.ROOT);

    static final String digits = "0123456789";

    static final String alphanum = upper + lower + digits;

    final Random random;

    final char[] symbols;

    final char[] buf;

    public UniqueRandomId(int length, Random random, String symbols)
    {
        if (length < 1) throw new IllegalArgumentException();
        if (symbols.length() < 2) throw new IllegalArgumentException();
        this.random = Objects.requireNonNull(random);
        this.symbols = symbols.toCharArray();
        this.buf = new char[length];
    }

    // Create an alphanumeric string generator.
    public UniqueRandomId(int length, Random random)
    {
        this(length, random, alphanum);
    }

    // Create an alphanumeric strings from a secure generator.
    public UniqueRandomId(int length)
    {
        this(length, new SecureRandom());
    }

    // Create session identifiers.
    public UniqueRandomId()
    {
        this(21);
    }
}