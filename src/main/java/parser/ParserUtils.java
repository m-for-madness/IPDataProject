package parser;

public class ParserUtils {

    public static Long safeParseLong(String byteSym) {
        try {
            return Long.parseLong(byteSym);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}


