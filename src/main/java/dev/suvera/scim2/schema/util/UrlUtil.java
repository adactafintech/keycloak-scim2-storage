package dev.suvera.scim2.schema.util;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * author: suvera
 * date: 10/17/2020 2:48 PM
 */
public class UrlUtil {

    public static String urlEncode(Object str) {
        try {
            return URLEncoder.encode(str.toString(), StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            return str.toString();
        }
    }

    public static String queryString(Object params) {
        if (params == null) {
            return null;
        } else if (params instanceof Map) {
            return ((Map<?, ?>) params).entrySet().stream()
                    .map(p -> urlEncode(p.getKey()) + "=" + urlEncode(p.getValue()))
                    .reduce((p1, p2) -> p1 + "&" + p2)
                    .orElse("");
        } else {
            return params.toString();
        }
    }

    /**
     * Lenient URI parsing, like browsers and OkHttp: characters that are not legal in a URI (spaces, quotes,
     * non-ASCII, ...) are percent-encoded as UTF-8, while existing {@code %XX} escapes and legal characters are
     * kept as they are. {@link URI#create} would reject such URLs.
     */
    public static URI toUri(String url) {
        StringBuilder out = new StringBuilder(url.length() + 16);
        byte[] bytes = url.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xff;
            if (b == '%' && i + 2 < bytes.length && isHex(bytes[i + 1]) && isHex(bytes[i + 2])) {
                out.append('%');
            } else if (b < 0x80 && LEGAL.indexOf(b) >= 0) {
                out.append((char) b);
            } else {
                out.append('%').append(HEX[b >> 4]).append(HEX[b & 0xf]);
            }
        }
        return URI.create(out.toString());
    }

    private static final String LEGAL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            + "-._~" + ":/?#[]@" + "!$&'()*+,;=";
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private static boolean isHex(byte b) {
        return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F');
    }
}
