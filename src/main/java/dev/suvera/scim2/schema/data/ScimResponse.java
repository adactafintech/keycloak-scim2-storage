package dev.suvera.scim2.schema.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

/**
 * author: suvera
 * date: 10/18/2020 12:08 PM
 */
@SuppressWarnings("unused")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScimResponse {
    private int code;
    private String body;
    private Map<String, List<String>> headers;

    /**
     * Returns the last value corresponding to the specified field, or null.
     */
    public String header(String name) {
        if (headers != null && headers.containsKey(name) && !headers.get(name).isEmpty()) {
            return headers.get(name).get(headers.get(name).size() - 1);
        }
        return null;
    }

    public List<String> headers(String name) {
        if (headers == null) {
            return null;
        }
        return headers.get(name);
    }

    /** Header names are matched case-insensitively. */
    public static ScimResponse of(HttpResponse<String> response) {
        return new ScimResponse(
                response.statusCode(),
                response.body(),
                response.headers().map()
        );
    }
}
