package cxp.ingest;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Predicate;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by markmo on 8/05/15.
 */
public final class JsonPathUtils {

    public static <T> T evaluate(Object json, String jsonPath, Predicate... predicates) throws Exception {
//        if (json instanceof String) {
//            return JsonPath.read((String) json, jsonPath, predicates);
//        }
//        else if (json instanceof File) {
//            return JsonPath.read((File) json, jsonPath, predicates);
//        }
//        else if (json instanceof URL) {
//            return JsonPath.read((URL) json, jsonPath, predicates);
//        }
//        else if (json instanceof InputStream) {
//            return JsonPath.read((InputStream) json, jsonPath, predicates);
//        }
//        else {
            return JsonPath.read(json, jsonPath, predicates);
//        }
    }

    private JsonPathUtils() {
    }
}
