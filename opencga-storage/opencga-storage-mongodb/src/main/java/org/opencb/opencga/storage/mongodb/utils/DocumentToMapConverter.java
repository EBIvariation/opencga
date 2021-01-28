package org.opencb.opencga.storage.mongodb.utils;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

public class DocumentToMapConverter {
    public static Map<String, Object> convertToMap(Document document) {
        Map<String, Object> resultMap = new HashMap<>();
        for (String key:document.keySet()) {
            resultMap.put(key, document.get(key));
        }
        return resultMap;
    }
}
