package org.opencb.opencga.storage.mongodb.utils;

import com.mongodb.BasicDBList;
import java.util.ArrayList;

public class ArrayToBasicDBListConverter {
    public static BasicDBList toBasicDBList(Object listObject) {
        if (listObject != null) {
            BasicDBList dbList = new BasicDBList();
            dbList.addAll((ArrayList) listObject);
            return dbList;
        }
        return null;
    }
}
