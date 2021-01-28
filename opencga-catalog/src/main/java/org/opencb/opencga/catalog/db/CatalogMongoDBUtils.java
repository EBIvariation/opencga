package org.opencb.opencga.catalog.db;

import com.mongodb.BasicDBObject;
import org.bson.Document;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;

/**
 * Created by imedina on 21/11/14.
 */
class CatalogMongoDBUtils {


    static int getNewAutoIncrementId(MongoDBCollection metaCollection) {
        return getNewAutoIncrementId("idCounter", metaCollection);
    }

    static int getNewAutoIncrementId(String field, MongoDBCollection metaCollection){
        QueryResult<Document> result = metaCollection.findAndModify(
                new Document("_id", CatalogMongoDBAdaptor.METADATA_OBJECT_ID),  //Query
                new Document(field, true),  //Fields
                null,
                new Document("$inc", new BasicDBObject(field, 1)), //Update
                new QueryOptions("returnNew", true),
                Document.class
        );
//        return (int) Float.parseFloat(result.getResult().get(0).get(field).toString());
        return result.getResult().get(0).getInteger(field);
    }

    static void checkUserExist(String userId, boolean exists, MongoDBCollection UserMongoDBCollection) throws CatalogDBException {
        if(userId == null) {
            throw new CatalogDBException("userId param is null");
        }
        if(userId.equals("")) {
            throw new CatalogDBException("userId is empty");
        }

    }

}
