package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

/**
 * Created by jacobo on 9/01/15.
 */
public class VariantMongoDBIterator extends VariantDBIterator {

    private FindIterable<Document> dbCursor;
    private DocumentToVariantConverter documentToVariantConverter;

    VariantMongoDBIterator(FindIterable<Document> dbCursor, DocumentToVariantConverter documentToVariantConverter) { //Package protected
        this(dbCursor, documentToVariantConverter, 100);
    }

    VariantMongoDBIterator(FindIterable<Document> dbCursor, DocumentToVariantConverter documentToVariantConverter, int batchSize) { //Package protected
        this.dbCursor = dbCursor;
        this.documentToVariantConverter = documentToVariantConverter;
        if(batchSize > 0) {
            dbCursor.batchSize(batchSize);
        }
    }

    @Override
    public boolean hasNext() {
        return dbCursor.iterator().hasNext();
    }

    @Override
    public Variant next() {
        Document document = dbCursor.iterator().next();
        return documentToVariantConverter.convertToDataModelType(document);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException( "can't remove from a cursor" );
    }
}
