package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBList;
import org.bson.Document;

import java.util.*;

import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.mongodb.utils.ArrayToBasicDBListConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 * @author Jose Miguel Mut <jmmut@ebi.ac.uk>
 *
 * Design policies:
 *  - IDS
 *      - The ids of a Variant will NOT be put in the Document. Using addToSet(ids) and
 *              setOnInsert(without ids) avoids overwriting ids.
 *      - In a Document, both an empty ids array or no ids property, converts to a Variant with an
 *              empty set of ids.
 */
public class DocumentToVariantConverter implements ComplexTypeConverter<Variant, Document> {

    public final static String CHROMOSOME_FIELD = "chr";
    public final static String START_FIELD = "start";
    public final static String END_FIELD = "end";
    public final static String LENGTH_FIELD = "len";
    public final static String REFERENCE_FIELD = "ref";
    public final static String ALTERNATE_FIELD = "alt";
//    public final static String ID_FIELD = "id";
    public final static String IDS_FIELD = "ids";
    
    public final static String HGVS_FIELD = "hgvs";
    public final static String TYPE_FIELD = "type";
    public final static String NAME_FIELD = "name";
    
    public final static String FILES_FIELD = "files";
    
    public final static String EFFECTS_FIELD = "effs";
    public final static String SOTERM_FIELD = "so";
    public final static String GENE_FIELD = "gene";
    public final static String ANNOTATION_FIELD = "annot";
    public final static String STATS_FIELD = "st";

    public final static Map<String, String> fieldsMap;

    static {
        fieldsMap = new HashMap<>();
        fieldsMap.put("chromosome", CHROMOSOME_FIELD);
        fieldsMap.put("start", START_FIELD);
        fieldsMap.put("end", END_FIELD);
        fieldsMap.put("length", LENGTH_FIELD);
        fieldsMap.put("reference", REFERENCE_FIELD);
        fieldsMap.put("alternative", ALTERNATE_FIELD);
//        fieldsMap.put("id", ID_FIELD);
        fieldsMap.put("ids", IDS_FIELD);
        fieldsMap.put("hgvs", HGVS_FIELD);
        fieldsMap.put("type", TYPE_FIELD);
//        fields.put("name", NAME_FIELD);
        fieldsMap.put("sourceEntries", FILES_FIELD);
        fieldsMap.put("sourceEntries.cohortStats", STATS_FIELD);
        fieldsMap.put("annotation", ANNOTATION_FIELD);
    }

    private DocumentToVariantSourceEntryConverter variantSourceEntryConverter;
    private DocumentToVariantAnnotationConverter variantAnnotationConverter;
    private DocumentToVariantStatsConverter statsConverter;

    /**
     * Create a converter between Variant and Document entities when there is 
     * no need to convert the files the variant was read from.
     */
    public DocumentToVariantConverter() {
        this(null, null);
    }

    /**
     * Create a converter between Variant and Document entities. A converter for 
     * the files the variant was read from can be provided in case those 
     * should be processed during the conversion.
     *
     * @param variantSourceEntryConverter The object used to convert the files
     * @param statsConverter
     */
    public DocumentToVariantConverter(DocumentToVariantSourceEntryConverter variantSourceEntryConverter, DocumentToVariantStatsConverter statsConverter) {
        this.variantSourceEntryConverter = variantSourceEntryConverter;
        this.variantAnnotationConverter = new DocumentToVariantAnnotationConverter();
        this.statsConverter = statsConverter;
    }
    
    
    @Override
    public Variant convertToDataModelType(Document object) {
        String chromosome = (String) object.get(CHROMOSOME_FIELD);
        int start = getDocumentNumericFieldAsInt(object.get(START_FIELD));
        int end = getDocumentNumericFieldAsInt(object.get(END_FIELD));
        String reference = (String) object.get(REFERENCE_FIELD);
        String alternate = (String) object.get(ALTERNATE_FIELD);
        Variant variant = new Variant(chromosome, start, end, reference, alternate);
        if (object.containsKey(IDS_FIELD)) {
            Object ids = object.get(IDS_FIELD);
            variant.setIds(new HashSet<String>(((Collection<String>) ids)));
        } else {
            variant.setIds(new HashSet<String>());
        }

        // Transform HGVS: List of map entries -> Map of lists
        BasicDBList mongoHgvs = ArrayToBasicDBListConverter.toBasicDBList(object.get(HGVS_FIELD));
        if (mongoHgvs != null) {
            for (Object o : mongoHgvs) {
                Document dbo = (Document) o;
                variant.addHgvs((String) dbo.get(TYPE_FIELD), (String) dbo.get(NAME_FIELD));
            }
        }

        // Files
        if (variantSourceEntryConverter != null) {
            BasicDBList mongoFiles = ArrayToBasicDBListConverter.toBasicDBList(object.get(FILES_FIELD));
            if (mongoFiles != null) {
                for (Object o : mongoFiles) {
                    Document dbo = (Document) o;
                    variant.addSourceEntry(variantSourceEntryConverter.convertToDataModelType(dbo));
                }
            }
        }

        // Annotations
        Document mongoAnnotation = (Document) object.get(ANNOTATION_FIELD);
        if (mongoAnnotation != null) {
            VariantAnnotation annotation = variantAnnotationConverter.convertToDataModelType(mongoAnnotation);
            annotation.setChromosome(variant.getChromosome());
            annotation.setAlternativeAllele(variant.getAlternate());
            annotation.setReferenceAllele(variant.getReference());
            annotation.setStart(variant.getStart());
            variant.setAnnotation(annotation);
        }

        // Statistics
        if (statsConverter != null && object.containsKey(STATS_FIELD)) {
            statsConverter.convertCohortsToDataModelType(object.get(STATS_FIELD), variant);
        }
        return variant;
    }

    private int getDocumentNumericFieldAsInt(Object field) {
        if (field instanceof Integer) {
            return (int)field;
        } else {
            return Math.toIntExact((long)field);
        }
    }

    @Override
    public Document convertToStorageType(Variant object) {
        // Attributes easily calculated
        Document mongoVariant = new Document("_id", buildStorageId(object))
//                .append(IDS_FIELD, object.getIds())    //Do not include IDs.
                .append(TYPE_FIELD, object.getType().name())
                .append(CHROMOSOME_FIELD, object.getChromosome())
                .append(START_FIELD, object.getStart())
                .append(END_FIELD, object.getEnd())
                .append(LENGTH_FIELD, object.getLength())
                .append(REFERENCE_FIELD, object.getReference())
                .append(ALTERNATE_FIELD, object.getAlternate());

        // Internal fields used for query optimization (dictionary named "_at")
        Document _at = new Document();
        mongoVariant.append("_at", _at);

        // ChunkID (1k and 10k)
        String chunkSmall = object.getChromosome() + "_" + object.getStart() / VariantMongoDBWriter.CHUNK_SIZE_SMALL + "_" + VariantMongoDBWriter.CHUNK_SIZE_SMALL / 1000 + "k";
        String chunkBig = object.getChromosome() + "_" + object.getStart() / VariantMongoDBWriter.CHUNK_SIZE_BIG + "_" + VariantMongoDBWriter.CHUNK_SIZE_BIG / 1000 + "k";
        BasicDBList chunkIds = new BasicDBList(); chunkIds.add(chunkSmall); chunkIds.add(chunkBig);
        _at.append("chunkIds", chunkIds);

        // Transform HGVS: Map of lists -> List of map entries
        BasicDBList hgvs = new BasicDBList();
        for (Map.Entry<String, Set<String>> entry : object.getHgvs().entrySet()) {
            for (String value : entry.getValue()) {
                hgvs.add(new Document(TYPE_FIELD, entry.getKey()).append(NAME_FIELD, value));
            }
        }
        mongoVariant.append(HGVS_FIELD, hgvs);

        // Files
        if (variantSourceEntryConverter != null) {
            BasicDBList mongoFiles = new BasicDBList();
            for (VariantSourceEntry archiveFile : object.getSourceEntries().values()) {
                mongoFiles.add(variantSourceEntryConverter.convertToStorageType(archiveFile));
            }
            mongoVariant.append(FILES_FIELD, mongoFiles);
        }
        
//        // Annotations
//        if (variantAnnotationConverter != null) {
//            if (object.getAnnotation() != null) {
//                Document annotation = variantAnnotationConverter.convertToStorageType(object.getAnnotation());
//                mongoVariant.append(ANNOTATION_FIELD, annotation);
//            }
//        }

        // Statistics
        if (statsConverter != null) {
            List mongoStats = statsConverter.convertCohortsToStorageType(object.getSourceEntries());
            mongoVariant.put(STATS_FIELD, mongoStats);
        }
        return mongoVariant;
    }

    public String buildStorageId(Variant v) {
        return buildStorageId(v.getChromosome(), v.getStart(), v.getReference(), v.getAlternate());
    }

    public String buildStorageId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome);
        builder.append("_");
        builder.append(start);
        builder.append("_");
        if (reference.equals("-")) {

        } else if (reference.length() < Variant.SV_THRESHOLD) {
            builder.append(reference);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(reference)));
        }
        
        builder.append("_");

        if (alternate.equals("-")) {

        } else if (alternate.length() < Variant.SV_THRESHOLD) {
            builder.append(alternate);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(alternate)));
        }
            
        return builder.toString();
    }


    public static String toShortFieldName(String longFieldName) {
        return fieldsMap.get(longFieldName);
    }

}
