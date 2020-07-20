package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoWriteException;
import com.mongodb.QueryBuilder;
import com.mongodb.bulk.BulkWriteResult;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Pattern;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptor implements VariantDBAdaptor {


    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private DocumentToVariantConverter variantConverter;
    private DocumentToVariantSourceEntryConverter variantSourceEntryConverter;
    private final String collectionName;
    private final VariantSourceMongoDBAdaptor variantSourceMongoDBAdaptor;

    private DataWriter dataWriter;

    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBAdaptor.class);

    public VariantMongoDBAdaptor(MongoCredentials credentials, String variantsCollectionName, String filesCollectionName)
            throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        variantSourceMongoDBAdaptor = new VariantSourceMongoDBAdaptor(credentials, filesCollectionName);

        collectionName = variantsCollectionName;
        
        // Converters from Document to Java classes
        // TODO Allow to configure depending on the type of study?
        variantSourceEntryConverter = new DocumentToVariantSourceEntryConverter(
                VariantStorageManager.IncludeSrc.FULL,
                new DocumentToSamplesConverter(credentials, filesCollectionName)
        );
        variantConverter = new DocumentToVariantConverter(variantSourceEntryConverter, new DocumentToVariantStatsConverter());
    }


    @Override
    public void setDataWriter(DataWriter dataWriter) {
        this.dataWriter = dataWriter;
    }

    @Override
    public void setConstantSamples(String sourceEntry) {
        List<String> samples = null;
        QueryResult samplesBySource = variantSourceMongoDBAdaptor.getSamplesBySource(sourceEntry, null);    // TODO jmmut: check when we remove fileId
        if(samplesBySource.getResult().isEmpty()) {
            logger.error("setConstantSamples(): couldn't find samples in source {} " + sourceEntry);
        } else {
            samples = (List<String>) samplesBySource.getResult().get(0);
        }
        
        variantSourceEntryConverter = new DocumentToVariantSourceEntryConverter(
                VariantStorageManager.IncludeSrc.FULL,
                new DocumentToSamplesConverter(samples)
        );
        
        variantConverter = new DocumentToVariantConverter(variantSourceEntryConverter, new DocumentToVariantStatsConverter());
    }

    @Override
    public QueryResult<Variant> getAllVariants(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        Document projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

        return coll.find(new Document(qb.get().toMap()), projection, variantConverter, options);
    }


    @Override
    public QueryResult<Variant> getVariantById(String id, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

//        Document query = new Document(DocumentToVariantConverter.ID_FIELD, id);

        if(options == null) {
            options = new QueryOptions(ID, id);
        } else {
            options.addToListOption(ID, id);
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        Document projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

//        return coll.find(query, options, variantConverter);
        QueryResult<Variant> queryResult = coll.find(new Document(qb.get().toMap()), projection, variantConverter, options);
        queryResult.setId(id);
        return queryResult;
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        List<QueryResult<Variant>> allResults = new ArrayList<>(idList.size());
        for (String r : idList) {
            QueryResult<Variant> queryResult = getVariantById(r, options);
            allResults.add(queryResult);
        }
        return allResults;
    }


    @Override
    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        getRegionFilter(region, qb);
        parseQueryOptions(options, qb);
        Document projection = parseProjectionQueryOptions(options);

        if (options == null) {
            options = new QueryOptions();
        }
        
        QueryResult<Variant> queryResult = coll.find(new Document(qb.get().toMap()), projection, variantConverter, options);
        queryResult.setId(region.toString());
        return queryResult;
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        List<QueryResult<Variant>> allResults;
        if (options == null) {
            options = new QueryOptions();
        }
        
        // If the users asks to sort the results, do it by chromosome and start
        if (options.getBoolean(SORT, false)) {
            options.put(SORT, new Document("chr", 1).append("start", 1));
        }
        
        // If the user asks to merge the results, run only one query,
        // otherwise delegate in the method to query regions one by one
        if (options.getBoolean(MERGE, false)) {
            options.add(REGION, regionList);
            allResults = Collections.singletonList(getAllVariants(options));
        } else {
            allResults = new ArrayList<>(regionList.size());
            for (Region r : regionList) {
                QueryResult queryResult = getAllVariantsByRegion(r, options);
                queryResult.setId(r.toString());
                allResults.add(queryResult);
            }
        }
        return allResults;
    }

    @Override
    public QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyId, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        // Aggregation for filtering when more than one study is present
        QueryBuilder qb = QueryBuilder.start(DocumentToVariantConverter.FILES_FIELD + "." + DocumentToVariantSourceEntryConverter.STUDYID_FIELD).in(studyId);
        getRegionFilter(region, qb);
        parseQueryOptions(options, qb);

        Document match = new Document("$match", qb.get());
        Document unwind = new Document("$unwind", "$" + DocumentToVariantConverter.FILES_FIELD);
        Document match2 = new Document("$match", 
                new Document(DocumentToVariantConverter.FILES_FIELD + "." + DocumentToVariantSourceEntryConverter.STUDYID_FIELD,
                                  new Document("$in", studyId)));

        logger.debug("Query to be executed {}", qb.get().toString());

        return coll.aggregate(/*"$variantsRegionStudies", */Arrays.asList(match, unwind, match2), options);
    }

    @Override
    public QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
        // db.variants.aggregate( { $match: { $and: [ {chr: "1"}, {start: {$gt: 251391, $lt: 2701391}} ] }}, 
        //                        { $group: { _id: { $subtract: [ { $divide: ["$start", 20000] }, { $divide: [{$mod: ["$start", 20000]}, 20000] } ] }, 
        //                                  totalCount: {$sum: 1}}})
        MongoDBCollection coll = db.getCollection(collectionName);

        if(options == null) {
            options = new QueryOptions();
        }

        int interval = options.getInt("interval", 20000);

        Document start = new Document("$gt", region.getStart());
        start.append("$lt", region.getEnd());

        BasicDBList andArr = new BasicDBList();
        andArr.add(new Document(DocumentToVariantConverter.CHROMOSOME_FIELD, region.getChromosome()));
        andArr.add(new Document(DocumentToVariantConverter.START_FIELD, start));

        // Parsing the rest of options
        QueryBuilder qb = new QueryBuilder();
        Document optionsMatch = new Document(parseQueryOptions(options, qb).get().toMap());
        if(!optionsMatch.keySet().isEmpty()) {
            andArr.add(optionsMatch);
        }
        Document match = new Document("$match", new Document("$and", andArr));


//        qb.and("_at.chunkIds").in(chunkIds);
//        qb.and(DocumentToVariantConverter.END_FIELD).greaterThanEquals(region.getStart());
//        qb.and(DocumentToVariantConverter.START_FIELD).lessThanEquals(region.getEnd());
//
//        List<String> chunkIds = getChunkIds(region);
//        Document regionObject = new Document("_at.chunkIds", new Document("$in", chunkIds))
//                .append(DocumentToVariantConverter.END_FIELD, new Document("$gte", region.getStart()))
//                .append(DocumentToVariantConverter.START_FIELD, new Document("$lte", region.getEnd()));


        BasicDBList divide1 = new BasicDBList();
        divide1.add("$start");
        divide1.add(interval);

        BasicDBList divide2 = new BasicDBList();
        divide2.add(new Document("$mod", divide1));
        divide2.add(interval);

        BasicDBList subtractList = new BasicDBList();
        subtractList.add(new Document("$divide", divide1));
        subtractList.add(new Document("$divide", divide2));


        Document substract = new Document("$subtract", subtractList);

        Document totalCount = new Document("$sum", 1);

        Document g = new Document("_id", substract);
        g.append("features_count", totalCount);
        Document group = new Document("$group", g);

        Document sort = new Document("$sort", new Document("_id", 1));

//        logger.info("getAllIntervalFrequencies - (>·_·)>");
//        System.out.println(options.toString());
//
//        System.out.println(match.toString());
//        System.out.println(group.toString());
//        System.out.println(sort.toString());

        long dbTimeStart = System.currentTimeMillis();
        QueryResult output = coll.aggregate(/*"$histogram", */Arrays.asList(match, group, sort), options);
        long dbTimeEnd = System.currentTimeMillis();

        Map<Long, Document> ids = new HashMap<>();
        // Create Document for intervals with features inside them
        for (Document intervalObj : (List<Document>) output.getResult()) {
            Long _id = Math.round((Double) intervalObj.get("_id"));//is double

            Document intervalVisited = ids.get(_id);
            if (intervalVisited == null) {
                intervalObj.put("_id", _id);
                intervalObj.put("start", getChunkStart(_id.intValue(), interval));
                intervalObj.put("end", getChunkEnd(_id.intValue(), interval));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", Math.log((int) intervalObj.get("features_count")));
                ids.put(_id, intervalObj);
            } else {
                Double sum = (Double) intervalVisited.get("features_count") + Math.log((int) intervalObj.get("features_count"));
                intervalVisited.put("features_count", sum.intValue());
            }
        }

        // Create Document for intervals without features inside them
        BasicDBList resultList = new BasicDBList();
        int firstChunkId = getChunkId(region.getStart(), interval);
        int lastChunkId = getChunkId(region.getEnd(), interval);
        Document intervalObj;
        for (int chunkId = firstChunkId; chunkId <= lastChunkId; chunkId++) {
            intervalObj = ids.get((long) chunkId);
            if (intervalObj == null) {
                intervalObj = new Document();
                intervalObj.put("_id", chunkId);
                intervalObj.put("start", getChunkStart(chunkId, interval));
                intervalObj.put("end", getChunkEnd(chunkId, interval));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", 0);
            }
            resultList.add(intervalObj);
        }

        QueryResult queryResult = new QueryResult(region.toString(), ((Long) (dbTimeEnd - dbTimeStart)).intValue(),
                resultList.size(), resultList.size(), null, null, resultList);

        return queryResult;
    }


    @Override
    public QueryResult getAllVariantsByGene(String geneName, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        if(options == null) {
            options = new QueryOptions(GENE, geneName);
        } else {
            options.addToListOption(GENE, geneName);
        }
        options.put(GENE, geneName);
        
        // If the users asks to sort the results, do it by chromosome and start
        if (options.getBoolean(SORT, false)) {
            options.put(SORT, new Document("chr", 1).append("start", 1));
        }
        
        parseQueryOptions(options, qb);
        Document projection = parseProjectionQueryOptions(options);
        QueryResult<Variant> queryResult = coll.find(new Document(qb.get().toMap()), projection, variantConverter, options);
        queryResult.setId(geneName);
        return queryResult;
    }


    @Override
    public QueryResult groupBy(String field, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        String documentPath;
        switch (field) {
            case "gene":
            default:
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." + DocumentToVariantAnnotationConverter.GENE_NAME_FIELD;
                break;
            case "ensemblGene":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." + DocumentToVariantAnnotationConverter.ENSEMBL_GENE_ID_FIELD;
                break;
            case "ct":
            case "consequence_type":
                documentPath = DocumentToVariantConverter.ANNOTATION_FIELD + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." + DocumentToVariantAnnotationConverter.SO_ACCESSION_FIELD;
                break;
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);

        Document match = new Document("$match", qb.get());
        Document project = new Document("$project", new Document("field", "$"+documentPath));
        Document unwind = new Document("$unwind", "$field");
        Document group = new Document("$group", new Document("_id", "$field")
//                .append("field", "$field")
                .append("count", new Document("$sum", 1))); // sum, count, avg, ...?
        Document sort = new Document("$sort", new Document("count", options != null ? options.getInt("order", -1) : -1)); // 1 = ascending, -1 = descending
        Document limit = new Document("$limit", options != null && options.getInt("limit", -1) > 0 ? options.getInt("limit") : 10);

        return coll.aggregate(Arrays.asList(match, project, unwind, group, sort, limit), options);
    }

    @Override
    public QueryResult getMostAffectedGenes(int numGenes, QueryOptions options) {
        return getGenesRanking(numGenes, -1, options);
    }

    @Override
    public QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options) {
        return getGenesRanking(numGenes, 1, options);
    }

    private QueryResult getGenesRanking(int numGenes, int order, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        options.put("limit", numGenes);
        options.put("order", order);

        return groupBy("gene", options);
    }


    @Override
    public QueryResult getTopConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        return getConsequenceTypesRanking(numConsequenceTypes, -1, options);
    }

    @Override
    public QueryResult getBottomConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        return getConsequenceTypesRanking(numConsequenceTypes, 1, options);
    }

    private QueryResult getConsequenceTypesRanking(int numConsequenceTypes, int order, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        options.put("limit", numConsequenceTypes);
        options.put("order", order);

        return groupBy("ct", options);
    }

    @Override
    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return variantSourceMongoDBAdaptor;
    }

    @Override
    public VariantDBIterator iterator() {
        MongoDBCollection coll = db.getCollection(collectionName);

        FindIterable<Document> dbCursor = coll.nativeQuery().find(new Document(), new QueryOptions());
        return new VariantMongoDBIterator(dbCursor, variantConverter);
    }

    @Override
    public VariantDBIterator iterator(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        Document projection = parseProjectionQueryOptions(options);
        FindIterable<Document> dbCursor = coll.nativeQuery().find(new Document(qb.get().toMap()), projection, options);
        return new VariantMongoDBIterator(dbCursor, variantConverter);
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {

        MongoCollection<Document> coll = db.getDb().getCollection(collectionName);
        List<WriteModel<Document>> documentsToBulkWrite = new ArrayList<>();

        long start = System.nanoTime();
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            String id = variantConverter.buildStorageId(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                    variantAnnotation.getReferenceAllele(), variantAnnotation.getAlternativeAllele());
            Document find = new Document("_id", id);
            DocumentToVariantAnnotationConverter converter = new DocumentToVariantAnnotationConverter();
            Document convertedVariantAnnotation = converter.convertToStorageType(variantAnnotation);
//            System.out.println("convertedVariantAnnotation = " + convertedVariantAnnotation);
            Document update = new Document("$set", new Document(DocumentToVariantConverter.ANNOTATION_FIELD,
                                                                          convertedVariantAnnotation));
            documentsToBulkWrite.add(new UpdateOneModel<>(find, update));
        }

        BulkWriteResult writeResult = null;
        try {
            writeResult = coll.bulkWrite(documentsToBulkWrite);
        }
        catch (MongoWriteException ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String stackTrace = sw.toString();
            logger.error("Error while executing bulk update ", stackTrace);
        }

        return new QueryResult<>("", ((int) (System.nanoTime() - start)), 1, 1, "", "", Collections.singletonList(writeResult));
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, QueryOptions queryOptions) {
        MongoCollection<Document> coll = db.getDb().getCollection(collectionName);
        List<WriteModel<Document>> pullDocumentsToBulkWrite = new ArrayList<>();
        List<WriteModel<Document>> pushDocumentsToBulkWrite = new ArrayList<>();

        long start = System.nanoTime();
        DocumentToVariantStatsConverter statsConverter = new DocumentToVariantStatsConverter();
        VariantSource variantSource = queryOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
        boolean overwrite = queryOptions.getBoolean(VariantStorageManager.OVERWRITE_STATS, false);

        // TODO make unset of 'st' if already present?
        for (VariantStatsWrapper wrapper : variantStatsWrappers) {
            Map<String, VariantStats> cohortStats = wrapper.getCohortStats();
            Iterator<VariantStats> iterator = cohortStats.values().iterator();
            VariantStats variantStats = iterator.hasNext()? iterator.next() : null;
            List<Document> cohorts = statsConverter.convertCohortsToStorageType(cohortStats, variantSource.getStudyId(), variantSource.getFileId());   // TODO jmmut: remove when we remove fileId
//            List cohorts = statsConverter.convertCohortsToStorageType(cohortStats, variantSource.getStudyId());   // TODO jmmut: use when we remove fileId

            // add cohorts, overwriting old values if that cid, fid and sid already exists: remove and then add
            // db.variants.update(
            //      {_id:<id>},
            //      {$pull:{st:{cid:{$in:["Cohort 1","cohort 2"]}, fid:{$in:["file 1", "file 2"]}, sid:{$in:["study 1", "study 2"]}}}}
            // )
            // db.variants.update(
            //      {_id:<id>},
            //      {$push:{st:{$each: [{cid:"Cohort 1", fid:"file 1", ... , value:3},{cid:"Cohort 2", ... , value:3}] }}}
            // )
            if (!cohorts.isEmpty()) {
                String id = variantConverter.buildStorageId(wrapper.getChromosome(), wrapper.getPosition(),
                        variantStats.getRefAllele(), variantStats.getAltAllele());

                Document find = new Document("_id", id);
                if (overwrite) {
                    List<Document> idsList = new ArrayList<>(cohorts.size());
                    for (Document cohort : cohorts) {
                        Document ids = new Document()
                                .append(DocumentToVariantStatsConverter.COHORT_ID, cohort.get(
                                        DocumentToVariantStatsConverter.COHORT_ID))
                                .append(DocumentToVariantStatsConverter.FILE_ID, cohort.get(
                                        DocumentToVariantStatsConverter.FILE_ID))
                                .append(DocumentToVariantStatsConverter.STUDY_ID, cohort.get(
                                        DocumentToVariantStatsConverter.STUDY_ID));
                        idsList.add(ids);
                    }
                    Document update = new Document("$pull",
                            new Document(DocumentToVariantConverter.STATS_FIELD,
                                              new Document("$or", idsList)));
                    pullDocumentsToBulkWrite.add(new UpdateOneModel<>(find, update));
                }
                Document push = new Document("$push",
                        new Document(DocumentToVariantConverter.STATS_FIELD,
                                          new Document("$each", cohorts)));
                pushDocumentsToBulkWrite.add(new UpdateOneModel<>(find, push));
            }
        }

        BulkWriteResult writeResult = null;
        try {
            // TODO handle if the variant didn't had that studyId in the files array
            if (overwrite) {
                coll.bulkWrite(pullDocumentsToBulkWrite);
            }

            writeResult = coll.bulkWrite(pushDocumentsToBulkWrite);
        }
        catch (MongoWriteException ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String stackTrace = sw.toString();
            logger.error("Error while executing bulk update ", stackTrace);
        }
        assert writeResult != null;
        int writes = writeResult.getModifiedCount();

        return new QueryResult<>("", ((int) (System.nanoTime() - start)), writes, writes, "", "",
                                 Collections.singletonList(writeResult));
    }

    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

    private QueryBuilder parseQueryOptions(QueryOptions options, QueryBuilder builder) {
        if (options != null) {

            /** GENOMIC REGION **/

            if (options.getString(ID) != null && !options.getString(ID).isEmpty()) { //) && !options.getString("id").isEmpty()) {
                List<String> ids = options.getAsStringList(ID);
                addQueryListFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DocumentToVariantAnnotationConverter.XREF_ID_FIELD
                        , ids, builder, QueryOperation.OR);

                addQueryListFilter(DocumentToVariantConverter.IDS_FIELD
                        , ids, builder, QueryOperation.OR);
            }

            if (options.containsKey(REGION) && !options.getString(REGION).isEmpty()) {
                List<String> stringList = options.getAsStringList(REGION);
                List<Region> regions = new ArrayList<>(stringList.size());
                for (String reg : stringList) {
                    Region region = Region.parseRegion(reg);
                    regions.add(region);
                }
                getRegionFilter(regions, builder);
            }

            if (options.containsKey(GENE)) {
                List<String> xrefs = options.getAsStringList(GENE);
                addQueryListFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DocumentToVariantAnnotationConverter.XREF_ID_FIELD
                        , xrefs, builder, QueryOperation.OR);
            }

            if (options.containsKey(CHROMOSOME)) {
                List<String> chromosome = options.getAsStringList(CHROMOSOME);
                addQueryListFilter(DocumentToVariantConverter.CHROMOSOME_FIELD
                        , chromosome, builder, QueryOperation.OR);
            }

            /** VARIANT **/

            if (options.containsKey(TYPE)) { // && !options.getString("type").isEmpty()) {
                addQueryStringFilter(DocumentToVariantConverter.TYPE_FIELD, options.getString(TYPE), builder);
            }

            if (options.containsKey(REFERENCE) && options.getString(REFERENCE) != null) {
                addQueryStringFilter(DocumentToVariantConverter.REFERENCE_FIELD, options.getString(REFERENCE), builder);
            }

            if (options.containsKey(ALTERNATE) && options.getString(ALTERNATE) != null) {
                addQueryStringFilter(DocumentToVariantConverter.ALTERNATE_FIELD, options.getString(ALTERNATE), builder);
            }

            /** ANNOTATION **/

            if (options.containsKey(ANNOTATION_EXISTS)) {
                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD).exists(options.getBoolean(ANNOTATION_EXISTS));
            }

            if (options.containsKey(ANNOT_XREF)) {
                List<String> xrefs = options.getAsStringList(ANNOT_XREF);
                addQueryListFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DocumentToVariantAnnotationConverter.XREF_ID_FIELD
                        , xrefs, builder, QueryOperation.AND);
            }

            if (options.containsKey(ANNOT_CONSEQUENCE_TYPE)) {
//                List<Integer> cts = getIntegersList(options.get(ANNOT_CONSEQUENCE_TYPE));
                List<String> cts = new ArrayList<>(options.getAsStringList(ANNOT_CONSEQUENCE_TYPE));
                List<Integer> ctsInteger = new ArrayList<>(cts.size());
                for (Iterator<String> iterator = cts.iterator(); iterator.hasNext(); ) {
                    String ct = iterator.next();
                    if (ct.startsWith("SO:")) {
                        ct = ct.substring(3);
                    }
                    try {
                        ctsInteger.add(Integer.parseInt(ct));
                    } catch (NumberFormatException e) {
                        logger.error("Error parsing integer ", e);
                        iterator.remove();  //Remove the malformed query params.
                    }
                }
                options.put(ANNOT_CONSEQUENCE_TYPE, cts); //Replace the QueryOption without the malformed query params
                addQueryListFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DocumentToVariantAnnotationConverter.SO_ACCESSION_FIELD
                        , ctsInteger, builder, QueryOperation.AND);
            }

            if (options.containsKey(ANNOT_BIOTYPE)) {
                List<String> biotypes = options.getAsStringList(ANNOT_BIOTYPE);
                addQueryListFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DocumentToVariantAnnotationConverter.BIOTYPE_FIELD
                        , biotypes, builder, QueryOperation.AND);
            }

            if (options.containsKey(POLYPHEN)) {
                addCompQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DocumentToVariantAnnotationConverter.POLYPHEN_FIELD + "." +
                        DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, options.getString(POLYPHEN), builder);
            }

            if (options.containsKey(SIFT)) {
                addCompQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DocumentToVariantAnnotationConverter.SIFT_FIELD + "." +
                        DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, options.getString(SIFT), builder);
            }

            if (options.containsKey(PROTEIN_SUBSTITUTION)) {
                List<String> list = new ArrayList<>(options.getAsStringList(PROTEIN_SUBSTITUTION));
                addScoreFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DocumentToVariantAnnotationConverter.PROTEIN_SUBSTITUTION_SCORE_FIELD, list, builder);
                options.put(PROTEIN_SUBSTITUTION, list); //Replace the QueryOption without the malformed query params
            }

            if (options.containsKey(CONSERVED_REGION)) {
                List<String> list = new ArrayList<>(options.getAsStringList(CONSERVED_REGION));
                addScoreFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "." +
                        DocumentToVariantAnnotationConverter.CONSERVED_REGION_SCORE_FIELD, list, builder);
                options.put(PROTEIN_SUBSTITUTION, list); //Replace the QueryOption without the malformed query params
            }

            /** STATS **/

            if (options.get(MAF) != null && !options.getString(MAF).isEmpty()) {
                addCompQueryFilter(
                        DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter.MAF_FIELD,
                        options.getString(MAF), builder);
            }

            if (options.get(MGF) != null && !options.getString(MGF).isEmpty()) {
                addCompQueryFilter(
                        DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter.MGF_FIELD,
                        options.getString(MGF), builder);
            }

            if (options.get(MISSING_ALLELES) != null && !options.getString(MISSING_ALLELES).isEmpty()) {
                addCompQueryFilter(
                        DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter.MISSALLELE_FIELD,
                        options.getString(MISSING_ALLELES), builder);
            }

            if (options.get(MISSING_GENOTYPES) != null && !options.getString(MISSING_GENOTYPES).isEmpty()) {
                addCompQueryFilter(
                        DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD,
                        options.getString(MISSING_GENOTYPES), builder);
            }

            if (options.get("numgt") != null && !options.getString("numgt").isEmpty()) {
                for (String numgt : options.getAsStringList("numgt")) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(
                            DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter.NUMGT_FIELD + "." + split[0],
                            split[1], builder);
                }
            }

//            if (options.get("freqgt") != null && !options.getString("freqgt").isEmpty()) {
//                for (String freqgt : getStringList(options.get("freqgt"))) {
//                    String[] split = freqgt.split(":");
//                    addCompQueryFilter(
//                            DocumentToVariantSourceEntryConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter.FREQGT_FIELD + "." + split[0],
//                            split[1], builder);
//                }
//            }


            /** FILES **/
            QueryBuilder fileBuilder = QueryBuilder.start();

            if (options.containsKey(STUDIES)) { // && !options.getList("studies").isEmpty() && !options.getListAs("studies", String.class).get(0).isEmpty()) {
                addQueryListFilter(
                        DocumentToVariantSourceEntryConverter.STUDYID_FIELD, options.getAsStringList(STUDIES),
                        fileBuilder, QueryOperation.AND);
            }

            if (options.containsKey(FILES)) { // && !options.getList("files").isEmpty() && !options.getListAs("files", String.class).get(0).isEmpty()) {
                addQueryListFilter(
                        DocumentToVariantSourceConverter.FILEID_FIELD, options.getAsStringList(FILES),
                        fileBuilder, QueryOperation.AND);
            }

            if (options.containsKey(GENOTYPE)) {
                String sampleGenotypesCSV = options.getString(GENOTYPE);

//                String AND = ",";
//                String OR = ";";
//                String IS = ":";

//                String AND = "AND";
//                String OR = "OR";
//                String IS = ":";

                String AND = ";";
                String OR = ",";
                String IS = ":";

                String[] sampleGenotypesArray = sampleGenotypesCSV.split(AND);
                for (String sampleGenotypes : sampleGenotypesArray) {
                    String[] sampleGenotype = sampleGenotypes.split(IS);
                    if(sampleGenotype.length != 2) {
                        continue;
                    }
                    int sample = Integer.parseInt(sampleGenotype[0]);
                    String[] genotypes = sampleGenotype[1].split(OR);
                    QueryBuilder genotypesBuilder = QueryBuilder.start();
                    for (String genotype : genotypes) {
                        String s = DocumentToVariantSourceEntryConverter.SAMPLES_FIELD + "." + genotype;
                        //or [ {"samp.0|0" : { $elemMatch : { $eq : <sampleId> } } } ]
                        genotypesBuilder.or(new BasicDBObject(s, new BasicDBObject("$elemMatch", new BasicDBObject("$eq", sample))));
                    }
                    fileBuilder.and(genotypesBuilder.get());
                }
            }

            DBObject fileQuery = fileBuilder.get();
            if (fileQuery.keySet().size() != 0) {
                builder.and(DocumentToVariantConverter.FILES_FIELD).elemMatch(fileQuery);
            }
        }

        logger.debug("Find = " + builder.get());
        return builder;
    }

    /**
     * when the tags "include" or "exclude" The names are the same as the members of Variant.
     * @param options
     * @return
     */
    private Document parseProjectionQueryOptions(QueryOptions options) {
        Document projection = new Document();

        if(options == null) {
            return projection;
        }

        List<String> includeList = options.getAsStringList("include");
        if (!includeList.isEmpty()) { //Include some
            for (String s : includeList) {
                String key = DocumentToVariantConverter.toShortFieldName(s);
                if (key != null) {
                    projection.put(key, 1);
                } else {
                    logger.warn("Unknown include field: {}", s);
                }
            }
        } else { //Include all
            for (String values : DocumentToVariantConverter.fieldsMap.values()) {
                projection.put(values, 1);
            }
            if (options.containsKey("exclude")) { // Exclude some
                List<String> excludeList = options.getAsStringList("exclude");
                for (String s : excludeList) {
                    String key = DocumentToVariantConverter.toShortFieldName(s);
                    if (key != null) {
                        projection.remove(key);
                    } else {
                        logger.warn("Unknown exclude field: {}", s);
                    }
                }
            }
        }

        if (options.containsKey(FILE_ID) && projection.containsKey(DocumentToVariantConverter.FILES_FIELD)) {
//            List<String> files = options.getListAs(FILES, String.class);
            String file = options.getString(FILE_ID);
            projection.put(
                    DocumentToVariantConverter.FILES_FIELD,
                    new Document(
                            "$elemMatch",
                            new Document(
                                    DocumentToVariantSourceEntryConverter.FILEID_FIELD,
                                    file
//                                    new Document(
//                                            "$in",
//                                            files
//                                    )
                            )
                    )
            );
        }

        logger.debug("Projection: {}", projection);
        return projection;
    }

    private enum QueryOperation {
        AND, OR
    }

    private QueryBuilder addQueryStringFilter(String key, String value, QueryBuilder builder) {
        if(value != null && !value.isEmpty()) {
            if(value.indexOf(",") == -1) {
                builder.and(key).is(value);
            }else {
                String[] values = value.split(",");
                builder.and(key).in(values);
            }
        }
        return builder;
    }

    private QueryBuilder addQueryListFilter(String key, List<?> values, QueryBuilder builder, QueryOperation op) {
        if (values != null)
            if (values.size() == 1) {
                if(op == QueryOperation.AND) {
                    builder.and(key).is(values.get(0));
                } else {
                    builder.or(QueryBuilder.start(key).is(values.get(0)).get());
                }
            } else if (!values.isEmpty()) {
                if(op == QueryOperation.AND) {
                    builder.and(key).in(values);
                } else {
                    builder.or(QueryBuilder.start(key).in(values).get());
                }
            }
        return builder;
    }

    private QueryBuilder addCompQueryFilter(String key, String value, QueryBuilder builder) {
        String op = value.substring(0, 2);
        op = op.replaceFirst("[0-9]", "");
        String obj = value.replaceFirst(op, "");

        switch(op) {
            case "<":
                builder.and(key).lessThan(Float.parseFloat(obj));
                break;
            case "<=":
                builder.and(key).lessThanEquals(Float.parseFloat(obj));
                break;
            case ">":
                builder.and(key).greaterThan(Float.parseFloat(obj));
                break;
            case ">=":
                builder.and(key).greaterThanEquals(Float.parseFloat(obj));
                break;
            case "=":
            case "==":
                builder.and(key).is(Float.parseFloat(obj));
                break;
            case "!=":
                builder.and(key).notEquals(Float.parseFloat(obj));
                break;
            case "~=":
                builder.and(key).regex(Pattern.compile(obj));
                break;
        }
        return builder;
    }

    private QueryBuilder addScoreFilter(String key, List<String> list, QueryBuilder builder) {
//        ArrayList<Document> and = new ArrayList<>(list.size());
//        Document[] ands = new Document[list.size()];
        List<Document> ands = new ArrayList<>();
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
            String elem = iterator.next();
            String[] split = elem.split(":");
            if (split.length == 2) {
                String source = split[0];
                String score = split[1];
                QueryBuilder scoreBuilder = new QueryBuilder();
                scoreBuilder.and(DocumentToVariantAnnotationConverter.SCORE_SOURCE_FIELD).is(source);
                addCompQueryFilter(DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD
                        , score, scoreBuilder);
//                builder.and(key).elemMatch(scoreBuilder.get());
                ands.add(new Document(key, new Document("$elemMatch", scoreBuilder.get())));
            } else {
                logger.error("Bad score filter: " + elem);
                iterator.remove(); //Remove the malformed query params.
            }
        }
        if (!ands.isEmpty()) {
            builder.and(ands.toArray(new DBObject[ands.size()]));
        }
        return builder;
    }

    private QueryBuilder getRegionFilter(Region region, QueryBuilder builder) {
        List<String> chunkIds = getChunkIds(region);
        builder.and("_at.chunkIds").in(chunkIds);
        builder.and(DocumentToVariantConverter.END_FIELD).greaterThanEquals(region.getStart());
        builder.and(DocumentToVariantConverter.START_FIELD).lessThanEquals(region.getEnd());
        return builder;
    }

    private QueryBuilder getRegionFilter(List<Region> regions, QueryBuilder builder) {
        DBObject[] objects = new DBObject[regions.size()];

        int i = 0;
        for (Region region : regions) {
            List<String> chunkIds = getChunkIds(region);
            DBObject regionObject = new BasicDBObject("_at.chunkIds", new BasicDBObject("$in", chunkIds))
                    .append(DocumentToVariantConverter.END_FIELD, new BasicDBObject("$gte", region.getStart()))
                    .append(DocumentToVariantConverter.START_FIELD, new BasicDBObject("$lte", region.getEnd()));
            objects[i] = regionObject;
            i++;
        }
        builder.or(objects);
        return builder;
    }

    /* *******************
     * Auxiliary methods *
     * *******************/

    private List<String> getChunkIds(Region region) {
        List<String> chunkIds = new LinkedList<>();

        int chunkSize = (region.getEnd() - region.getStart() > VariantMongoDBWriter.CHUNK_SIZE_BIG) ?
                VariantMongoDBWriter.CHUNK_SIZE_BIG : VariantMongoDBWriter.CHUNK_SIZE_SMALL;
        int ks = chunkSize / 1000;
        int chunkStart = region.getStart() / chunkSize;
        int chunkEnd = region.getEnd() / chunkSize;

        for (int i = chunkStart; i <= chunkEnd; i++) {
            String chunkId = region.getChromosome() + "_" + i + "_" + ks + "k";
            chunkIds.add(chunkId);
        }

        return chunkIds;
    }

    private int getChunkId(int position, int chunksize) {
        return position / chunksize;
    }

    private int getChunkStart(int id, int chunksize) {
        return (id == 0) ? 1 : id * chunksize;
    }

    private int getChunkEnd(int id, int chunksize) {
        return (id * chunksize) + chunksize - 1;
    }


}
