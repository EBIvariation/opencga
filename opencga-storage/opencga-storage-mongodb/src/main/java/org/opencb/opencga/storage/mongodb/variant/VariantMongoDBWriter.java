package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.annotation.VariantEffect;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.core.variant.io.VariantDBWriter;
import org.slf4j.LoggerFactory;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBWriter extends VariantDBWriter {

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
//    public static final Logger logger = Logger.getLogger(VariantMongoDBWriter.class.getName());
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(VariantDBWriter.class);

    private final VariantSource source;

    private MongoCredentials credentials;

    private MongoDataStore mongoDataStore;
    private MongoDataStoreManager mongoDataStoreManager;
    private MongoDBCollection variantMongoCollection;
    private MongoDBCollection filesMongoCollection;

    private String filesCollectionName;
    private String variantsCollectionName;

//    @Deprecated private MongoClient mongoClient;
    @Deprecated private MongoDatabase db;
    @Deprecated private MongoCollection<Document> filesCollection;
    @Deprecated private MongoCollection<Document> variantsCollection;

    @Deprecated private Map<String, Document> mongoMap;
    @Deprecated private Map<String, Document> mongoFileMap;


    private boolean includeStats;
    @Deprecated private boolean includeEffect;
    private VariantStorageManager.IncludeSrc includeSrc = VariantStorageManager.IncludeSrc.FULL;
    private boolean includeSamples;
    private boolean compressDefaultGenotype = true;
    private String defaultGenotype = null;

    private Map<String, Integer> samplesIds;
    @Deprecated private List<String> samples;
    @Deprecated private Map<String, Integer> conseqTypes;

    private DocumentToVariantConverter variantConverter;
    private DocumentToVariantStatsConverter statsConverter;
    private DocumentToVariantSourceConverter sourceConverter;
    private DocumentToVariantSourceEntryConverter sourceEntryConverter;
    private DocumentToSamplesConverter sampleConverter;

    private long numVariantsWritten;

    private List<WriteModel<Document>> documentsToBulkWrite = new ArrayList<>();
    private int currentBulkSize = 0;
    private int bulkSize = 0;

    private long checkExistsTime = 0;
    private long checkExistsDBTime = 0;
    private long bulkTime = 0;

    private AtomicBoolean variantSourceWritten = new AtomicBoolean(false);


    public VariantMongoDBWriter(final VariantSource source, MongoCredentials credentials) {
        this(source, credentials, "variants", "files");
    }

    public VariantMongoDBWriter(final VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection) {
        this(source, credentials, variantsCollection, filesCollection, false, false, false);
    }

    public VariantMongoDBWriter(final VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection,
                                boolean includeSamples, boolean includeStats, @Deprecated boolean includeEffect) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.source = source;
        this.credentials = credentials;
        this.filesCollectionName = filesCollection;
        this.variantsCollectionName = variantsCollection;

        this.mongoMap = new HashMap<>();
        this.mongoFileMap = new HashMap<>();

        this.includeSamples = includeSamples;
        this.includeStats = includeStats;
        this.includeEffect = includeEffect;

        conseqTypes = new LinkedHashMap<>();
        samples = new ArrayList<>();

        numVariantsWritten = 0;
    }

    @Override
    public boolean open() {
//        try {
//            // Mongo configuration
//            List<ServerAddress> serverAddresses = new LinkedList<>();
//            for (DataStoreServerAddress dataStoreServerAddress : credentials.getDataStoreServerAddresses()) {
//                serverAddresses.add(new ServerAddress(dataStoreServerAddress.getHost(), dataStoreServerAddress.getPort()));
//            }
//            if (credentials.getMongoCredentials() != null) {
//                mongoClient = new MongoClient(serverAddresses, Arrays.asList(credentials.getMongoCredentials()));
//            } else {
//                mongoClient = new MongoClient(serverAddresses);
//            }
//            db = mongoDataStore.getDb();
//        } catch (UnknownHostException ex) {
//            Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, null, ex);
//            return false;
//        }
        mongoDataStoreManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        mongoDataStore = mongoDataStoreManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        db = mongoDataStore.getDb();

        return mongoDataStore != null;
    }

    @Override
    public boolean pre() {
        // Mongo collection creation
        variantMongoCollection = mongoDataStore.getCollection(variantsCollectionName);
        filesMongoCollection = mongoDataStore.getCollection(filesCollectionName);

        filesCollection = db.getCollection(filesCollectionName);
        variantsCollection = db.getCollection(variantsCollectionName);
        variantSourceWritten.set(false);
        setConverters();

        resetBulk();

        return variantMongoCollection != null && filesMongoCollection != null;
    }

    @Override
    public boolean write(Variant variant) {
        return write(Collections.singletonList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
        return write_setOnInsert(data);
    }

    @Deprecated public boolean write_old(List<Variant> data) {
        buildBatchRaw(data);
        if (this.includeEffect) {
            buildEffectRaw(data);
        }
        buildBatchIndex(data);
        return writeBatch(data);
    }


    public boolean write_updateInsert(List<Variant> data) {
        List<String> variantIds = new ArrayList<>(data.size());

        Map<String, Variant> idVariantMap = new HashMap<>();
        Set<String> existingVariants = new HashSet<>();

        long start1 = System.nanoTime();
        for (Variant variant : data) {
            numVariantsWritten++;
            if(numVariantsWritten % 1000 == 0) {
                logger.info("Num variants written " + numVariantsWritten);
            }
            String id = variantConverter.buildStorageId(variant);
            variantIds.add(id);
            idVariantMap.put(id, variant);
        }

        long startQuery = System.nanoTime();
        Document query = new Document("_id", new Document("$in", variantIds));
        QueryResult<Document> idsResult =
                variantMongoCollection.find(query, new Document("_id", true), null);
        this.checkExistsDBTime += System.nanoTime() - startQuery;

        for (Document dbDocument : idsResult.getResult()) {
            String id = dbDocument.get("_id").toString();
            existingVariants.add(id);
        }
        this.checkExistsTime += System.nanoTime() - start1;



        for (Variant variant : data) {
            variant.setAnnotation(null);
            String id = variantConverter.buildStorageId(variant);

            if (!existingVariants.contains(id)) {
                Document variantDocument = variantConverter.convertToStorageType(variant);
                List<Document> mongoFiles = new LinkedList<>();
                for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                    if (!variantSourceEntry.getFileId().equals(source.getFileId())) {
                        continue;
                    }
                    Document mongoFile = sourceEntryConverter.convertToStorageType(variantSourceEntry);
                    mongoFiles.add(mongoFile);
                }
                variantDocument.put(DocumentToVariantConverter.FILES_FIELD, mongoFiles);
                this.documentsToBulkWrite.add(new InsertOneModel<>(variantDocument));
                currentBulkSize++;
            } else {
                for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                    if (!variantSourceEntry.getFileId().equals(source.getFileId())) {
                        continue;
                    }
//                    bulk.find(new Document("_id", id)).updateOne(new Document().append("$push",
//                                                                                                 new Document(DocumentToVariantConverter.FILES_FIELD, sourceEntryConverter.convertToStorageType(variantSourceEntry))));
                    Document filter = new Document("_id", id);
                    Document update = new Document().append("$push",
                                                            new Document(DocumentToVariantConverter.FILES_FIELD,
                                                                         sourceEntryConverter.convertToStorageType(
                                                                                 variantSourceEntry)));
                    this.documentsToBulkWrite.add(new UpdateOneModel(filter, update));
                    currentBulkSize++;
                }
            }
        }
        if (currentBulkSize >= bulkSize) {
            executeBulk();
        }

        return true;
    }

    public boolean write_setOnInsert(List<Variant> data) {
        numVariantsWritten += data.size();
        if(numVariantsWritten % 1000 == 0) {
            logger.info("Num variants written " + numVariantsWritten);
        }


        for (Variant variant : data) {
            variant.setAnnotation(null);
            String id = variantConverter.buildStorageId(variant);

            for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                if (!variantSourceEntry.getFileId().equals(source.getFileId())) {
                    continue;
                }

                // the chromosome and start appear just as shard keys, in an unsharded cluster they wouldn't be needed
                Document query = new Document("_id", id)
                        .append(DocumentToVariantConverter.CHROMOSOME_FIELD, variant.getChromosome())
                        .append(DocumentToVariantConverter.START_FIELD, variant.getStart());

                Document addToSet = new Document()
                        .append(DocumentToVariantConverter.FILES_FIELD,
                                sourceEntryConverter.convertToStorageType(variantSourceEntry));

                if (includeStats) {
                    List<Document> sourceEntryStats = statsConverter.convertCohortsToStorageType(variantSourceEntry.getCohortStats(),
                            variantSourceEntry.getStudyId(), variantSourceEntry.getFileId());
                    addToSet.put(DocumentToVariantConverter.STATS_FIELD, new Document("$each", sourceEntryStats));
                }

                if (variant.getIds() != null && !variant.getIds().isEmpty()) {
                    addToSet.put(DocumentToVariantConverter.IDS_FIELD, new Document("$each", variant.getIds()));
                }

                Document update = new Document()
                        .append("$addToSet", addToSet)
                        .append("$setOnInsert", variantConverter.convertToStorageType(variant));    // assuming variantConverter.statsConverter == null

                //bulk.find(query).upsert().updateOne(update);
                this.documentsToBulkWrite.add(new UpdateOneModel<>(query, update, new UpdateOptions().upsert(true)));

                currentBulkSize++;
            }

        }
        if (currentBulkSize >= bulkSize && currentBulkSize != 0) {
            executeBulk();
        }
        return true;
    }

    @Override @Deprecated
    protected boolean buildBatchRaw(List<Variant> data) {
        for (Variant v : data) {
            // Check if this variant is already stored
            String rowkey = variantConverter.buildStorageId(v);
            Document mongoVariant = new Document("_id", rowkey);

            if (variantsCollection.count(mongoVariant) == 0) {
                mongoVariant = variantConverter.convertToStorageType(v);
            } /*else {
                System.out.println("Variant " + v.getChromosome() + ":" + v.getStart() + "-" + v.getEnd() + " already found");
            }*/

            BasicDBList mongoFiles = new BasicDBList();
            for (VariantSourceEntry archiveFile : v.getSourceEntries().values()) {
                if (!archiveFile.getFileId().equals(source.getFileId())) {
                    continue;
                }

                if (this.includeSamples && samples.isEmpty() && archiveFile.getSamplesData().size() > 0) {
                    // First time a variant is loaded, the list of samples is populated.
                    // This guarantees that samples are loaded only once to keep order among variants,
                    // and that they are loaded before needed by the ArchivedVariantFileConverter
                    samples.addAll(archiveFile.getSampleNames());
                }

                Document mongoFile = sourceEntryConverter.convertToStorageType(archiveFile);
                mongoFiles.add(mongoFile);
                mongoFileMap.put(rowkey + "_" + archiveFile.getFileId(), mongoFile);
            }

            mongoVariant.put(DocumentToVariantConverter.FILES_FIELD, mongoFiles);
            mongoMap.put(rowkey, mongoVariant);
        }

        return true;
    }

    @Override @Deprecated
    protected boolean buildEffectRaw(List<Variant> variants) {
        for (Variant v : variants) {
            Document mongoVariant = mongoMap.get(variantConverter.buildStorageId(v));

            if (!mongoVariant.containsKey(DocumentToVariantConverter.CHROMOSOME_FIELD)) {
                // TODO It means that the same position was already found in this file, so __for now__ it won't be processed again
                continue;
            }

            Set<String> genesSet = new HashSet<>();
            Set<String> soSet = new HashSet<>();

            // Add effects to file
            if (!v.getAnnotation().getEffects().isEmpty()) {
                Set<Document> effectsSet = new HashSet<>();

                for (List<VariantEffect> effects : v.getAnnotation().getEffects().values()) {
                    for (VariantEffect effect : effects) {
                        Document object = getVariantEffectDocument(effect);
                        effectsSet.add(object);

                        addConsequenceType(effect);
                        soSet.addAll(Arrays.asList((String[]) object.get("so")));
                        if (object.containsKey("geneName")) {
                            genesSet.add(object.get("geneName").toString());
                        }
                    }
                }

                BasicDBList effectsList = new BasicDBList();
                effectsList.addAll(effectsSet);
                mongoVariant.put("effects", effectsList);
            }

            // Add gene fields directly to the variant, for query optimization purposes
            Document _at = (Document) mongoVariant.get("_at");
            if (!genesSet.isEmpty()) {
                BasicDBList genesList = new BasicDBList(); genesList.addAll(genesSet);
                _at.append("gn", genesList);
            }
            if (!soSet.isEmpty()) {
                BasicDBList soList = new BasicDBList(); soList.addAll(soSet);
                _at.append("ct", soList);
            }
        }

        return false;
    }

    @Deprecated
    private Document getVariantEffectDocument(VariantEffect effect) {
        String[] consequenceTypes = new String[effect.getConsequenceTypes().length];
        for (int i = 0; i < effect.getConsequenceTypes().length; i++) {
            consequenceTypes[i] = ConsequenceTypeMappings.accessionToTerm.get(effect.getConsequenceTypes()[i]);
        }

        Document object = new Document("so", consequenceTypes).append("featureId", effect.getFeatureId());
        if (effect.getGeneName() != null && !effect.getGeneName().isEmpty()) {
            object.append("geneName", effect.getGeneName());
        }
        return object;
    }

    @Override @Deprecated
    protected boolean buildBatchIndex(List<Variant> data) {
//        variantsCollection.createIndex(new Document("_at.chunkIds", 1));
//        variantsCollection.createIndex(new Document("_at.gn", 1));
//        variantsCollection.createIndex(new Document("_at.ct", 1));
//        variantsCollection.createIndex(new Document(DocumentToVariantConverter.ID_FIELD, 1));
//        variantsCollection.createIndex(new Document(DocumentToVariantConverter.CHROMOSOME_FIELD, 1));
//        variantsCollection.createIndex(new Document(DocumentToVariantConverter.FILES_FIELD + "." + DocumentToVariantSourceEntryConverter.STUDYID_FIELD, 1)
//                .append(DocumentToVariantConverter.FILES_FIELD + "." + DocumentToVariantSourceEntryConverter.FILEID_FIELD, 1));
        return true;
    }

    @Override @Deprecated
    protected boolean writeBatch(List<Variant> batch) {
        for (Variant v : batch) {
            String rowkey = variantConverter.buildStorageId(v);
            Document mongoVariant = mongoMap.get(rowkey);
            Document query = new Document("_id", rowkey);
            WriteResult wr;

            if (mongoVariant.containsKey(DocumentToVariantConverter.CHROMOSOME_FIELD)) {
                // Was fully built in this run because it didn't exist, and must be inserted
                try {
                    variantsCollection.insertOne(mongoVariant);
                } catch(MongoInternalException ex) {
                    System.out.println(v);
                    Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, v.getChromosome() + ":" + v.getStart(), ex);
                } catch(MongoWriteException ex) {
                    Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.WARNING,
                            "Variant already existed: {0}:{1}", new Object[]{v.getChromosome(), v.getStart()});
                } catch(MongoException ex) {
                    Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, ex.getMessage());
                }

            } else { // It existed previously, was not fully built in this run and only files need to be updated
                // TODO How to do this efficiently, inserting all files at once?
                for (VariantSourceEntry archiveFile : v.getSourceEntries().values()) {
                    Document mongoFile = mongoFileMap.get(rowkey + "_" + archiveFile.getFileId());
                    Document changes = new Document().append("$addToSet",
                            new Document(DocumentToVariantConverter.FILES_FIELD, mongoFile));
                    try {
                        variantsCollection.updateOne(query, changes, new UpdateOptions().upsert(true));
                    } catch (MongoException ex) {
                        Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, ex.getMessage());
                    }
                }
            }

        }

        mongoMap.clear();
        mongoFileMap.clear();

        numVariantsWritten += batch.size();
        Variant lastVariantInBatch = batch.get(batch.size()-1);
        Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.INFO, "{0}\tvariants written upto position {1}:{2}",
                new Object[]{numVariantsWritten, lastVariantInBatch.getChromosome(), lastVariantInBatch.getStart()});

        return true;
    }

    private boolean writeSourceSummary(VariantSource source) {
        if (!variantSourceWritten.getAndSet(true)) {
            Document studyMongo = sourceConverter.convertToStorageType(source);
            Document query = new Document(DocumentToVariantSourceConverter.FILEID_FIELD, source.getFileId());
            filesMongoCollection.update(query, studyMongo, new QueryOptions("upsert", true));
        }
        return true;
    }

    @Override
    public boolean post() {
        if (currentBulkSize != 0) {
            executeBulk();
        }
        logger.info("POST");
        writeSourceSummary(source);

        Document onBackground = new Document("background", true);
        variantMongoCollection.createIndex(new Document("_at.chunkIds", 1), onBackground);
        variantMongoCollection.createIndex(new Document("annot.xrefs.id", 1), onBackground);
        variantMongoCollection.createIndex(new Document("annot.ct.so", 1), onBackground);
        variantMongoCollection.createIndex(new Document(DocumentToVariantConverter.IDS_FIELD, 1), onBackground);
        variantMongoCollection.createIndex(new Document(DocumentToVariantConverter.CHROMOSOME_FIELD, 1), onBackground);
        variantMongoCollection.createIndex(
                new Document(DocumentToVariantConverter.FILES_FIELD + "." + DocumentToVariantSourceEntryConverter.STUDYID_FIELD, 1)
                        .append(DocumentToVariantConverter.FILES_FIELD + "." + DocumentToVariantSourceEntryConverter.FILEID_FIELD, 1), onBackground);
        variantMongoCollection.createIndex(new Document("st.maf", 1), onBackground);
        variantMongoCollection.createIndex(new Document("st.mgf", 1), onBackground);
        variantMongoCollection.createIndex(
                new Document(DocumentToVariantConverter.CHROMOSOME_FIELD, 1)
                        .append(DocumentToVariantConverter.START_FIELD, 1)
                        .append(DocumentToVariantConverter.END_FIELD, 1), onBackground);
        logger.debug("sent order to create indices");

        logger.debug("checkExistsTime " + checkExistsTime / 1000000.0 + "ms ");
        logger.debug("checkExistsDBTime " + checkExistsDBTime / 1000000.0 + "ms ");
        logger.debug("bulkTime " + bulkTime / 1000000.0 + "ms ");
        return true;
    }

    @Override
    public boolean close() {
        this.mongoDataStoreManager.close(this.mongoDataStore.getDb().getName());
        return true;
    }

    @Override
    public final void includeStats(boolean b) {
        includeStats = b;
    }

    public final void includeSrc(VariantStorageManager.IncludeSrc b) {
        includeSrc = b;
    }

    @Override
    public final void includeSamples(boolean b) {
        includeSamples = b;
    }

    @Override @Deprecated
    public final void includeEffect(boolean b) {
        includeEffect = b;
    }

    public void setCompressDefaultGenotype(boolean compressDefaultGenotype) {
        this.compressDefaultGenotype = compressDefaultGenotype;
    }

    public void setDefaultGenotype(String defaultGenotype) {
        this.defaultGenotype = defaultGenotype;
    }

    public void setThreadSyncronizationBoolean(AtomicBoolean atomicBoolean) {
        this.variantSourceWritten = atomicBoolean;
    }

    private void setConverters() {

        if (samplesIds == null || samplesIds.isEmpty()) {
            logger.info("Using sample position as sample id");
            samplesIds = source.getSamplesPosition();
        }

        sourceConverter = new DocumentToVariantSourceConverter();
        statsConverter = includeStats ? new DocumentToVariantStatsConverter() : null;
        sampleConverter = includeSamples ? new DocumentToSamplesConverter(compressDefaultGenotype, samplesIds): null; //TODO: Add default genotype

        sourceEntryConverter = new DocumentToVariantSourceEntryConverter(
                includeSrc,
                sampleConverter
        );
//        sourceEntryConverter.setIncludeSrc(includeSrc);

        // Do not create the VariantConverter with the sourceEntryConverter nor the statsconverter.
        // The variantSourceEntry and stats conversion will be done on demand to create a proper mongoDB update query.
        // variantConverter = new DocumentToVariantConverter(sourceEntryConverter);
        variantConverter = new DocumentToVariantConverter(null, null);
    }

    @Deprecated private void addConsequenceType(VariantEffect effect) {
        for (int so : effect.getConsequenceTypes()) {
            String ct = ConsequenceTypeMappings.accessionToTerm.get(so);
            int ctCount = conseqTypes.containsKey(ct) ? conseqTypes.get(ct)+1 : 1;
            conseqTypes.put(ct, ctCount);
        }
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    /**
     * This sample Ids will be used at conversion, to replace sample name for some numerical Id.
     * If this param is not provided, the variantSource.samplesPosition will be used instead.
     * @param samplesIds    Map between sampleName and sampleId
     */
    public void setSamplesIds(Map<String, Integer> samplesIds) {
        this.samplesIds = samplesIds;
    }


    private void executeBulk() {
        logger.debug("Execute bulk. BulkSize : " + currentBulkSize);
        long startBulk = System.nanoTime();
        try {
            this.variantsCollection.bulkWrite(documentsToBulkWrite);
        }
        catch (MongoWriteException ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String stackTrace = sw.toString();
            logger.error(stackTrace);
        }
        finally {
            resetBulk();
            this.bulkTime += System.nanoTime() - startBulk;
        }
    }

    private void resetBulk() {
        this.documentsToBulkWrite.clear();
        currentBulkSize = 0;
    }

}
