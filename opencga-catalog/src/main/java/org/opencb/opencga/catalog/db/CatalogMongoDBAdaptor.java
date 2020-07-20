package org.opencb.opencga.catalog.db;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor extends CatalogDBAdaptor {

    private static final String USER_COLLECTION = "user";
    private static final String STUDY_COLLECTION = "study";
    private static final String FILE_COLLECTION = "file";
    private static final String JOB_COLLECTION = "job";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String METADATA_COLLECTION = "metadata";

    static final String METADATA_OBJECT_ID = "METADATA";

    //Keys to foreign objects.
    private static final String _ID = "_id";
    private static final String _PROJECT_ID = "_projectId";
    private static final String _STUDY_ID = "_studyId";
    private static final String FILTER_ROUTE_STUDIES = "projects.studies.";
    private static final String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    private static final String FILTER_ROUTE_FILES =   "projects.studies.files.";
    private static final String FILTER_ROUTE_JOBS =    "projects.studies.jobs.";

    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration configuration;
    private final String database;
    //    private final DataStoreServerAddress dataStoreServerAddress;
    private MongoDataStore db;

    private MongoDBCollection metaCollection;
    private MongoDBCollection userCollection;
    private MongoDBCollection studyCollection;
    private MongoDBCollection fileCollection;
    private MongoDBCollection sampleCollection;
    private MongoDBCollection jobCollection;

    //    private static final Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static ObjectReader jsonFileReader;
    private static ObjectReader jsonUserReader;
    private static ObjectReader jsonJobReader;
    private static ObjectReader jsonStudyReader;
    private static ObjectReader jsonSampleReader;
    private static Map<Class, ObjectReader> jsonReaderMap;

    static {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonReaderMap = new HashMap<>();
        jsonReaderMap.put(File.class, jsonFileReader = jsonObjectMapper.reader(File.class));
        jsonReaderMap.put(User.class, jsonUserReader = jsonObjectMapper.reader(User.class));
        jsonReaderMap.put(Job.class, jsonJobReader = jsonObjectMapper.reader(Job.class));
//        jsonProjectReader = jsonObjectMapper.reader(Project.class);
        jsonReaderMap.put(Study.class, jsonStudyReader = jsonObjectMapper.reader(Study.class));
//        jsonAnalysisReader = jsonObjectMapper.reader(Job.class);
        jsonReaderMap.put(Sample.class, jsonSampleReader = jsonObjectMapper.reader(Sample.class));
    }

    public CatalogMongoDBAdaptor(List<DataStoreServerAddress> dataStoreServerAddressList, MongoDBConfiguration configuration, String database)
            throws CatalogDBException {
        super();
        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddressList);
        this.configuration = configuration;
        this.database = database;

        logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);

        connect();
    }

    private void connect() throws CatalogDBException {
        db = mongoManager.get(database, configuration);
        if(db == null){
            throw new CatalogDBException("Unable to connect to MongoDB");
        }

        metaCollection = db.getCollection(METADATA_COLLECTION);
        userCollection = db.getCollection(USER_COLLECTION);
        studyCollection = db.getCollection(STUDY_COLLECTION);
        fileCollection = db.getCollection(FILE_COLLECTION);
        sampleCollection = db.getCollection(SAMPLE_COLLECTION);
        jobCollection = db.getCollection(JOB_COLLECTION);

        //If "metadata" document doesn't exist, create.
        QueryResult<Long> queryResult = metaCollection.count(new Document("_id", METADATA_OBJECT_ID));
        if(queryResult.getResult().get(0) == 0){
            try {
                Document metadataObject = getDocument(new Metadata(), "Metadata");
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject, null);
                insertUser(new User("admin", "admin", "admin@email.com", "admin", "opencb", User.Role.ADMIN, "active"), new QueryOptions());

            } catch (MongoWriteException e){
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            }
            //Set indexes
//            Document unique = new Document("unique", true);
//            nativeUserCollection.createIndex(new Document("id", 1), unique);
//            nativeFileCollection.createIndex(DocumentBuilder.start("studyId", 1).append("path", 1).get(), unique);
//            nativeJobCollection.createIndex(new Document("id", 1), unique);
        }
    }

    @Override
    public void disconnect(){
        mongoManager.close(db.getDatabaseName());
    }


    /**
     Auxiliary query methods
     */
    private int getNewId()  {return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);}
//    private int getNewProjectId()  {return CatalogMongoDBUtils.getNewAutoIncrementId("projectCounter", metaCollection);}
//    private int getNewStudyId()    {return CatalogMongoDBUtils.getNewAutoIncrementId("studyCounter", metaCollection);}
//    private int getNewFileId()     {return CatalogMongoDBUtils.getNewAutoIncrementId("fileCounter", metaCollection);}
//    //    private int getNewAnalysisId() {return CatalogMongoDBUtils.getNewAutoIncrementId("analysisCounter");}
//    private int getNewJobId()      {return CatalogMongoDBUtils.getNewAutoIncrementId("jobCounter", metaCollection);}
//    private int getNewToolId()      {return CatalogMongoDBUtils.getNewAutoIncrementId("toolCounter", metaCollection);}
//    private int getNewSampleId()   {return CatalogMongoDBUtils.getNewAutoIncrementId("sampleCounter", metaCollection);}


    private void checkParameter(Object param, String name) throws CatalogDBException {
        if (param == null) {
            throw new CatalogDBException("Error: parameter '" + name + "' is null");
        }
        if(param instanceof String) {
            if(param.equals("") || param.equals("null")) {
                throw new CatalogDBException("Error: parameter '" + name + "' is empty or it values 'null");
            }
        }
    }

    /** **************************
     * User methods
     * ***************************
     */

    @Override
    public boolean checkUserCredentials(String userId, String sessionId) {
        return false;
    }

    @Override
    public boolean userExists(String userId){
        QueryResult<Long> count = userCollection.count(new Document("id", userId));
        long l = count.getResult().get(0);
        return l != 0;
    }

    @Override
    public QueryResult<User> createUser(String userId, String userName, String email, String password,
                                        String organization, QueryOptions options) throws CatalogDBException {
        checkParameter(userId, "userId");
        long startTime = startQuery();

        if(userExists(userId)) {
            throw new CatalogDBException("User {id:\"" + userId + "\"} already exists");
        }
        return null;

    }

    @Override
    public QueryResult<User> insertUser(User user, QueryOptions options) throws CatalogDBException {
        checkParameter(user, "user");
        long startTime = startQuery();

        if(userExists(user.getId())) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        List<Project> projects = user.getProjects();
        user.setProjects(Collections.<Project>emptyList());
        user.setLastActivity(TimeUtils.getTimeMillis());
        Document userDocument = getDocument(user, "User " + user.getId());
        userDocument.put("_id", user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDocument, null);
        } catch (MongoWriteException e) {
            throw new CatalogDBException("User {id:\""+user.getId()+"\"} already exists");
        }

        String errorMsg = insert.getErrorMsg() != null ? insert.getErrorMsg() : "";
        for (Project p : projects) {
            String projectErrorMsg = createProject(user.getId(), p, options).getErrorMsg();
            if(projectErrorMsg != null && !projectErrorMsg.isEmpty()){
                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
            }
        }

        //Get the inserted user.
        user.setProjects(projects);
        List<User> result = getUser(user.getId(), options, "").getResult();

        return endQuery("insertUser", startTime, result, errorMsg, null);
    }

    /**
     * TODO: delete user from:
     *      project acl and owner
     *      study acl and owner
     *      file acl and creator
     *      job userid
     * also, delete his:
     *      projects
     *      studies
     *      analysesS
     *      jobs
     *      files
     */
    @Override
    public QueryResult<Long> deleteUser(String userId) throws CatalogDBException {
        checkParameter(userId, "userId");
        long startTime = startQuery();

//        WriteResult id = nativeUserCollection.remove(new Document("id", userId));
        DeleteResult wr = userCollection.remove(new Document("id", userId), null).getResult().get(0);
        if (wr.getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("User", userId);
        } else {
            return endQuery("Delete user", startTime, Arrays.asList(wr.getDeletedCount()));
        }
    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException {
        checkParameter(userId, "userId");
        checkParameter(password, "password");

        long startTime = startQuery();

        QueryResult<Long> count = userCollection.count(new Document("id", userId).append("password", password));
        if(count.getResult().get(0) == 0){
            throw new CatalogDBException("Bad user or password");
        } else {

            QueryResult<Long> countSessions = userCollection.count(new Document("sessions.id", session.getId()));
            if (countSessions.getResult().get(0) != 0) {
                throw new CatalogDBException("Already logged");
            } else {
                Document id = new Document("id", userId);
                Document updates = new Document(
                        "$push", new Document(
                        "sessions", getDocument(session, "Sesion")
                )
                );
                userCollection.update(id, updates, null);

                ObjectMap resultObjectMap = new ObjectMap();
                resultObjectMap.put("sessionId", session.getId());
                resultObjectMap.put("userId", userId);
                return endQuery("Login", startTime, Arrays.asList(resultObjectMap));
            }
        }
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogDBException {
        long startTime = startQuery();

        String userIdBySessionId = getUserIdBySessionId(sessionId);
        if(userIdBySessionId.isEmpty()){
            return endQuery("logout", startTime, null, "", "Session not found");
        }
        if(userIdBySessionId.equals(userId)){
            userCollection.update(
                    new Document("sessions.id", sessionId),
                    new Document("$set", new Document("sessions.$.logout", TimeUtils.getTime())),
                    null);

        } else {
            throw new CatalogDBException("UserId mismatches with the sessionId");
        }

        return endQuery("Logout", startTime);
    }

    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> countSessions = userCollection.count(new Document("sessions.id", session.getId()));
        if(countSessions.getResult().get(0) != 0){
            throw new CatalogDBException("Error, sessionID already exists");
        }
        String userId = "anonymous_" + session.getId();
        User user = new User(userId, "Anonymous", "", "", "", User.Role.ANONYMOUS, "");
        user.getSessions().add(session);
        Document anonymous = getDocument(user, "User");
        anonymous.put("_id", user.getId());

        try {
            userCollection.insert(anonymous, null);
        } catch (MongoWriteException e) {
            throw new CatalogDBException("Anonymous user {id:\""+user.getId()+"\"} already exists");
        }

        ObjectMap resultObjectMap = new ObjectMap();
        resultObjectMap.put("sessionId", session.getId());
        resultObjectMap.put("userId", userId);
        return endQuery("Login as anonymous", startTime, Arrays.asList(resultObjectMap));
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) throws CatalogDBException {
        long startTime = startQuery();
        String userId = "anonymous_" + sessionId;
        logout(userId, sessionId);
        deleteUser(userId);
        return endQuery("Logout anonymous", startTime);
    }

    @Override
    public QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogDBException {
        long startTime = startQuery();
        Document query = new Document("id", userId);
        query.put("lastActivity", new Document("$ne", lastActivity));
        QueryResult<Document> result = userCollection.find(query, options);
        User user = parseUser(result);
        if(user == null){
            throw CatalogDBException.idNotFound("User", userId);
        }
        joinFields(user, options);
        if(user.getLastActivity() != null && user.getLastActivity().equals(lastActivity)) { // TODO explain
            return endQuery("Get user", startTime);
        } else {
            return endQuery("Get user", startTime, Arrays.asList(user));
        }
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("id", userId);
        query.put("password", oldPassword);
        Document fields = new Document("password", newPassword);
        Document action = new Document("$set", fields);
        QueryResult<UpdateResult> update = userCollection.update(query, action, null);
        if(update.getResult().get(0).getMatchedCount() == 0){  //0 query matches.
            throw new CatalogDBException("Bad user or password");
        }
        return endQuery("Change Password", startTime, update);
    }

    @Override
    public QueryResult changeEmail(String userId, String newEmail) throws CatalogDBException {
        return modifyUser(userId, new ObjectMap("email", newEmail));
    }

    @Override
    public void updateUserLastActivity(String userId) throws CatalogDBException {
        modifyUser(userId, new ObjectMap("lastActivity", TimeUtils.getTimeMillis()));
    }

    @Override
    public QueryResult modifyUser(String userId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> userParameters = new HashMap<>();

        String[] acceptedParams = {"name", "email", "organization", "lastActivity", "role", "status"};
        filterStringParams(parameters, userParameters, acceptedParams);
        String[] acceptedIntParams = {"diskQuota", "diskUsage"};
        filterIntParams(parameters, userParameters, acceptedIntParams);

        String[] acceptedMapParams = {"attributes", "configs"};
        filterMapParams(parameters, userParameters, acceptedMapParams);

        if(!userParameters.isEmpty()) {
            QueryResult<UpdateResult> update = userCollection.update(
                    new Document("id", userId),
                    new Document("$set", userParameters), null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getMatchedCount() == 0){
                throw CatalogDBException.idNotFound("User", userId);
            }
        }

        return endQuery("Modify user", startTime);
    }

    @Override
    public QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("id", userId);
        query.put("email", email);
        Document fields = new Document("password", newCryptPass);
        Document action = new Document("$set", fields);
        QueryResult<UpdateResult> update = userCollection.update(query, action, null);
        if(update.getResult().get(0).getMatchedCount() == 0){  //0 query matches.
            throw new CatalogDBException("Bad user or email");
        }
        return endQuery("Reset Password", startTime, update);
    }

    @Override
    public QueryResult getSession(String userId, String sessionId) throws CatalogDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUserIdBySessionId(String sessionId){
        QueryResult id = userCollection.find(
                new Document("sessions.id", sessionId)
                        .append("sessions.logout", ""),
                new Document("id", true),
                null);

        if (id.getNumResults() != 0) {
            return (String) ((Document) id.getResult().get(0)).get("id");
        } else {
            return "";
        }
    }


    /**
     * Project methods
     * ***************************
     */

    @Override
    public boolean projectExists(int projectId) {
        QueryResult<Long> count = userCollection.count(new Document("projects.id", projectId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<Study> studies = project.getStudies();
        if(studies == null) {
            studies = Collections.emptyList();
        }
        project.setStudies(Collections.<Study>emptyList());


        // Check if project.alias already exists.
        Document countQuery = new Document("id", userId).append("projects.alias", project.getAlias());
        QueryResult<Long> count = userCollection.count(countQuery);
        if(count.getResult().get(0) != 0){
            throw new CatalogDBException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }
//        if(getProjectId(userId, project.getAlias()) >= 0){
//            throw new CatalogManagerException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
//        }

        //Generate json
        int projectId = getNewId();
        project.setId(projectId);
        Document query = new Document("id", userId);
        query.put("projects.alias", new Document("$ne", project.getAlias()));
        Document projectDocument = getDocument(project, "Project");
        Document update = new Document("$push", new Document ("projects", projectDocument));

        //Update object
        QueryResult<UpdateResult> queryResult = userCollection.update(query, update, null);

        if (queryResult.getResult().get(0).getModifiedCount() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }

        String errorMsg = "";
        for (Study study : studies) {
            String studyErrorMsg = createStudy(project.getId(), study, options).getErrorMsg();
            if(studyErrorMsg != null && !studyErrorMsg.isEmpty()){
                errorMsg += ", " + study.getAlias() + ":" + studyErrorMsg;
            }
        }
        List<Project> result = getProject(project.getId(), null).getResult();
        return endQuery("Create Project", startTime, result, errorMsg, null);
    }

    @Override
    public QueryResult<Project> getProject(int projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("projects.id", projectId);
        Document projection = new Document(
                "projects",
                new Document(
                        "$elemMatch",
                        new Document("id", projectId)
                )
        );
        QueryResult<Document> result = userCollection.find(query, projection, options);
        User user = parseUser(result);
        if(user == null || user.getProjects().isEmpty()) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }
        List<Project> projects = user.getProjects();
        joinFields(projects.get(0), options);

        return endQuery("Get project", startTime, projects);
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Long> deleteProject(int projectId) throws CatalogDBException {
        long startTime = startQuery();
        Document query = new Document("projects.id", projectId);
        Document pull = new Document("$pull",
                new Document("projects",
                        new Document("id", projectId)));

        QueryResult<UpdateResult> update = userCollection.update(query, pull, null);
        List<Long> deletes = new LinkedList<>();
        if (update.getResult().get(0).getMatchedCount() == 0) {
            throw CatalogDBException.idNotFound("Project", projectId);
        } else {
            deletes.add(update.getResult().get(0).getModifiedCount());
            return endQuery("delete project", startTime, deletes);
        }
    }

    @Override
    public QueryResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("id", userId);
        Document projection = new Document("projects", true);
        projection.put("_id", false);
        QueryResult<Document> result = userCollection.find(query, projection, options);

        User user = parseUser(result);
        List<Project> projects = user.getProjects();
        for (Project project : projects) {
            joinFields(project, options);
        }
        return endQuery(
                "User projects list", startTime,
                projects);
    }


    /**
     db.user.update(
     {
     "projects.id" : projectId,
     "projects.alias" : {
     $ne : newAlias
     }
     },
     {
     $set:{
     "projects.$.alias":newAlias
     }
     })
     */
    @Override
    public QueryResult renameProjectAlias(int projectId, String newProjectAlias) throws CatalogDBException {
        long startTime = startQuery();
//        String projectOwner = getProjectOwner(projectId);
//
//        int collisionProjectId = getProjectId(projectOwner, newProjectAlias);
//        if (collisionProjectId != -1) {
//            throw new CatalogManagerException("Couldn't rename project alias, alias already used in the same user");
//        }

        QueryResult<Project> projectResult = getProject(projectId, null); // if projectId doesn't exist, an exception is raised
        Project project = projectResult.getResult().get(0);

        //String oldAlias = project.getAlias();
        project.setAlias(newProjectAlias);

        // check that any other project in the user has the new name
        Document query = new Document("projects.id", projectId).append("projects.alias", new Document("$ne",
                                                                                                      newProjectAlias));
        Document update = new Document("$set",
                new Document("projects.$.alias", newProjectAlias));

        QueryResult<UpdateResult> result = userCollection.update(query, update, null);
        if (result.getResult().get(0).getModifiedCount() == 0) {    //Check if the the study has been inserted
            throw new CatalogDBException("Project {alias:\"" + newProjectAlias+ "\"} already exists");
        }
        return endQuery("rename project alias", startTime, result);
    }

    @Override
    public QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        if (!projectExists(projectId)) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }
        Document projectParameters = new Document();

        String[] acceptedParams = {"name", "creationDate", "description", "organization", "status", "lastActivity"};
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                projectParameters.put("projects.$."+s, parameters.getString(s));
            }
        }
        String[] acceptedIntParams = {"diskQuota", "diskUsage"};
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if(anInt != Integer.MIN_VALUE) {
                    projectParameters.put(s, anInt);
                }
            }
        }
        Map<String, Object> attributes = parameters.getMap("attributes");
        if(attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                projectParameters.put("projects.$.attributes."+entry.getKey(), entry.getValue());
            }
//            projectParameters.put("projects.$.attributes", attributes);
        }

        if(!projectParameters.isEmpty()) {
            Document query = new Document("projects.id", projectId);
            Document updates = new Document("$set", projectParameters);
            QueryResult<UpdateResult> updateResult = userCollection.update(query, updates, null);
            if(updateResult.getResult().get(0).getMatchedCount() == 0){
                throw CatalogDBException.idNotFound("Project", projectId);
            }
        }
        return endQuery("Modify project", startTime);
    }

    @Override
    public int getProjectId(String userId, String projectAlias) throws CatalogDBException {
        QueryResult<Document> queryResult = userCollection.find(
                new Document("projects.alias", projectAlias).append("id", userId),
                new Document("projects.id", true).append("projects",
                                                         new Document("$elemMatch", new Document("alias", projectAlias)
                                                         )),
                null
        );
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            return -1;
        } else {
            return user.getProjects().get(0).getId();
        }
    }

    @Override
    public String getProjectOwnerId(int projectId) throws CatalogDBException {
        Document query = new Document("projects.id", projectId);
        Document projection = new Document("id", "true");
        QueryResult<Document> result = userCollection.find(query, projection, null);

        if(result.getResult().isEmpty()){
            throw CatalogDBException.idNotFound("Project", projectId);
        } else {
            return result.getResult().get(0).get("id").toString();
        }
    }

    public Acl getFullProjectAcl(int projectId, String userId) throws CatalogDBException {
        QueryResult<Project> project = getProject(projectId, null);
        if (project.getNumResults() != 0) {
            List<Acl> acl = project.getResult().get(0).getAcl();
            for (Acl acl1 : acl) {
                if (userId.equals(acl1.getUserId())) {
                    return acl1;
                }
            }
        }
        return null;
    }
    /**
     * db.user.aggregate(
     * {"$match": {"projects.id": 2}},
     * {"$project": {"projects.acl":1, "projects.id":1}},
     * {"$unwind": "$projects"},
     * {"$match": {"projects.id": 2}},
     * {"$unwind": "$projects.acl"},
     * {"$match": {"projects.acl.userId": "jmmut"}}).pretty()
     */
    @Override
    public QueryResult<Acl> getProjectAcl(int projectId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        Document match1 = new Document("$match", new Document("projects.id", projectId));
        Document project = new Document("$project", new Document("_id", false).append("projects.acl", true)
                                                                              .append("projects.id", true));
        Document unwind1 = new Document("$unwind", "$projects");
        Document match2 = new Document("$match", new Document("projects.id", projectId));
        Document unwind2 = new Document("$unwind", "$projects.acl");
        Document match3 = new Document("$match", new Document("projects.acl.userId", userId));

        List<Document> operations = new LinkedList<>();
        operations.add(match1);
        operations.add(project);
        operations.add(unwind1);
        operations.add(match2);
        operations.add(unwind2);
        operations.add(match3);
        QueryResult aggregate = userCollection.aggregate(operations, null);

        List<Acl> acls = new LinkedList<>();
        if (aggregate.getNumResults() != 0) {
            Object aclObject = ((Document) ((Document) aggregate.getResult().get(0)).get("projects")).get("acl");
            Acl acl;
            try {
                acl = jsonObjectMapper.reader(Acl.class).readValue(aclObject.toString());
                acls.add(acl);
            } catch (IOException e) {
                throw new CatalogDBException("get Project ACL: error parsing ACL");
            }
        }
        return endQuery("get project ACL", startTime, acls);
    }

    @Override
    public QueryResult setProjectAcl(int projectId, Acl newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        Document newAclObject = getDocument(newAcl, "ACL");

        List<Acl> projectAcls = getProjectAcl(projectId, userId).getResult();
        Document query = new Document("projects.id", projectId);
        Document push = new Document("$push", new Document("projects.$.acl", newAclObject));
        if (!projectAcls.isEmpty()) {  // ensure that there is no acl for that user in that project. pull
            Document pull = new Document("$pull", new Document("projects.$.acl", new Document("userId", userId)));
            userCollection.update(query, pull, null);
        }
        //Put study
        QueryResult pushResult = userCollection.update(query, push, null);
        return endQuery("Set project acl", startTime, pushResult);
    }


//    public QueryResult<Project> searchProject(QueryOptions query, QueryOptions options) throws CatalogDBException {
//        long startTime = startQuery();
//
//
//        return endQuery("Search Proyect", startTime, projects);
//    }

    /**
     * Study methods
     * ***************************
     */

    @Override
    public boolean studyExists(int studyId) {
        QueryResult<Long> count = studyCollection.count(new Document("id", studyId));
        return count.getResult().get(0) != 0;
    }

    private void checkStudyId(int studyId) throws CatalogDBException {
        if(!studyExists(studyId)) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
    }

    private boolean studyAliasExists(int projectId, String studyAlias) {
        // Check if study.alias already exists.
        Document countQuery = new Document(_PROJECT_ID, projectId).append("alias", studyAlias);

        QueryResult<Long> queryResult = studyCollection.count(countQuery);
        return queryResult.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if(projectId < 0){
            throw CatalogDBException.idNotFound("Project", projectId);
        }

        // Check if study.alias already exists.
        if (studyAliasExists(projectId, study.getAlias())) {
            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }

        //Set new ID
        int newId = getNewId();
        study.setId(newId);

        //Empty nested fields
        List<File> files = study.getFiles();
        study.setFiles(Collections.<File>emptyList());

        List<Job> jobs = study.getJobs();
        study.setJobs(Collections.<Job>emptyList());

        //Create Document
        Document studyObject = getDocument(study, "Study");
        studyObject.put(_ID, newId);

        //Set ProjectId
        studyObject.put(_PROJECT_ID, projectId);

        //Insert
        QueryResult<WriteResult> updateResult = studyCollection.insert(studyObject, null);

        //Check if the the study has been inserted
//        if (updateResult.getResult().get(0).getN() == 0) {
//            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
//        }

        // Insert nested fields
        String errorMsg = updateResult.getErrorMsg() != null? updateResult.getErrorMsg() : "";

        for (File file : files) {
            String fileErrorMsg = createFileToStudy(study.getId(), file, options).getErrorMsg();
            if(fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg +=  file.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        for (Job job : jobs) {
//            String jobErrorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
            String jobErrorMsg = createJob(study.getId(), job, options).getErrorMsg();
            if(jobErrorMsg != null && !jobErrorMsg.isEmpty()){
                errorMsg += job.getName() + ":" + jobErrorMsg + ", ";
            }
        }

        List<Study> studyList = getStudy(study.getId(), options).getResult();
        return endQuery("Create Study", startTime, studyList, errorMsg, null);

    }

    @Override
    public QueryResult<Study> getAllStudies(int projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if(!projectExists(projectId)) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }

        Document query = new Document(_PROJECT_ID, projectId);

        QueryResult<Document> queryResult = studyCollection.find(query, filterOptions(options, FILTER_ROUTE_STUDIES));

        List<Study> studies = parseStudies(queryResult);
        for (Study study : studies) {
            joinFields(study, options);
        }
        return endQuery("Get all studies", startTime, studies);
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        //TODO: Parse QueryOptions include/exclude
        Document query = new Document("id", studyId);
        QueryResult result = studyCollection.find(query, filterOptions(options, FILTER_ROUTE_STUDIES));
//        QueryResult queryResult = endQuery("get study", startTime, result);

        List<Study> studies = parseStudies(result);
        if (studies.isEmpty()) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        joinFields(studies.get(0), options);

        //queryResult.setResult(studies);
        return endQuery("Get Study", startTime, studies);

    }

    @Override
    public QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException {
        //TODO
//        long startTime = startQuery();
//
//        QueryResult studyResult = getStudy(studyId, sessionId);
        return null;
    }

    @Override
    public void updateStudyLastActivity(int studyId) throws CatalogDBException {
        modifyStudy(studyId, new ObjectMap("lastActivity", TimeUtils.getTime()));
    }

    @Override
    public QueryResult modifyStudy(int studyId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        checkStudyId(studyId);
        Document studyParameters = new Document();

        String[] acceptedParams = {"name", "creationDate", "creationId", "description", "status", "lastActivity", "cipher"};
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, parameters, acceptedLongParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

        if(parameters.containsKey("type")) {
            Study.Type type = parameters.get("type", Study.Type.class);
            studyParameters.put("type", type);
        }

        if(parameters.containsKey("uri")) {
            URI uri = parameters.get("uri", URI.class);
            studyParameters.put("uri", uri.toString());
        }

        if(!studyParameters.isEmpty()) {
            Document query = new Document("id", studyId);
            Document updates = new Document("$set", studyParameters);
            QueryResult<UpdateResult> updateResult = studyCollection.update(query, updates, null);
            if(updateResult.getResult().get(0).getMatchedCount() == 0){
                throw CatalogDBException.idNotFound("Study", studyId);
            }
        }
        return endQuery("Modify study", startTime, Collections.singletonList(new ObjectMap(studyParameters)));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Long> deleteStudy(int studyId) throws CatalogDBException {
        long startTime = startQuery();
        Document query = new Document("id", studyId);
        QueryResult<DeleteResult> remove = studyCollection.remove(query, null);

        List<Long> deletes = new LinkedList<>();

        if (remove.getResult().get(0).getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        } else {
            deletes.add(remove.getResult().get(0).getDeletedCount());
            return endQuery("delete study", startTime, deletes);
        }
    }

    @Override
    public int getStudyId(int projectId, String studyAlias) throws CatalogDBException {
        Document query = new Document(_PROJECT_ID, projectId).append("alias", studyAlias);
        Document projection = new Document("id", "true");
        QueryResult<Document> queryResult = studyCollection.find(query, projection, null);
        List<Study> studies = parseStudies(queryResult);
        return studies == null || studies.isEmpty() ? -1 : studies.get(0).getId();
    }

    @Override
    public int getProjectIdByStudyId(int studyId) throws CatalogDBException {
        Document query = new Document("id", studyId);
        Document projection = new Document(_PROJECT_ID, "true");
        QueryResult<Document> result = studyCollection.find(query, projection, null);

        if (!result.getResult().isEmpty()) {
            Document study = result.getResult().get(0);
            return Integer.parseInt(study.get(_PROJECT_ID).toString());
        } else {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
    }

    @Override
    public String getStudyOwnerId(int studyId) throws CatalogDBException {
        int projectId = getProjectIdByStudyId(studyId);
        return getProjectOwnerId(projectId);
    }

    @Override
    public QueryResult<Acl> getStudyAcl(int studyId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        Document query = new Document("id", studyId);
        Document projection = new Document("acl", new Document("$elemMatch", new Document("userId", userId)));
        QueryResult<Document> documentQueryResult = studyCollection.find(query, projection, null);
        List<Study> studies = parseStudies(documentQueryResult);
        if(studies.isEmpty()) {
            throw CatalogDBException.idNotFound("Study", studyId);
        } else {
            List<Acl> acl = studies.get(0).getAcl();
            return endQuery("getStudyAcl", startTime, acl);
        }
    }

    @Override
    public QueryResult setStudyAcl(int studyId, Acl newAcl) throws CatalogDBException {
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        Document newAclObject = getDocument(newAcl, "ACL");

        Document query = new Document("id", studyId);
        Document pull = new Document("$pull", new Document("acl", new Document("userId", newAcl.getUserId())));
        Document push = new Document("$push", new Document("acl", newAclObject));
        studyCollection.update(query, pull, null);
        studyCollection.update(query, push, null);

        return getStudyAcl(studyId, userId);
    }


    /**
     * File methods
     * ***************************
     */

    private boolean filePathExists(int studyId, String path) {
        Document query = new Document(_STUDY_ID, studyId);
        query.put("path", path);
        QueryResult<Long> count = fileCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<File> createFileToStudy(int studyId, File file, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        String ownerId = getStudyOwnerId(studyId);
        if(ownerId == null || ownerId.isEmpty()) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        if(filePathExists(studyId, file.getPath())){
            throw new CatalogDBException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
        }

        //new File Id
        int newFileId = getNewId();
        file.setId(newFileId);
        if(file.getOwnerId() == null) {
            file.setOwnerId(ownerId);
        }
        Document fileDocument = getDocument(file, "File");
        fileDocument.put(_STUDY_ID, studyId);
        fileDocument.put(_ID, newFileId);

        try {
            fileCollection.insert(fileDocument, null);
        } catch (MongoWriteException e) {
            throw new CatalogDBException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
        }

        return endQuery("Create file", startTime, getFile(newFileId, options));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Long> deleteFile(int fileId) throws CatalogDBException {
        long startTime = startQuery();

        DeleteResult id = fileCollection.remove(new Document("id", fileId), null).getResult().get(0);
        List<Long> deletes = new LinkedList<>();
        if(id.getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        } else {
            deletes.add(id.getDeletedCount());
            return endQuery("delete file", startTime, deletes);
        }
    }

    @Override
    public int getFileId(int studyId, String path) throws CatalogDBException {

        Document query = new Document(_STUDY_ID, studyId).append("path", path);
        Document projection = new Document("id", true);
        QueryResult<Document> queryResult = fileCollection.find(query, projection, null);
        File file = parseFile(queryResult);
        return file != null ? file.getId() : -1;
    }

    @Override
    public QueryResult<File> getAllFiles(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Document> queryResult = fileCollection.find( new Document(_STUDY_ID, studyId), filterOptions(options, FILTER_ROUTE_FILES));
        List<File> files = parseFiles(queryResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Document> folderResult = fileCollection.find( new Document("id", folderId), filterOptions(options, FILTER_ROUTE_FILES));

        File folder = parseFile(folderResult);
        if (!folder.getType().equals(File.Type.FOLDER)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Object studyId = folderResult.getResult().get(0).get(_STUDY_ID);

        Document query = new Document(_STUDY_ID, studyId);
        query.put("path", new Document("$regex", "^" + folder.getPath() + "[^/]+/?$"));
        QueryResult<Document> filesResult = fileCollection.find(query, null);
        List<File> files = parseFiles(filesResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Document> queryResult = fileCollection.find( new Document("id", fileId), options);

        File file = parseFile(queryResult);
        if(file != null) {
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public QueryResult setFileStatus(int fileId, File.Status status) throws CatalogDBException {
        long startTime = startQuery();
        return endQuery("Set file status", startTime, modifyFile(fileId, new ObjectMap("status", status.toString())));
    }

    @Override
    public QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {"type", "format", "bioformat", "uriScheme", "description", "status"};
        filterStringParams(parameters, fileParameters, acceptedParams);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, fileParameters, acceptedLongParams);

        String[] acceptedIntParams = {"jobId"};
        filterIntParams(parameters, fileParameters, acceptedIntParams);

        String[] acceptedIntegerListParams = {"sampleIds"};
        filterIntegerListParams(parameters, fileParameters, acceptedIntegerListParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, fileParameters, acceptedMapParams);

        if(!fileParameters.isEmpty()) {
            QueryResult<UpdateResult> update = fileCollection.update(new Document("id", fileId),
                    new Document("$set", fileParameters), null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getMatchedCount() == 0){
                throw CatalogDBException.idNotFound("File", fileId);
            }
        }

        return endQuery("Modify file", startTime);
    }


    /**
     * @param filePath assuming 'pathRelativeToStudy + name'
     */
    @Override
    public QueryResult<UpdateResult> renameFile(int fileId, String filePath) throws CatalogDBException {
        long startTime = startQuery();

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        File file = getFile(fileId, null).getResult().get(0);

        int studyId = getStudyIdByFileId(fileId);
        int collisionFileId = getFileId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        if (file.getType().equals(File.Type.FOLDER)) {  // recursive over the files inside folder
            QueryResult<File> allFilesInFolder = getAllFilesInFolder(fileId, null);
            String oldPath = file.getPath();
            filePath += filePath.endsWith("/")? "" : "/";
            for (File subFile : allFilesInFolder.getResult()) {
                String replacedPath = subFile.getPath().replace(oldPath, filePath);
                renameFile(subFile.getId(), replacedPath); // first part of the path in the subfiles 3
            }
        }
        Document query = new Document("id", fileId);
        Document set = new Document("$set", new Document("name", fileName).append("path", filePath));
        QueryResult<UpdateResult> update = fileCollection.update(query, set, null);
        if (update.getResult().isEmpty() || update.getResult().get(0).getMatchedCount() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return endQuery("rename file", startTime, update);
    }


    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogDBException {
        Document query = new Document("id", fileId);
        Document projection = new Document(_STUDY_ID, "true");
        QueryResult<Document> result = fileCollection.find(query, projection, null);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get(_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public String getFileOwnerId(int fileId) throws CatalogDBException {
        QueryResult<File> fileQueryResult = getFile(fileId);
        if(fileQueryResult == null || fileQueryResult.getResult() == null || fileQueryResult.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return fileQueryResult.getResult().get(0).getOwnerId();
//        int studyId = getStudyIdByFileId(fileId);
//        return getStudyOwnerId(studyId);
    }

    private long getDiskUsageByStudy(int studyId){
        List<Document> operations = Arrays.<Document>asList(
                new Document(
                        "$match",
                        new Document(
                                _STUDY_ID,
                                studyId
                                //new Document("$in",studyIds)
                        )
                ),
                new Document(
                        "$group",
                        new Document("_id", "$" + _STUDY_ID)
                                .append("diskUsage",
                                        new Document(
                                                "$sum",
                                                "$diskUsage"
                                        ))
                )
        );
        QueryResult<Document> aggregate = fileCollection.aggregate(operations, null);
        if(aggregate.getNumResults() == 1){
            Object diskUsage = aggregate.getResult().get(0).get("diskUsage");
            if(diskUsage instanceof Integer){
                return ((Integer) diskUsage).longValue();
            } else if (diskUsage instanceof Long) {
                return ((Long) diskUsage);
            } else {
                return Long.parseLong(diskUsage.toString());
            }
        } else {
            return 0;
        }
    }

    /**
     * query: db.file.find({id:2}, {acl:{$elemMatch:{userId:"jcoll"}}, studyId:1})
     */
    @Override
    public QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        Document projection = new Document("acl",
                                           new Document("$elemMatch", new Document("userId", userId)))
                .append("_id", false);

        QueryResult queryResult = fileCollection.find(new Document("id", fileId), projection, null);
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("getFileAcl: There is no file with fileId = " + fileId);
        }
        List<Acl> acl = parseFile(queryResult).getAcl();
        return endQuery("get file acl", startTime, acl);
    }

    @Override
    public QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        Document newAclObject = getDocument(newAcl, "ACL");

        List<Acl> aclList = getFileAcl(fileId, userId).getResult();
        Document match;
        Document updateOperation;
        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
            match = new Document("id", fileId);
            updateOperation = new Document("$push", new Document("acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            match = new Document("id", fileId).append("acl.userId", userId);
            updateOperation = new Document("$set", new Document("acl.$", newAclObject));
        }
        QueryResult update = fileCollection.update(match, updateOperation, null);
        return endQuery("set file acl", startTime);
    }

    public QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBList filters = new BasicDBList();
        Document mongoQuery = new Document();

        if(query.containsKey("id")){
            addQueryIntegerListFilter("id", query, "_id", mongoQuery);
        }
        if(query.containsKey("studyId")){
            addQueryIntegerListFilter("studyId", query, _STUDY_ID, mongoQuery);
        }
        if(query.containsKey("name")){
            addQueryStringListFilter("name", query, mongoQuery);
        }
        if(query.containsKey("type")){
            addQueryStringListFilter("type", query, mongoQuery);
        }
        if(query.containsKey("path")){
            addQueryStringListFilter("path", query, mongoQuery);
        }
        if(query.containsKey("bioformat")){
            addQueryStringListFilter("bioformat", query, mongoQuery);
        }
        if(query.containsKey("status")){
            addQueryStringListFilter("status", query, mongoQuery);
        }
        if(query.containsKey("maxSize")){
            mongoQuery.put("size", new Document("$lt", query.getInt("maxSize")));
        }
        if(query.containsKey("minSize")){
            mongoQuery.put("size", new Document("$gt", query.getInt("minSize")));
        }
        if(query.containsKey("startDate")){
            mongoQuery.put("creationDate", new Document("$lt", query.getString("startDate")));
        }
        if(query.containsKey("endDate")){
            mongoQuery.put("creationDate", new Document("$gt", query.getString("endDate")));
        }
        if(query.containsKey("like")){
            mongoQuery.put("name", new Document("$regex", query.getString("like")));
        }
        if (query.containsKey("startsWith")){
            mongoQuery.put("name", new Document("$regex", "^" + query.getString("startsWith")));
        }
        if (query.containsKey("directory")){
            mongoQuery.put("path", new Document("$regex", "^" + query.getString("directory") + "[^/]+/?$"));
        }

//        Document query = new Document("$and", filters);
//        QueryResult<Document> queryResult = fileCollection.find(query, null);

        QueryResult<Document> queryResult = fileCollection.find(mongoQuery, filterOptions(options, FILTER_ROUTE_FILES));

        List<File> files = parseFiles(queryResult);

        return endQuery("Search File", startTime, files);
    }

    @Override
    public QueryResult<Dataset> createDataset(int studyId, Dataset dataset, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        checkStudyId(studyId);

        QueryResult<Long> count = studyCollection.count(new Document(_ID, studyId)
                .append("datasets.name", dataset.getName()));

        if(count.getResult().get(0) > 0) {
            throw new CatalogDBException("Dataset { name: \"" + dataset.getName() + "\" } already exists in this study.");
        }

        int newId = getNewId();
        dataset.setId(newId);

        Document datasetObject = getDocument(dataset, "Dataset");
        QueryResult<UpdateResult> update = studyCollection.update(
                new Document(_ID, studyId),
                new Document("$push", new Document("datasets", datasetObject)), null);

        if (update.getResult().get(0).getMatchedCount() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        return endQuery("createDataset", startTime, getDataset(newId, options));
    }

    @Override
    public QueryResult<Dataset> getDataset(int datasetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("datasets.id", datasetId);
        Document projection = new Document("datasets", new Document("$elemMatch", new Document("id", datasetId)));
        QueryResult<Document> queryResult = studyCollection.find(query, projection, filterOptions(options, FILTER_ROUTE_STUDIES));

        List<Study> studies = parseStudies(queryResult);
        if(studies == null || studies.get(0).getDatasets().isEmpty()) {
            throw CatalogDBException.idNotFound("Dataset", datasetId);
        } else {
            return endQuery("getDataset", startTime, studies.get(0).getDatasets());
        }
    }

    @Override
    public int getStudyIdByDatasetId(int datasetId) throws CatalogDBException {
        Document query = new Document("datasets.id", datasetId);
        QueryResult<Document> queryResult = studyCollection.find(query, new Document("id", 1), null);
        if(queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsKey("id")) {
            throw CatalogDBException.idNotFound("Dataset", datasetId);
        } else {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Integer ? (Integer) id : Integer.parseInt(id.toString());
        }
    }

    /**
     * Job methods
     * ***************************
     */

    @Override
    public boolean jobExists(int jobId) {
        QueryResult<Long> count = jobCollection.count(new Document("id", jobId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Job> createJob(int studyId, Job job, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        checkStudyId(studyId);

        int jobId = getNewId();
        job.setId(jobId);

        Document jobObject = getDocument(job, "job");
        jobObject.put(_ID, jobId);
        jobObject.put(_STUDY_ID, studyId);
        QueryResult insertResult = jobCollection.insert(jobObject, null); //TODO: Check results.get(0).getN() != 0

        return endQuery("Create Job", startTime, getJob(jobId, filterOptions(options, FILTER_ROUTE_JOBS)));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Long> deleteJob(int jobId) throws CatalogDBException {
        long startTime = startQuery();

        DeleteResult id = jobCollection.remove(new Document("id", jobId), null).getResult().get(0);
        List<Long> deletes = new LinkedList<>();
        if (id.getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("Job", jobId);
        } else {
            deletes.add(id.getDeletedCount());
            return endQuery("delete job", startTime, deletes);
        }
    }

    @Override
    public QueryResult<Job> getJob(int jobId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Document> queryResult = jobCollection.find(new Document("id", jobId), filterOptions(options, FILTER_ROUTE_JOBS));
        Job job = parseJob(queryResult);
        if(job != null) {
            return endQuery("Get job", startTime, Arrays.asList(job));
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    public QueryResult<Job> getAllJobs(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Document> queryResult = jobCollection.find(new Document(_STUDY_ID, studyId), filterOptions(options, FILTER_ROUTE_JOBS));
        List<Job> jobs = parseJobs(queryResult);
        return endQuery("Get all jobs", startTime, jobs);
    }

    @Override
    public String getJobStatus(int jobId, String sessionId) throws CatalogDBException {   // TODO remove?
        throw new UnsupportedOperationException("Not implemented method");
    }

    @Override
    public QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("id", jobId);
        Job job = parseJob(jobCollection.<Document>find(query, new Document("visits", true), null));
        int visits;
        if (job != null) {
            visits = job.getVisits()+1;
            Document set = new Document("$set", new Document("visits", visits));
            jobCollection.update(query, set, null);
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
        return endQuery("Inc visits", startTime, Arrays.asList(new ObjectMap("visits", visits)));
    }

    @Override
    public QueryResult modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> jobParameters = new HashMap<>();

        String[] acceptedParams = {"name", "userId", "toolName", "date", "description", "outputError", "commandLine", "status", "outdir"};
        filterStringParams(parameters, jobParameters, acceptedParams);

        String[] acceptedIntParams = {"visits"};
        filterIntParams(parameters, jobParameters, acceptedIntParams);

        String[] acceptedLongParams = {"startTime", "endTime", "diskUsage"};
        filterLongParams(parameters, jobParameters, acceptedLongParams);

        String[] acceptedIntegerListParams = {"output"};
        filterIntegerListParams(parameters, jobParameters, acceptedIntegerListParams);

        String[] acceptedMapParams = {"attributes", "resourceManagerAttributes"};
        filterMapParams(parameters, jobParameters, acceptedMapParams);

        if(!jobParameters.isEmpty()) {
            Document query = new Document("id", jobId);
            Document updates = new Document("$set", jobParameters);
//            System.out.println("query = " + query);
//            System.out.println("updates = " + updates);
            QueryResult<UpdateResult> update = jobCollection.update(query, updates, null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getMatchedCount() == 0){
                throw CatalogDBException.idNotFound("Job", jobId);
            }
        }
        return endQuery("Modify job", startTime);
    }

    @Override
    public int getStudyIdByJobId(int jobId) throws CatalogDBException {
        Document query = new Document("id", jobId);
        Document projection = new Document(_STUDY_ID, true);
        QueryResult<Document> id = jobCollection.find(query, projection, null);

        if (id.getNumResults() != 0) {
            return Integer.parseInt(id.getResult().get(0).get(_STUDY_ID).toString());
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    public QueryResult<Job> searchJob(QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document();

        if(options.containsKey("ready")) {
            if(options.getBoolean("ready")) {
                query.put("status", Job.Status.READY.name());
            } else {
                query.put("status", new Document("$ne", Job.Status.READY.name()));
            }
            options.remove("ready");
        }
        query.putAll(options);
//        System.out.println("query = " + query);
        QueryResult<Document> queryResult = jobCollection.find(query, null);
        List<Job> jobs = parseJobs(queryResult);
        return endQuery("Search job", startTime, jobs);
    }


    /**
     * Tool methods
     * ***************************
     */

    @Override
    public QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException {
        long startTime = startQuery();

        if (!userExists(userId)) {
            throw new CatalogDBException("User {id:" + userId + "} does not exist");
        }

        // Check if tools.alias already exists.
        Document countQuery = new Document("id", userId)
                .append("tools.alias", tool.getAlias());
        QueryResult<Long> count = userCollection.count(countQuery);
        if(count.getResult().get(0) != 0){
            throw new CatalogDBException( "Tool {alias:\"" + tool.getAlias() + "\"} already exists in this user");
        }

        tool.setId(getNewId());

        Document toolObject = getDocument(tool, "tool");
        Document query = new Document("id", userId);
        query.put("tools.alias", new Document("$ne", tool.getAlias()));
        Document update = new Document("$push", new Document ("tools", toolObject));

        //Update object
        QueryResult<UpdateResult> queryResult = userCollection.update(query, update, null);

        if (queryResult.getResult().get(0).getModifiedCount() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Tool {alias:\"" + tool.getAlias() + "\"} already exists in this user");
        }


        return endQuery("Create Job", startTime, getTool(tool.getId()).getResult());
    }


    public QueryResult<Tool> getTool(int id) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("tools.id", id);
        Document projection = new Document("tools",
                new Document("$elemMatch",
                        new Document("id", id)
                )
        );
        QueryResult<Document> queryResult = userCollection.find(query, projection, new QueryOptions("include", Collections.singletonList("tools")));

        if(queryResult.getNumResults() != 1 ) {
            throw new CatalogDBException("Tool {id:" + id + "} no exists");
        }

        User user = parseUser(queryResult);
        return endQuery("Get tool", startTime, user.getTools());
    }

    @Override
    public int getToolId(String userId, String toolAlias) throws CatalogDBException {
        Document query = new Document("id", userId)
                .append("tools.alias", toolAlias);
        Document projection = new Document("tools",
                new Document("$elemMatch",
                        new Document("alias", toolAlias)
                )
        );

        QueryResult<Document> queryResult = userCollection.find(query, projection, null);
        if(queryResult.getNumResults() != 1 ) {
            throw new CatalogDBException("Tool {alias:" + toolAlias + "} no exists");
        }
        User user = parseUser(queryResult);
        return user.getTools().get(0).getId();
    }

//    @Override
//    public QueryResult<Tool> searchTool(QueryOptions query, QueryOptions options) {
//        long startTime = startQuery();
//
//        QueryResult queryResult = userCollection.find(new Document(options),
//                new QueryOptions("include", Arrays.asList("tools")), null);
//
//        User user = parseUser(queryResult);
//
//        return endQuery("Get tool", startTime, user.getTools());
//    }

    /**
     * Experiments methods
     * ***************************
     */

    @Override
    public boolean experimentExists(int experimentId) {
        return false;
    }

    /**
     * Samples methods
     * ***************************
     */

    @Override
    public boolean sampleExists(int sampleId) {
        Document query = new Document("id", sampleId);
        QueryResult<Long> count = sampleCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        checkStudyId(studyId);
        QueryResult<Long> count = sampleCollection.count(
                new Document("name", sample.getName()).append(_STUDY_ID, studyId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getName() + "'} already exists.");
        }

        int sampleId = getNewId();
        sample.setId(sampleId);
        sample.setAnnotationSets(Collections.<AnnotationSet>emptyList());
        //TODO: Add annotationSets
        Document sampleObject = getDocument(sample, "sample");
        sampleObject.put(_STUDY_ID, studyId);
        sampleObject.put(_ID, sampleId);
        sampleCollection.insert(sampleObject, null);

        return endQuery("createSample", startTime, getSample(sampleId, options));
    }


    @Override
    public QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);
        Document query = new Document("id", sampleId);

        QueryResult<Document> queryResult = sampleCollection.find(query, filteredOptions);
        List<Sample> samples = parseSamples(queryResult);

        if(samples.isEmpty()) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }

        return endQuery("getSample", startTime, samples);
    }

    @Override
    public QueryResult<Sample> getAllSamples(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        String warning = "";

        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);
        Document query = new Document(_STUDY_ID, studyId);

        // Sample Filters  //
        addQueryIntegerListFilter("id", options, "_id", query);
        addQueryStringListFilter("name", options, query);
        addQueryStringListFilter("source", options, query);

        // AnnotationSet Filters //
        Document annotationSetFilter = new Document();
        addQueryIntegerListFilter("variableSetId", options, annotationSetFilter);
        addQueryStringListFilter("annotationSetId", options, "id", annotationSetFilter);


        List<Document> annotationFilters = new LinkedList<>();
        // Annotation Filters
        if (options.containsKey("annotation")) {
            List<String> annotations = options.getAsStringList("annotation");
            for (String annotation : annotations) {
                String[] split = annotation.split(":", 2);
                if (split.length != 2) {
                    String w = "Malformed annotation query : " + annotation;
                    warning += w + "\n";
                    logger.warn(warning);
                    continue;
                }
//                annotationFilters.add(
//                        new Document("annotations",
//                                new Document("$elemMatch", DocumentBuilder
//                                        .start("id", split[0])
//                                        .add("value", split[1]).get()
//                                )
//                        )
//                );
                String[] values = split[1].split(",");
                if (values.length > 1) {
                    annotationFilters.add(new Document("_annotMap" + "." + split[0], new Document("$in", Arrays.asList(values)) ));
                } else {
                    annotationFilters.add(new Document("_annotMap" + "." + split[0], split[1] ));
                }
            }
        }

        if (!annotationFilters.isEmpty()) {
            annotationSetFilter.put("$and", annotationFilters);
        }
        if (!annotationSetFilter.isEmpty()) {
            query.put("annotationSets", new Document("$elemMatch", annotationSetFilter));
        }
        QueryResult<Document> queryResult = sampleCollection.find(query, filteredOptions);
        List<Sample> samples = parseSamples(queryResult);

        QueryResult<Sample> result = endQuery("getAllSamples", startTime, samples, null, warning.isEmpty() ? null : warning);
        result.setNumTotalResults(queryResult.getNumTotalResults());
        return result;
    }

    @Override
    public QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException {
        //TODO
        throw new UnsupportedOperationException("No implemented");
    }

    @Override
    public QueryResult<Long> deleteSample(int sampleId) throws CatalogDBException {
        long startTime = startQuery();

        DeleteResult id = sampleCollection.remove(new Document("id", sampleId), null).getResult().get(0);
        List<Long> deletes = new LinkedList<>();
        if (id.getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        } else {
            deletes.add(id.getDeletedCount());
            return endQuery("delete sample", startTime, deletes);
        }
    }

    @Override
//<<<<<<< HEAD
//    public QueryResult<Job> getJob(int jobId) throws CatalogManagerException {
//        long startTime = startQuery();
//        QueryResult queryResult = jobCollection.find(new Document("id", jobId), null);
//        Job job = parseJob(queryResult);
//        if(job != null) {
//            return endQuery("Get job", startTime, Arrays.asList(job));
//=======
    public int getStudyIdBySampleId(int sampleId) throws CatalogDBException {
        Document query = new Document("id", sampleId);
        Document projection = new Document(_STUDY_ID, true);
        QueryResult<Document> queryResult = sampleCollection.find(query, projection, null);
        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(_STUDY_ID);
            return studyId instanceof Integer ? (Integer) studyId : Integer.parseInt(studyId.toString());
//>>>>>>> bba62bea67b13e466ff74c6c0befb010e6fd05db
        } else {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }
    }

    @Override
    public QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException {
        long startTime = startQuery();
//<<<<<<< HEAD
//        QueryResult queryResult = jobCollection.find(new Document("analysisId", analysisId), null);
//        List<Job> jobs = parseJobs(queryResult);
//        return endQuery("Get all jobs", startTime, jobs);
//    }
//=======
        checkStudyId(studyId);
//>>>>>>> bba62bea67b13e466ff74c6c0befb010e6fd05db

        QueryResult<Long> count = studyCollection.count(new Document(_ID, studyId)
                                                                .append("cohorts.name", cohort.getName()));

        if(count.getResult().get(0) > 0) {
            throw new CatalogDBException("Cohort { name: \"" + cohort.getName() + "\" } already exists in this study.");
        }

        int newId = getNewId();
        cohort.setId(newId);

        Document cohortObject = getDocument(cohort, "Cohort");
        QueryResult<UpdateResult> update = studyCollection.update(
                new Document(_ID, studyId),
                new Document("$push", new Document("cohorts", cohortObject)), null);

        if (update.getResult().get(0).getMatchedCount() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        return endQuery("createDataset", startTime, getCohort(newId));
    }

    @Override
    public QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException {
        long startTime = startQuery();

        Document query = new Document("cohorts.id", cohortId);
        Document projection = new Document("cohorts", new Document("$elemMatch", new Document("id", cohortId)));
        QueryResult<Document> queryResult = studyCollection.find(query, projection, null);

        List<Study> studies = parseStudies(queryResult);
        if(studies == null || studies.get(0).getCohorts().isEmpty()) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            return endQuery("getCohort", startTime, studies.get(0).getCohorts());
        }
    }

    @Override
    public int getStudyIdByCohortId(int cohortId) throws CatalogDBException {
        Document query = new Document("cohorts.id", cohortId);
        QueryResult<Document> queryResult = studyCollection.find(query, new Document("id", true), null);
        if(queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsKey("id")) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Integer ? (Integer) id : Integer.parseInt(id.toString());
        }
    }

    /**
     * Annotation Methods
     * ***************************
     */

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = studyCollection.count(
                new Document("variableSets.name", variableSet.getName()).append("id", studyId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("VariableSet { name: '" + variableSet.getName() + "'} already exists.");
        }

        int variableSetId = getNewId();
        variableSet.setId(variableSetId);
        Document object = getDocument(variableSet, "VariableSet");
        Document query = new Document("id", studyId);
        Document update = new Document("$push", new Document("variableSets", object));

        QueryResult<UpdateResult> queryResult = studyCollection.update(query, update, null);

        return endQuery("createVariableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_STUDIES);
        Document query = new Document("variableSets.id", variableSetId);
        Document projection = new Document(
                "variableSets",
                new Document(
                        "$elemMatch",
                        new Document("id", variableSetId)
                )
        );
        QueryResult<Document> queryResult = studyCollection.find(query, projection, filteredOptions);
        List<Study> studies = parseStudies(queryResult);
        if(studies.isEmpty() || studies.get(0).getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist");
        }

        return endQuery("", startTime, studies.get(0).getVariableSets());
    }

    @Override
    public QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = sampleCollection.count(
                new Document("annotationSets.id", annotationSet.getId()).append("id", sampleId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("AnnotationSet { id: " + annotationSet.getId() + "} already exists.");
        }

        Document object = getDocument(annotationSet, "AnnotationSet");
        Map<String, String> annotationMap = new HashMap<>();
        for (Annotation annotation : annotationSet.getAnnotations()) {
            annotationMap.put(annotation.getId(), annotation.getValue().toString());
        }
        object.put("_annotMap", annotationMap);

        Document query = new Document("id", sampleId);
        Document update = new Document("$push", new Document("annotationSets", object));

        QueryResult<UpdateResult> queryResult = sampleCollection.update(query, update, null);

        return endQuery("", startTime, Arrays.asList(annotationSet));
    }


    @Override
    public int getStudyIdByVariableSetId(int variableSetId) throws CatalogDBException {
        Document query = new Document("variableSets.id", variableSetId);

        QueryResult<Document> queryResult = studyCollection.find(query, new Document("id", true), null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get("id");
            return studyId instanceof Integer ? (Integer) studyId : Integer.parseInt(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
    }


    /*
    * Helper methods
    ********************/

    //Join fields from other collections
    private void joinFields(User user, QueryOptions options) throws CatalogDBException {
        if (options == null) {
            return;
        }
        if (user.getProjects() != null) {
            for (Project project : user.getProjects()) {
                joinFields(project, options);
            }
        }
    }

    private void joinFields(Project project, QueryOptions options) throws CatalogDBException {
        if (options == null) {
            return;
        }
        if (options.getBoolean("includeStudies")) {
            project.setStudies(getAllStudies(project.getId(), options).getResult());
        }
    }

    private void joinFields(Study study, QueryOptions options) throws CatalogDBException {
        study.setDiskUsage(getDiskUsageByStudy(study.getId()));

        if (options == null) {
            return;
        }
        if (options.getBoolean("includeFiles")) {
            study.setFiles(getAllFiles(study.getId(), options).getResult());
        }
        if (options.getBoolean("includeJobs")) {
            study.setJobs(getAllJobs(study.getId(), options).getResult());
        }
        if (options.getBoolean("includeSamples")) {
            study.setSamples(getAllSamples(study.getId(), options).getResult());
        }
    }

    private User parseUser(QueryResult<Document> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonUserReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing user", e);
        }
    }

    private List<Study> parseStudies(QueryResult<Document> result) throws CatalogDBException {
        List<Study> studies = new LinkedList<>();
        try {
            for (Document object : result.getResult()) {
                studies.add(jsonStudyReader.<Study>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing study", e);
        }
        return studies;
    }

    private File parseFile(QueryResult<Document> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonFileReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private List<File> parseFiles(QueryResult<Document> result) throws CatalogDBException {
        List<File> files = new LinkedList<>();
        try {
            for (Document o : result.getResult()) {
                files.add(jsonFileReader.<File>readValue(restoreDotsInKeys(o).toString()));
            }
            return files;
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private Job parseJob(QueryResult<Document> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonJobReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
    }

    private List<Job> parseJobs(QueryResult<Document> result) throws CatalogDBException {
        LinkedList<Job> jobs = new LinkedList<>();
        try {
            for (Document object : result.getResult()) {
                jobs.add(jsonJobReader.<Job>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
        return jobs;
    }

    private List<Sample> parseSamples(QueryResult<Document> result) throws CatalogDBException {
        LinkedList<Sample> samples = new LinkedList<>();
        try {
            for (Document object : result.getResult()) {
                samples.add(jsonSampleReader.<Sample>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing samples", e);
        }
        return samples;
    }

    private <T> List<T> parseObjects(QueryResult<Document> result, Class<T> tClass) throws CatalogDBException {
        LinkedList<T> objects = new LinkedList<>();
        try {
            for (Document object : result.getResult()) {
                objects.add(jsonReaderMap.get(tClass).<T>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
        return objects;
    }

    private <T> T parseObject(QueryResult<Document> result, Class<T> tClass) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonReaderMap.get(tClass).readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }

    private Document getDocument(Object object, String objectName) throws CatalogDBException {
        Document Document;
        try {
            Document = (Document) JSON.parse(jsonObjectWriter.writeValueAsString(object));
            Document = replaceDotsInKeys(Document);
        } catch (Exception e) {
            throw new CatalogDBException("Error while writing to Json : " + objectName);
        }
        return Document;
    }

    static final String TO_REPLACE_DOTS = "&#46;";
//    static final String TO_REPLACE_DOTS = "\uff0e";

    /**
     * Scan all the Document and replace all the dots in keys with
     * @param object
     * @return
     */


    static <T> T replaceDotsInKeys(T object) {
        return replaceInKeys(object, ".", TO_REPLACE_DOTS);
    }
    static <T> T restoreDotsInKeys(T object) {
        return replaceInKeys(object, TO_REPLACE_DOTS, ".");
    }
    static <T> T replaceInKeys(T object, String target, String replacement) {
        if (object instanceof Document) {
            Document document = (Document) object;
            List<String> keys = new ArrayList<>();
            for (String s : document.keySet()) {
                if (s.contains(target)) {
                    keys.add(s);
                }
                replaceInKeys(document.get(s), target, replacement);
            }
            for (String key : keys) {
                Object value = document.remove(key);
                key = key.replace(target, replacement);
                document.put(key, value);
            }
        } else if (object instanceof List) {
            for (Object o : ((List) object)) {
                replaceInKeys(o, target, replacement);
            }
        }
        return object;
    }

    /**
     * Filter "include" and "exclude" options.
     *
     * Include and Exclude options are as absolute routes. This method removes all the values that are not in the
     * specified route. For the values in the route, the route is removed.
     *
     * [
     *  name,
     *  projects.id,
     *  projects.studies.id,
     *  projects.studies.alias,
     *  projects.studies.name
     * ]
     *
     * with route = "projects.studies.", then
     *
     * [
     *  id,
     *  alias,
     *  name
     * ]
     *
     * @param options
     * @param route
     * @return
     */
    private QueryOptions filterOptions(QueryOptions options, String route) {
        if(options == null) {
            return null;
        }

        QueryOptions filteredOptions = new QueryOptions(options); //copy queryOptions

        String[] filteringLists = {"include", "exclude"};
        for (String listName : filteringLists) {
            List<String> list = filteredOptions.getAsStringList(listName);
            List<String> filteredList = new LinkedList<>();
            int length = route.length();
            if(list != null) {
                for (String s : list) {
                    if(s.startsWith(route)) {
                        filteredList.add(s.substring(length));
                    }
                }
                filteredOptions.put(listName, filteredList);
            }
        }
        return filteredOptions;
    }


    /*  */

    private void addQueryStringListFilter(String key, QueryOptions options, Document query) {
        addQueryStringListFilter(key, options, key, query);
    }
    private void addQueryStringListFilter(String optionKey, QueryOptions options, String queryKey, Document query) {
        if (options.containsKey(optionKey)) {
            List<String> stringList = options.getAsStringList(optionKey);
            if (stringList.size() > 1) {
                query.put(queryKey, new Document("$in", stringList));
            } else if (stringList.size() == 1) {
                query.put(queryKey, stringList.get(0));
            }
        }
    }

    private void addQueryIntegerListFilter(String key, QueryOptions options, Document query) {
        addQueryIntegerListFilter(key, options, key, query);
    }

    private void addQueryIntegerListFilter(String optionKey, QueryOptions options, String queryKey, Document query) {
        if (options.containsKey(optionKey)) {
            List<Integer> integerList = options.getAsIntegerList(optionKey);
            if (integerList.size() > 1) {
                query.put(queryKey, new Document("$in", integerList));
            } else if (integerList.size() == 1) {
                query.put(queryKey, integerList.get(0));
            }
        }
    }

    /*  */

    private void filterStringParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedParams) {
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getString(s));
            }
        }
    }

    private void filterIntegerListParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntegerListParams) {
        for (String s : acceptedIntegerListParams) {
            if(parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getAsIntegerList(s));
            }
        }
    }

    private void filterMapParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedMapParams) {
        for (String s : acceptedMapParams) {
            if (parameters.containsKey(s)) {
                ObjectMap map;
                if (parameters.get(s) instanceof Map) {
                    map = new ObjectMap(parameters.getMap(s));
                } else {
                    map = new ObjectMap(parameters.getString(s));
                }
                try {
                    Document Document = getDocument(map, s);
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        filteredParams.put(s + "." + entry.getKey(), Document.get(entry.getKey()));
                    }
                } catch (CatalogDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void filterIntParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntParams) {
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if(anInt != Integer.MIN_VALUE) {
                    filteredParams.put(s, anInt);
                }
            }
        }
    }

    private void filterLongParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedLongParams) {
        for (String s : acceptedLongParams) {
            if(parameters.containsKey(s)) {
                long aLong = parameters.getLong(s, Long.MIN_VALUE);
                if (aLong != Long.MIN_VALUE) {
                    filteredParams.put(s, aLong);
                }
            }
        }
    }

}
