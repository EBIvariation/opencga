package org.opencb.opencga.catalog.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.JVM)
public class CatalogMongoDBAdaptorTest extends GenericTest {

    private static CatalogDBAdaptor catalogDBAdaptor;

//    @Rule
//    public Timeout globalTimeout = new Timeout(2000); // 200 ms max per method tested

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private static User user1;
    private static User user2;
    private static User user3;

    /**
     * This method is executed one single time beforeClass all the tests. It connects to the MongoDB server.
     *
     * @throws IOException
     * @throws CatalogDBException
     */
    @Before
    public void beforeClass() throws IOException, CatalogDBException {
        InputStream is = CatalogMongoDBAdaptorTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                properties.getProperty(CatalogManager.CATALOG_DB_HOSTS).split(",")[0], 27017);

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", properties.getProperty(CatalogManager.CATALOG_DB_USER, ""))
                .add("password", properties.getProperty(CatalogManager.CATALOG_DB_PASSWORD, ""))
                .add("authenticationDatabase", properties.getProperty(CatalogManager.CATALOG_DB_AUTHENTICATION_DB, ""))
                .build();

        String database = properties.getProperty(CatalogManager.CATALOG_DB_DATABASE) + "_catalog_test";
        /**
         * Database is cleared before each execution
         */
//        clearDB(dataStoreServerAddress, mongoCredentials);
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddress.getHost(), dataStoreServerAddress.getPort());
        MongoDataStore db = mongoManager.get(database);
        db.getDb().drop();

        catalogDBAdaptor = new CatalogMongoDBAdaptor(Arrays.asList(dataStoreServerAddress), mongoDBConfiguration, database);
        initDefaultCatalogDB();
    }


    @AfterClass
    public static void afterClass() {
        catalogDBAdaptor.disconnect();
    }

//    @Before
    public void initDefaultCatalogDB() throws CatalogDBException {
        /**
         * Let's init the database with some basic data to perform each of the tests
         */
        user1 = new User("jcoll", "Jacobo Coll", "jcoll@ebi", "1234", "", User.Role.USER, "", "", 100, 1000, Arrays.<Project>asList(new Project("project", "P1", "", "", ""), new Project("project", "P2", "", "", ""), new Project("project", "P3", "", "", "")),
                Collections.<Tool>emptyList(), Collections.<Session>emptyList(), Collections.<String, Object>emptyMap(),Collections.<String, Object>emptyMap());
        QueryResult createUser = catalogDBAdaptor.insertUser(user1, null);
        assertNotNull(createUser.getResult());

        user2 = new User("jmmut", "Jose Miguel", "jmmut@ebi", "1111", "ACME", User.Role.USER, "off");
        createUser = catalogDBAdaptor.insertUser(user2, null);
        assertNotNull(createUser.getResult());

        user3 = new User("imedina", "Nacho", "nacho@gmail", "2222", "SPAIN", User.Role.USER, "active", "", 1222, 122222,
                Arrays.asList(new Project(-1, "90 GigaGenomes", "90G", "today", "very long description", "Spain", "", "", 0, Collections.<Acl>emptyList(),
                                Arrays.asList(new Study(-1, "Study name", "ph1", Study.Type.CONTROL_SET, "", "", "", "", "", 1234, "", Collections.<Acl>emptyList(), Collections.<Experiment>emptyList(),
                                                Arrays.asList(new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "/data/file.vcf", null, null, "", File.Status.READY, 1000)
                                                ), Collections.<Job>emptyList(), new LinkedList<Sample>(), new LinkedList<Dataset>(), new LinkedList<Cohort>(), new LinkedList<VariableSet>(), null, Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap()
                                        )
                                ), Collections.<String, Object>emptyMap())
                ),
                Collections.<Tool>emptyList(), Collections.<Session>emptyList(),
                Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap());
        createUser = catalogDBAdaptor.insertUser(user3, null);
        assertNotNull(createUser.getResult());

    }

    /** **************************
     * User methods
     * ***************************
     */
    @Test
    public void createUserTest() throws CatalogDBException {
        User user = new User("NewUser", "", "", "", "", User.Role.USER, "");
        QueryResult createUser = catalogDBAdaptor.insertUser(user, null);
        assertNotSame(0, createUser.getResult().size());

        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.insertUser(user, null);
    }

    @Test
    public void deleteUserTest() throws CatalogDBException {
        User deletable1 = new User("deletable1", "deletable 1", "d1@ebi", "1234", "", User.Role.USER, "");
        QueryResult createUser = catalogDBAdaptor.insertUser(deletable1, null);
        assertFalse(createUser.getResult().isEmpty());
        assertNotNull(createUser.getResult().get(0));

        QueryResult deleteUser = catalogDBAdaptor.deleteUser(deletable1.getId());
        assertFalse(deleteUser.getResult().isEmpty());
        assertNotNull(deleteUser.getResult().get(0));

        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.deleteUser(deletable1.getId());
    }

    @Test
    public void getUserTest() throws CatalogDBException {
        QueryResult<User> user = catalogDBAdaptor.getUser(user1.getId(), null, null);
        assertNotSame(0, user.getResult().size());

        user = catalogDBAdaptor.getUser(user3.getId(), null, null);
        assertFalse(user.getResult().isEmpty());
        assertFalse(user.getResult().get(0).getProjects().isEmpty());

        user = catalogDBAdaptor.getUser(user3.getId(), new QueryOptions("exclude", Arrays.asList("projects")), null);
        assertNull(user.getResult().get(0).getProjects());

        user = catalogDBAdaptor.getUser(user3.getId(), null, user.getResult().get(0).getLastActivity());
        assertTrue(user.getResult().isEmpty());

        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.getUser("NonExistingUser", null, null);
    }

    @Test
    public void loginTest() throws CatalogDBException, IOException {
        String userId = user1.getId();
        Session sessionJCOLL = new Session("127.0.0.1");
        QueryResult<ObjectMap> login = catalogDBAdaptor.login(userId, "1234", sessionJCOLL);
        assertEquals(userId, login.getResult().get(0).getString("userId"));

        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.login(userId, "INVALID_PASSWORD", sessionJCOLL);
    }

    @Test
    public void logoutTest() throws CatalogDBException, IOException {
        String userId = user1.getId();
        Session sessionJCOLL = new Session("127.0.0.1");
        QueryResult<ObjectMap> login = catalogDBAdaptor.login(userId, "1234", sessionJCOLL);
        assertEquals(userId, login.getResult().get(0).getString("userId"));

        QueryResult logout = catalogDBAdaptor.logout(userId, sessionJCOLL.getId());
        assertEquals(0, logout.getResult().size());

        //thrown.expect(CatalogDBException.class);
        QueryResult falseSession = catalogDBAdaptor.logout(userId, "FalseSession");
        assertTrue(falseSession.getWarningMsg() != null && !falseSession.getWarningMsg().isEmpty());

    }

    @Test
    public void changePasswordTest() throws CatalogDBException {
//        System.out.println(catalogDBAdaptor.changePassword("jmmut", "1111", "1234"));
//        System.out.println(catalogDBAdaptor.changePassword("jmmut", "1234", "1111"));
//        try {
//            System.out.println(catalogDBAdaptor.changePassword("jmmut", "BAD_PASSWORD", "asdf"));
//            fail("Expected \"bad password\" exception");
//        } catch (CatalogDBException e) {
//            System.out.println(e);
//        }
        QueryResult queryResult = catalogDBAdaptor.changePassword(user2.getId(), user2.getPassword(), "1234");
        assertNotSame(0, queryResult.getResult().size());

        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.changePassword(user2.getId(), "BAD_PASSWORD", "asdf");
    }

    @Test
    public void modifyUserTest() throws CatalogDBException {

        ObjectMap genomeMapsConfig = new ObjectMap("lastPosition" , "4:1222222:1333333");
        genomeMapsConfig.put("otherConf", Arrays.asList(1,2,3,4,5));
        ObjectMap configs = new ObjectMap("genomemaps" , genomeMapsConfig);
        ObjectMap objectMap = new ObjectMap("configs", configs.toJson());
        catalogDBAdaptor.modifyUser(user1.getId(), objectMap);

        User user = catalogDBAdaptor.getUser(user1.getId(), null, null).first();
        System.out.println(user);
    }

    /**
     * Project methods
     * ***************************
     */
    @Test
    public void createProjectTest() throws CatalogDBException, JsonProcessingException {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        LinkedList<Acl> acl = new LinkedList<>();
        acl.push(new Acl(user1.getId(), true, false, true, true));
        acl.push(new Acl(user2.getId(), false, true, true, true));
        p.setAcl(acl);
        System.out.println(catalogDBAdaptor.createProject(user1.getId(), p, null));
        p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", "", "", 3000, "");
        System.out.println(catalogDBAdaptor.createProject(user1.getId(), p, null));
        p = new Project("Project management project", "pmp", "yesterday", "it is a system", "", "", 2000, "");
        System.out.println(catalogDBAdaptor.createProject(user2.getId(), p, null));
        System.out.println(catalogDBAdaptor.createProject(user1.getId(), p, null));

        try {
            System.out.println(catalogDBAdaptor.createProject(user1.getId(), p, null));
            fail("Expected \"projectAlias already exists\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getProjectIdTest() throws CatalogDBException {
        assertTrue(catalogDBAdaptor.getProjectId(user3.getId(), user3.getProjects().get(0).getAlias()) != -1);
        assertTrue(catalogDBAdaptor.getProjectId(user3.getId(), "nonExistingProject") == -1);
    }


    @Test
    public void getProjectTest() throws CatalogDBException {
        int projectId = catalogDBAdaptor.getProjectId(user3.getId(), user3.getProjects().get(0).getAlias());
        System.out.println("projectId = " + projectId);
        QueryResult<Project> project = catalogDBAdaptor.getProject(projectId, null);
        System.out.println(project);
        assertNotNull(project.getResult().get(0));

        thrown.expect(CatalogDBException.class);    //"Expected \"bad id\" exception"
        catalogDBAdaptor.getProject(-100, null);
    }

    @Test
    public void deleteProjectTest() throws CatalogDBException {
        Project p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", "", "", 3000, "");
        QueryResult<Project> result = catalogDBAdaptor.createProject(user1.getId(), p, null);
        System.out.println(result);
        p = result.getResult().get(0);
        QueryResult<Long> queryResult = catalogDBAdaptor.deleteProject(p.getId());

        System.out.println(queryResult);
        assertTrue(queryResult.getResult().get(0) == 1);
        thrown.expect(CatalogDBException.class);    //Expected "Project not found" exception
        catalogDBAdaptor.deleteProject(-1);
    }

    @Test
    public void getAllProjects() throws CatalogDBException {
        QueryResult<Project> allProjects = catalogDBAdaptor.getAllProjects(user3.getId(), null);
        System.out.println(allProjects);
        assertTrue(!allProjects.getResult().isEmpty());
    }

    /**
     * cases:
     * ok: correct projectId, correct newName
     * error: non-existent projectId
     * error: newName already used
     * error: newName == oldName
     *
     * @throws CatalogDBException
     */
    @Test
    public void renameProjectTest() throws CatalogDBException {
        Project p1 = catalogDBAdaptor.createProject(user1.getId(), new Project("project1", "p1", "Tomorrow", "Cool", "", "", 3000, ""), null).getResult().get(0);
        Project p2 = catalogDBAdaptor.createProject(user1.getId(), new Project("project2", "p2", "Tomorrow", "Cool", "", "", 3000, ""), null).getResult().get(0);
        System.out.println(catalogDBAdaptor.renameProjectAlias(p1.getId(), "newpmp"));

        try {
            System.out.println(catalogDBAdaptor.renameProjectAlias(-1, "falseProject"));
            fail("renamed project with projectId=-1");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
        try {
            System.out.println(catalogDBAdaptor.renameProjectAlias(p1.getId(), p2.getAlias()));
            fail("renamed project with name collision");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

//        try {
//            System.out.println(catalogDBAdaptor.renameProjectAlias(p1.getId(), p1.getAlias()));
//            fail("renamed project to its old name");
//        } catch (CatalogDBException e) {
//            System.out.println("correct exception: " + e);
//        }
    }

    @Test
    public void projectAclTest() throws CatalogDBException {
        int projectId = user3.getProjects().get(0).getId();
        List<Acl> acls = catalogDBAdaptor.getProjectAcl(projectId, user3.getId()).getResult();
        assertTrue(acls.isEmpty());
        acls = catalogDBAdaptor.getProjectAcl(projectId, user2.getId()).getResult();
        assertTrue(acls.isEmpty());
        acls = catalogDBAdaptor.getProjectAcl(projectId, "noUser").getResult();
        assertTrue(acls.isEmpty());


        Acl granted = new Acl("jmmut", true, true, true, false);
        System.out.println(catalogDBAdaptor.setProjectAcl(projectId, granted));  // overwrites
        Acl jmmut = catalogDBAdaptor.getProjectAcl(projectId, "jmmut").getResult().get(0);
        System.out.println(jmmut);
        assertTrue(jmmut.equals(granted));

        granted.setUserId("imedina");
        System.out.println(catalogDBAdaptor.setProjectAcl(projectId, granted));  // just pushes
        Acl imedina = catalogDBAdaptor.getProjectAcl(projectId, "imedina").getResult().get(0);
        System.out.println(imedina);
        assertTrue(imedina.equals(granted));
        try {
            granted.setUserId("noUser");
            catalogDBAdaptor.setProjectAcl(projectId, granted);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }


    /**
     * Study methods
     * ***************************
     */
    @Test
    public void createStudyTest() throws CatalogDBException {
        int projectId = catalogDBAdaptor.getProjectId(user1.getId(), "P1");
        int projectId2 = catalogDBAdaptor.getProjectId(user1.getId(), "P2");

        Study s = new Study("Phase 1", "ph1", Study.Type.CASE_CONTROL, "", "", null);
        LinkedList<Acl> acl = new LinkedList<>();
        acl.push(new Acl("jcoll", false, true, true, true));
        acl.push(new Acl("jmmut", false, false, true, false));
        s.setAcl(acl);
        System.out.println(catalogDBAdaptor.createStudy(projectId, s, null));
        System.out.println(catalogDBAdaptor.createStudy(projectId2, s, null));
        s = new Study("Phase 3", "ph3", Study.Type.CASE_CONTROL, "", "", null);
        System.out.println(catalogDBAdaptor.createStudy(projectId, s, null));
        s = new Study("Phase 7", "ph7", Study.Type.CASE_CONTROL, "", "", null);
        System.out.println(catalogDBAdaptor.createStudy(projectId, s, null));

        try {
            System.out.println(catalogDBAdaptor.createStudy(projectId, s, null));  //Repeated study
            fail("Expected \"Study alias already exist\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
        try {
            System.out.println(catalogDBAdaptor.createStudy(-100, s, null));  //ProjectId not exists
            fail("Expected \"bad project id\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void deleteStudyTest() throws CatalogDBException {
        int projectId = catalogDBAdaptor.getProjectId("jcoll", "P1");
        Study study = catalogDBAdaptor.createStudy(projectId, new Study("Phase 1", "ph1", Study.Type.CASE_CONTROL, "", "", null), null).first();
        QueryResult<Long> queryResult = catalogDBAdaptor.deleteStudy(study.getId());
        System.out.println(queryResult);
        assertTrue(queryResult.getResult().get(0) == 1);

        assertTrue(catalogDBAdaptor.getStudyId(projectId, study.getAlias()) == -1);
        try {
            catalogDBAdaptor.getStudy(study.getId(), null);
            fail("error: Expected \"Study not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        try {
            QueryResult<Long> queryResult1 = catalogDBAdaptor.deleteStudy(-1);
            fail("error: Expected \"Study not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getAllStudiesTest() throws CatalogDBException {
        int projectId = user3.getProjects().get(0).getId();
        QueryResult<Study> allStudies = catalogDBAdaptor.getAllStudies(projectId, null);
        assertTrue(allStudies.getNumResults() != 0);
        System.out.println(allStudies);
        try {
            System.out.println(catalogDBAdaptor.getAllStudies(-100, null));
            fail("Expected \"bad project id\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getStudyTest() throws CatalogDBException, JsonProcessingException {
        int projectId = user3.getProjects().get(0).getId();
        int studyId = catalogDBAdaptor.getStudyId(projectId, "ph1");
        System.out.println(catalogDBAdaptor.getStudy(studyId, null));
        try {
            catalogDBAdaptor.getStudy(-100, null);
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void modifyStudyTest() throws CatalogDBException {
        int projectId = user3.getProjects().get(0).getId();
        int studyId = catalogDBAdaptor.getStudyId(projectId, "ph1");

        String newName = "My new name";
        String unexpectedNewAlias = "myNewAlias";
        ObjectMap objectMap = new ObjectMap("name", newName);
        HashMap<String, Object> newAttributes = new HashMap<>();
        newAttributes.put("Value", 1);
        newAttributes.put("Value2", true);
        newAttributes.put("Value3", new ObjectMap("key", "ok"));

        objectMap.put("attributes", new ObjectMap(newAttributes).toJson());
        objectMap.put("alias", unexpectedNewAlias);
        ObjectMap modifiedParams = catalogDBAdaptor.modifyStudy(studyId, objectMap).first();

        Study study = catalogDBAdaptor.getStudy(studyId, null).first();

        assertTrue(modifiedParams.containsKey("attributes.Value"));
        assertTrue(modifiedParams.containsKey("attributes.Value2"));
        assertTrue(modifiedParams.containsKey("attributes.Value3"));
        for (Map.Entry<String, Object> entry : newAttributes.entrySet()) {
            assertEquals(study.getAttributes().get(entry.getKey()), entry.getValue());
        }

        assertEquals(newName, study.getName());
        assertTrue(modifiedParams.containsKey("name"));

        assertFalse("ModifyStudy must NO modify the alias ", unexpectedNewAlias.equals(study.getAlias()));
        assertFalse(modifiedParams.containsKey("alias"));
    }

    @Test
    public void getStudyAclTest() throws CatalogDBException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
        List<Acl> jmmut = catalogDBAdaptor.getStudyAcl(studyId, "jmmut").getResult();
        assertTrue(!jmmut.isEmpty());
        System.out.println(jmmut.get(0));
        List<Acl> noUser = catalogDBAdaptor.getStudyAcl(studyId, "noUser").getResult();
        assertTrue(noUser.isEmpty());
    }

    @Test
    public void setStudyAclTest() throws CatalogDBException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
        System.out.println(studyId);

        Acl granted = new Acl("jmmut", true, true, true, false);
        catalogDBAdaptor.setStudyAcl(studyId, granted);
        granted.setUserId("imedina");
        catalogDBAdaptor.setStudyAcl(studyId, granted);
        try {
            granted.setUserId("noUser");
            catalogDBAdaptor.setStudyAcl(studyId, granted);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    /**
     * Files methods
     * ***************************
     */
    @Test
    public void createFileToStudyTest() throws CatalogDBException, IOException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
        assertTrue(studyId >= 0);
        File f;
        f = new File("data/", File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, "/data/", null, TimeUtils.getTime(), "", File.Status.UPLOADING, 1000);
        LinkedList<Acl> acl = new LinkedList<>();
        acl.push(new Acl("jcoll", true, true, true, true));
        acl.push(new Acl("jmmut", false, false, true, true));
        f.setAcl(acl);
        System.out.println(catalogDBAdaptor.createFileToStudy(studyId, f, null));
        f = new File("file.sam", File.Type.FILE, File.Format.PLAIN, File.Bioformat.ALIGNMENT, "/data/file.sam", null, TimeUtils.getTime(), "", File.Status.UPLOADING, 1000);
        System.out.println(catalogDBAdaptor.createFileToStudy(studyId, f, null));
        f = new File("file.bam", File.Type.FILE, File.Format.BINARY, File.Bioformat.ALIGNMENT, "/data/file.bam", null, TimeUtils.getTime(), "", File.Status.UPLOADING, 1000);
        System.out.println(catalogDBAdaptor.createFileToStudy(studyId, f, null));
        f = new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "/data/file.vcf", null, TimeUtils.getTime(), "", File.Status.UPLOADING, 1000);

        try {
            System.out.println(catalogDBAdaptor.createFileToStudy(-20, f, null));
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        System.out.println(catalogDBAdaptor.createFileToStudy(studyId, f, null));

        try {
            System.out.println(catalogDBAdaptor.createFileToStudy(studyId, f, null));
            fail("Expected \"File already exist\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getFileIdTest() throws CatalogDBException, IOException {
        assertTrue(catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/") != -1);
        assertTrue(catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/file.sam") != -1);
        assertTrue(catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/file.txt") == -1);
    }

    @Test
    public void getFileTest() throws CatalogDBException {
        System.out.println(catalogDBAdaptor.getFile(catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/file.sam")));
        try {
            System.out.println(catalogDBAdaptor.getFile(-1));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getAllFilesTest() throws CatalogDBException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
        QueryResult<File> allFiles = catalogDBAdaptor.getAllFiles(studyId, null);
        List<File> files = allFiles.getResult();
        System.out.println(files);
        assertTrue(!files.isEmpty());

        studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph7");
        allFiles = catalogDBAdaptor.getAllFiles(studyId, null);
        assertTrue(allFiles.getResult().isEmpty());
    }

//    @Test
//    public void setFileStatus() throws CatalogDBException, IOException {
//        int fileId = catalogDBAdaptor.getFileId("jcoll", "1000G", "ph1", "/data/file.vcf");
//        System.out.println(catalogDBAdaptor.setFileStatus(fileId, File.Status.UPLOADING));
//        assertEquals(catalogDBAdaptor.getFile(fileId).getResult().get(0).getStatus(), File.Status.UPLOADING);
//        System.out.println(catalogDBAdaptor.setFileStatus(fileId, File.Status.UPLOADED));
//        assertEquals(catalogDBAdaptor.getFile(fileId).getResult().get(0).getStatus(), File.Status.UPLOADED);
//        System.out.println(catalogDBAdaptor.setFileStatus(fileId, File.Status.READY));
//        assertEquals(catalogDBAdaptor.getFile(fileId).getResult().get(0).getStatus(), File.Status.READY);
//        try {
//            System.out.println(catalogDBAdaptor.setFileStatus("jcoll", "1000G", "ph1", "/data/noExists", File.READY));
//            fail("Expected \"FileId not found\" exception");
//        } catch (CatalogDBException e) {
//            System.out.println(e);
//        }
//    }

    @Test
    public void modifyFileTest() throws CatalogDBException, IOException {
        int fileId = catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/file.vcf");
        DBObject stats = BasicDBObjectBuilder.start().append("stat1", 1).append("stat2", true).append("stat3", "ok" + StringUtils.randomString(20)).get();

        ObjectMap parameters = new ObjectMap();
        parameters.put("status", File.Status.READY);
        parameters.put("stats", stats);
        System.out.println(catalogDBAdaptor.modifyFile(fileId, parameters));

        File file = catalogDBAdaptor.getFile(fileId).getResult().get(0);
        assertEquals(file.getStatus(), File.Status.READY);
        assertEquals(file.getStats(), stats);

    }

    @Test
    public void renameFileTest() throws CatalogDBException {
        String newName = "newFile.bam";
        String parentPath = "/data/";
        int fileId = catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/file.bam");
        System.out.println(catalogDBAdaptor.renameFile(fileId, parentPath + newName));

        File file = catalogDBAdaptor.getFile(fileId).getResult().get(0);
        assertEquals(file.getName(), newName);
        assertEquals(file.getPath(), parentPath + newName);

        try {
            catalogDBAdaptor.renameFile(-1, "noFile");
            fail("error: expected \"file not found\"exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        int folderId = catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/");
        try {
            catalogDBAdaptor.renameFile(folderId, "notRenamed");
            fail("error: expected \"unsupported\"exception");
        } catch (UnsupportedOperationException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void deleteFileTest() throws CatalogDBException, IOException {
        QueryResult<Long> delete = catalogDBAdaptor.deleteFile(catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/file.sam"));
        System.out.println(delete);
        assertTrue(delete.getResult().get(0) == 1);
        try {
            System.out.println(catalogDBAdaptor.deleteFile(catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/noExists")));
            fail("error: Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getFileAclsTest() throws CatalogDBException {
        int fileId = catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/");

        List<Acl> jcoll = catalogDBAdaptor.getFileAcl(fileId, "jcoll").getResult();
        assertTrue(!jcoll.isEmpty());
        System.out.println(jcoll.get(0));
        List<Acl> imedina = catalogDBAdaptor.getFileAcl(fileId, "imedina").getResult();
        assertTrue(imedina.isEmpty());
    }

    @Test
    public void setFileAclsTest() throws CatalogDBException {
        int fileId = catalogDBAdaptor.getFileId(catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "/data/file.vcf");
        System.out.println(fileId);

        Acl granted = new Acl("jmmut", true, true, true, false);
        catalogDBAdaptor.setFileAcl(fileId, granted);
        granted.setUserId("imedina");
        catalogDBAdaptor.setFileAcl(fileId, granted);
        try {
            granted.setUserId("noUser");
            catalogDBAdaptor.setFileAcl(fileId, granted);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }


    /**
     * Analyses methods
     * ***************************
     */
//    @Test
//    public void createAnalysisTest() throws CatalogManagerException {
//        Analysis analysis = new Analysis(0, "analisis1Name", "analysis1Alias", "today", "creatorId", "creationDate", "analaysis 1 description");
//        System.out.println(catalogDBAdaptor.createAnalysis("jcoll", "1000G", "ph1", analysis));
//        System.out.println(catalogDBAdaptor.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, same alias
//        analysis = new Analysis(0, "analisis2Name", "analysis2Alias", "lastmonth", "creatorId", "creationDate", "analaysis 2 decrypton");
//        System.out.println(catalogDBAdaptor.createAnalysis("jcoll", "1000G", "ph1", analysis));  // same study, different alias
//        analysis = new Analysis(0, "analisis3Name", "analysis3Alias", "lastmonth", "jmmmut", "today", "analaysis 3 decrypton");
//        System.out.println(catalogDBAdaptor.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, different alias
//        analysis = new Analysis(0, "analisis3Name", "analysis2Alias", "lastmonth", "jmmmut", "today", "analaysis 2 decrypton");
//        System.out.println(catalogDBAdaptor.createAnalysis("jcoll", "1000G", "ph3", analysis));  // different study, different alias
//
//        try {
//            System.out.println(catalogDBAdaptor.createAnalysis("jcoll", "1000G", "ph3", analysis));
//            fail("expected \"analysis already exists\" exception");
//        } catch (CatalogManagerException e) {
//        }
//    }
//
//    @Test
//    public void getAllAnalysisTest() throws CatalogManagerException, JsonProcessingException {
//        System.out.println(catalogDBAdaptor.getAllAnalysis("jcoll", "1000G", "ph1"));
//        int studyId = catalogDBAdaptor.getStudyId("jcoll", "1000G", "ph1");
//        QueryResult<Analysis> allAnalysis = catalogDBAdaptor.getAllAnalysis(studyId);
//        System.out.println(allAnalysis);
//    }
//
//    @Test
//    public void getAnalysisTest() throws CatalogManagerException, JsonProcessingException {
//        QueryResult<Analysis> analysis = catalogDBAdaptor.getAnalysis(-1);
//        if (analysis.getNumResults() != 0) {
//            fail("error: expected no analysis. instead returned: " + analysis);
//        }
//        int studyId = catalogDBAdaptor.getStudyId("jcoll", "1000G", "ph1");
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");  // analysis3Alias does not belong to ph1
//        catalogDBAdaptor.getAnalysis(analysisId);
//    }
//
//    @Test
//    public void getAnalysisIdTest() throws CatalogManagerException {
//        int studyId = catalogDBAdaptor.getStudyId("jcoll", "1000G", "ph1");
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis3Alias");  // analysis3Alias does not belong to ph1
//        System.out.println("analysisId: " + analysisId);
//        assertTrue(analysisId < 0);
//
//        studyId = catalogDBAdaptor.getStudyId("jcoll", "1000G", "ph3");
//        analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis2Alias");
//        System.out.println("analysisId: " + analysisId);
//        assertTrue(analysisId >= 0);
//    }
//
//    @Test
//    public void modifyAnalysisTest() throws CatalogManagerException, IOException {
//        int studyId = catalogDBAdaptor.getStudyId("jcoll", "1000G", "ph3");
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis2Alias");
//        String newName = "newName-" + StringUtils.randomString(10);
//        String description = "description-" + StringUtils.randomString(50);
//        DBObject attributes = BasicDBObjectBuilder.start().append("stat1", 1).append("stat2", true).append("stat3", "ok" + StringUtils.randomString(20)).get();
//
//        ObjectMap parameters = new ObjectMap();
//        parameters.put("name", newName);
//        parameters.put("description", description);
//        parameters.put("attributes", attributes);
//        System.out.println(catalogDBAdaptor.modifyAnalysis(analysisId, parameters));
//
//        Analysis analysis = catalogDBAdaptor.getAnalysis(analysisId).getResult().get(0);
//        System.out.println(analysis);
//        assertEquals(analysis.getName(), newName);
//        assertEquals(analysis.getDescription(), description);
//        assertEquals(analysis.getAttributes(), attributes);
//
//    }


    /**
     * Job methods
     * ***************************
     */
    @Test
    public void createJobTest() throws CatalogDBException {
        Job job = new Job();
        job.setVisits(0);

        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
        job.setName("jobName1");
        System.out.println(catalogDBAdaptor.createJob(studyId, job, null));
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");

        job.setName("jobName2");
        System.out.println(catalogDBAdaptor.createJob(studyId, job, null));
        try {
            catalogDBAdaptor.createJob(-1, job, null);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void deleteJobTest() throws CatalogDBException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");
        int jobId = catalogDBAdaptor.getAllJobs(studyId, null).getResult().get(0).getId();
        QueryResult<Long> queryResult = catalogDBAdaptor.deleteJob(jobId);
        System.out.println(queryResult);
        assertTrue(queryResult.getResult().get(0) == 1);
        try {
            System.out.println(catalogDBAdaptor.deleteJob(-1));
            fail("error: Expected \"Job not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getAllJobTest() throws CatalogDBException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");
        QueryResult<Job> allJobs = catalogDBAdaptor.getAllJobs(studyId, null);
        System.out.println(allJobs);
    }


    @Test
    public void getJobTest() throws CatalogDBException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");
        QueryResult<Job> allJobs = catalogDBAdaptor.getAllJobs(studyId, null);
        Job job = catalogDBAdaptor.getJob(allJobs.getResult().get(0).getId(), null).getResult().get(0);
        System.out.println(job);

        try {
            catalogDBAdaptor.getJob(-1, null);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

    }

    @Test
    public void incJobVisits() throws CatalogDBException {
        int studyId = catalogDBAdaptor.getStudyId(catalogDBAdaptor.getProjectId("jcoll", "1000G"), "ph1");
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");
        int id = catalogDBAdaptor.getAllJobs(studyId, null).getResult().get(0).getId();
        Job jobBefore = catalogDBAdaptor.getJob(id, null).getResult().get(0);

        Integer visits = (Integer) catalogDBAdaptor.incJobVisits(jobBefore.getId()).getResult().get(0).get("visits");

        Job jobAfter = catalogDBAdaptor.getJob(id, null).getResult().get(0);
        assertTrue(jobBefore.getVisits() == jobAfter.getVisits() - 1);
        assertTrue(visits == jobAfter.getVisits());
    }



    /////////// Other tests
    @Test
    public void replaceDots() {

        DBObject original = new BasicDBObject("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new BasicDBObject("933.44", "df.sdf"))).append("key", new BasicDBObject("key....k", "value...2.2.2"));
        DBObject o = new BasicDBObject("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new BasicDBObject("933.44", "df.sdf"))).append("key", new BasicDBObject("key....k", "value...2.2.2"));
        System.out.println(o);

        CatalogMongoDBAdaptor.replaceDotsInKeys(o);
        System.out.println(o);

        CatalogMongoDBAdaptor.restoreDotsInKeys(o);
        System.out.println(o);

        Assert.assertEquals(original, o);

    }

}
