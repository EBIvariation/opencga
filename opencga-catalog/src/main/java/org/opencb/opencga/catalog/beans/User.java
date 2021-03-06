package org.opencb.opencga.catalog.beans;

import java.util.*;

/**
 * Created by imedina on 11/09/14.
 */
public class User {

    /**
     * id is a unique string in the database
     */
    private String id;
    private String name;
    private String email;
    private String password;
    private String organization;

    /**
     * This specifies the role of this user in OpenCGA, possible values: admin, user, demo, ...
     */
    private Role role;
    private String status;
    private String lastActivity;
    private long diskUsage;
    private long diskQuota;

    private List<Project> projects;
    private List<Tool> tools;

    /**
     * Open and closed sessions for this user.
     * More than one session can be open, i.e. logged from Chrome and Firefox
     */
    private List<Session> sessions;

    private Map<String, Object> configs;
    private Map<String, Object> attributes;

    /**
     * Things to think about:
     private List<Credential> credentials = new ArrayList<Credential>();
        private List<Bucket> buckets = new ArrayList<Bucket>();
     */

    public enum Role {
        ADMIN,  //= "admin";
        USER,  //= "user";
        ANONYMOUS  //= "anonymous";
    }

    public User() {
    }

    public User(String id, String name, String email, String password, String organization, Role role, String status) {
        this(id, name, email, password, organization, role, status, "", -1, -1, new ArrayList<Project>(),
                new ArrayList<Tool>(0), new ArrayList<Session>(0),
                new HashMap<String, Object>(), new HashMap<String, Object>());
    }

    public User(String id, String name, String email, String password, String organization, Role role, String status,
                String lastActivity, long diskUsage, long diskQuota, List<Project> projects, List<Tool> tools,
                List<Session> sessions, Map<String, Object> configs, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.password = password;
        this.organization = organization;
        this.role = role;
        this.status = status;
        this.lastActivity = lastActivity;
        this.diskUsage = diskUsage;
        this.diskQuota = diskQuota;
        this.projects = projects;
        this.tools = tools;
        this.sessions = sessions;
        this.configs = configs;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", email='" + email + '\'' +
                ", password='" + password + '\'' +
                ", organization='" + organization + '\'' +
                ", role='" + role + '\'' +
                ", status='" + status + '\'' +
                ", lastActivity='" + lastActivity + '\'' +
                ", diskUsage=" + diskUsage +
                ", diskQuota=" + diskQuota +
                ", projects=" + projects +
                ", tools=" + tools +
                ", sessions=" + sessions +
                ", configs=" + configs +
                ", attributes=" + attributes +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLastActivity() {
        return lastActivity;
    }

    public void setLastActivity(String lastActivity) {
        this.lastActivity = lastActivity;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public long getDiskQuota() {
        return diskQuota;
    }

    public void setDiskQuota(long diskQuota) {
        this.diskQuota = diskQuota;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

    public List<Tool> getTools() {
        return tools;
    }

    public void setTools(List<Tool> tools) {
        this.tools = tools;
    }

    public List<Session> getSessions() {
        return sessions;
    }

    public void setSessions(List<Session> sessions) {
        this.sessions = sessions;
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, Object> configs) {
        this.configs = configs;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

}
