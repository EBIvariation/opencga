package org.opencb.opencga.analysis.storage.variant;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hpccoll1 on 13/02/15.
 */
public class CatalogVariantStorageManager implements StorageManager<VariantWriter, VariantDBAdaptor> {
//public class CatalogVariantStorageManager extends VariantStorageManager {


    private CatalogManager catalogManager;
//    private VariantStorageManager storageManager;
    private Properties properties;
    private final List<URI> configUris;

    public CatalogVariantStorageManager() {
        this.properties = new Properties();
        configUris = new LinkedList<>();
    }

    public CatalogVariantStorageManager(CatalogManager catalogManager) {
        this();
        this.catalogManager = catalogManager;
//        this.storageManager = variantStorageManager;
    }

    @Override
    public void addConfigUri(URI configUri) {
        try {
            properties.load(new InputStreamReader(new FileInputStream(configUri.getPath())));
        } catch (IOException e) {
            e.printStackTrace();
        }
        configUris.add(configUri);
    }

    @Override
    public URI extract(URI from, URI to, ObjectMap params) throws StorageManagerException {
        return getStorageManager(params).extract(from, to, params);
    }

    @Override
    public URI preTransform(URI input, ObjectMap params) throws IOException, FileFormatException, StorageManagerException {
        return getStorageManager(params).preTransform(input, params);
    }

    @Override
    public URI transform(URI input, URI pedigree, URI output, ObjectMap params) throws IOException, FileFormatException, StorageManagerException {
        return getStorageManager(params).transform(input, pedigree, output, params);
    }

    @Override
    public URI postTransform(URI input, ObjectMap params) throws IOException, FileFormatException, StorageManagerException {
        return getStorageManager(params).postTransform(input, params);
    }

    @Override
    public URI preLoad(URI input, URI output, ObjectMap params) throws IOException, StorageManagerException {
        return getStorageManager(params).preLoad(input, output, params);
    }

    @Override
    public URI load(URI input, ObjectMap params) throws IOException, StorageManagerException {
        return getStorageManager(params).load(input, params);
    }

    @Override
    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException, StorageManagerException {
        return getStorageManager(params).postLoad(input, output, params);
    }

    @Override
    public VariantWriter getDBWriter(String dbName, ObjectMap params) throws StorageManagerException {
        if (dbName == null) {
            dbName = getCatalogManager().getUserIdBySessionId(params.getString("sessionId"));
        }
        return getStorageManager(params).getDBWriter(dbName, params);
    }

    @Override
    public VariantDBAdaptor getDBAdaptor(String dbName, ObjectMap params) throws StorageManagerException {
        if (dbName == null) {
            dbName = getCatalogManager().getUserIdBySessionId(params.getString("sessionId"));
        }
        return getStorageManager(params).getDBAdaptor(dbName, params);
    }

    public CatalogManager getCatalogManager() {
        if (catalogManager == null) {
            try {
                catalogManager = new CatalogManager(Config.getProperties("catalog", properties));
            } catch (CatalogException e) {
                e.printStackTrace();
            }
        }
        return catalogManager;
    }

//    public void setCatalogManager(CatalogManager catalogManager) {
//        this.catalogManager = catalogManager;
//    }

    public VariantStorageManager getStorageManager(ObjectMap params) throws StorageManagerException {
        try {
            QueryResult<File> file = getCatalogManager().getFile(params.getInt("fileId"), params.getString("sessionId"));
            String storageEngine = file.getResult().get(0).getAttributes().get("storageEngine").toString();
            return StorageManagerFactory.getVariantStorageManager(storageEngine);
        } catch (Exception e) {
            throw new StorageManagerException("Can't get StorageEngine", e);
        }
    }

//    public void setStorageManager(VariantStorageManager storageManager) {
//        this.storageManager = storageManager;
//    }
}
