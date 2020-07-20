package org.opencb.opencga.storage.mongodb.variant;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

/**
 * 
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToVariantSourceEntryConverter implements ComplexTypeConverter<VariantSourceEntry, Document> {

    public final static String FILEID_FIELD = "fid";
    public final static String STUDYID_FIELD = "sid";
    public final static String ALTERNATES_FIELD = "alts";
    public final static String ATTRIBUTES_FIELD = "attrs";
    public final static String FORMAT_FIELD = "fm";
    public final static String SAMPLES_FIELD = "samp";
    static final char CHARACTER_TO_REPLACE_DOTS = (char) 163; // <-- £
    
    private VariantStorageManager.IncludeSrc includeSrc;

    private DocumentToSamplesConverter samplesConverter;

    /**
     * Create a converter between VariantSourceEntry and Document entities when 
     * there is no need to provide a list of samples or statistics.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the Document
     */
    public DocumentToVariantSourceEntryConverter(VariantStorageManager.IncludeSrc includeSrc) {
        this.includeSrc = includeSrc;
        this.samplesConverter = null;
    }


    /**
     * Create a converter from VariantSourceEntry to Document entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *  @param includeSrc       If true, will include and gzip the "src" attribute in the Document
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     *
     */
    public DocumentToVariantSourceEntryConverter(VariantStorageManager.IncludeSrc includeSrc,
                                                 DocumentToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.samplesConverter = samplesConverter;
    }

    @Override
    public VariantSourceEntry convertToDataModelType(Document document) {
        String fileId = (String) document.get(FILEID_FIELD);
        String studyId = (String) document.get(STUDYID_FIELD);
        VariantSourceEntry file = new VariantSourceEntry(fileId, studyId);
        
        // Alternate alleles
        if (document.containsKey(ALTERNATES_FIELD)) {
            List list = (List) document.get(ALTERNATES_FIELD);
            String[] alternatives = new String[list.size()];
            int i = 0;
            for (Object o : list) {
                alternatives[i] = o.toString();
                i++;
            }
            file.setSecondaryAlternates(alternatives);
        }
        
        // Attributes
        if (document.containsKey(ATTRIBUTES_FIELD)) {
            Document attributes = (Document) document.get(ATTRIBUTES_FIELD);
            for (Map.Entry<String, Object> o : attributes.entrySet()) {
                file.addAttribute(o.getKey().replace(CHARACTER_TO_REPLACE_DOTS, '.'), o.getValue().toString());
            }
            
            // Unzip the "src" field, if available
            if (((Document) document.get(ATTRIBUTES_FIELD)).containsKey("src")) {
                byte[] o = (byte[]) ((Document) document.get(ATTRIBUTES_FIELD)).get("src");
                try {
                    file.addAttribute("src", org.opencb.commons.utils.StringUtils.gunzip(o));
                } catch (IOException ex) {
                    Logger.getLogger(DocumentToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        if (document.containsKey(FORMAT_FIELD)) {
            file.setFormat((String) document.get(FORMAT_FIELD));
        }
        
        // Samples
        if (samplesConverter != null && document.containsKey(SAMPLES_FIELD)) {
            VariantSourceEntry fileWithSamplesData = samplesConverter.convertToDataModelType(document);
            
            // Add the samples to the Java object, combining the data structures
            // with the samples' names and the genotypes
            for (Map.Entry<String, Map<String, String>> sampleData : fileWithSamplesData.getSamplesData().entrySet()) {
                file.addSampleData(sampleData.getKey(), sampleData.getValue());
            }
        }
        
        return file;
    }

    @Override
    public Document convertToStorageType(VariantSourceEntry object) {
        Document mongoFile = new Document(FILEID_FIELD, object.getFileId()).append(STUDYID_FIELD, object.getStudyId());

        // Alternate alleles
        if (object.getSecondaryAlternates().length > 0) {   // assuming secondaryAlternates doesn't contain the primary alternate
            mongoFile.append(ALTERNATES_FIELD, object.getSecondaryAlternates());
        }
        
        // Attributes
        if (object.getAttributes().size() > 0) {
            Document attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                Object value = entry.getValue();
                if (entry.getKey().equals("src")) {
                    if (VariantStorageManager.IncludeSrc.FULL.equals(includeSrc)) {
                        try {
                            value = org.opencb.commons.utils.StringUtils.gzip(entry.getValue());
                        } catch (IOException ex) {
                            Logger.getLogger(DocumentToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else if (VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS.equals(includeSrc)) {
                        String[] fields = entry.getValue().split("\t");
                        StringBuilder sb = new StringBuilder();
                        sb.append(fields[0]);
                        for (int i = 1; i < fields.length && i < 8; i++) {
                            sb.append("\t").append(fields[i]);
                        }
                        try {
                            value = org.opencb.commons.utils.StringUtils.gzip(sb.toString());
                        } catch (IOException ex) {
                            Logger.getLogger(DocumentToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        continue;
                    }
                }

                if (attrs == null) {
                    attrs = new Document(entry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS), value);
                } else {
                    attrs.append(entry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS), value);
                }
            }

            if (attrs != null) {
                mongoFile.put(ATTRIBUTES_FIELD, attrs);
            }
        }

//        if (samples != null && !samples.isEmpty()) {
        if (samplesConverter != null) {
            mongoFile.append(FORMAT_FIELD, object.getFormat()); // Useless field if genotypeCodes are not stored
            mongoFile.put(SAMPLES_FIELD, samplesConverter.convertToStorageType(object));
        }
        
        
        return mongoFile;
    }

    public void setIncludeSrc(VariantStorageManager.IncludeSrc includeSrc) {
        this.includeSrc = includeSrc;
    }
}
