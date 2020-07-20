package org.opencb.opencga.storage.mongodb.variant;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;


import java.util.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import static org.junit.Assert.*;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToVariantConverterTest {

    private Document mongoVariant;
    private Variant variant;
    protected VariantSourceEntry variantSourceEntry;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        //Setup variant
        variant = new Variant("1", 1000, 1000, "A", "C");
        variant.setId("rs666");

        //Setup variantSourceEntry
        variantSourceEntry = new VariantSourceEntry("f1", "s1");
        variantSourceEntry.addAttribute("QUAL", "0.01");
        variantSourceEntry.addAttribute("AN", "2");
        variantSourceEntry.setFormat("GT:DP");

        Map<String, String> na001 = new HashMap<>();
        na001.put("GT", "0/0");
        na001.put("DP", "4");
        variantSourceEntry.addSampleData("NA001", na001);
        Map<String, String> na002 = new HashMap<>();
        na002.put("GT", "0/1");
        na002.put("DP", "5");
        variantSourceEntry.addSampleData("NA002", na002);
        variant.addSourceEntry(variantSourceEntry);

        //Setup mongoVariant
        mongoVariant = new Document("_id", "1_1000_A_C")
                .append(DocumentToVariantConverter.IDS_FIELD, variant.getIds())
                .append(DocumentToVariantConverter.TYPE_FIELD, variant.getType().name())
                .append(DocumentToVariantConverter.CHROMOSOME_FIELD, variant.getChromosome())
                .append(DocumentToVariantConverter.START_FIELD, variant.getStart())
                .append(DocumentToVariantConverter.END_FIELD, variant.getStart())
                .append(DocumentToVariantConverter.LENGTH_FIELD, variant.getLength())
                .append(DocumentToVariantConverter.REFERENCE_FIELD, variant.getReference())
                .append(DocumentToVariantConverter.ALTERNATE_FIELD, variant.getAlternate());

        BasicDBList chunkIds = new BasicDBList();
        chunkIds.add("1_1_1k");
        chunkIds.add("1_0_10k");
        mongoVariant.append("_at", new Document("chunkIds", chunkIds));

        BasicDBList hgvs = new BasicDBList();
        hgvs.add(new Document("type", "genomic").append("name", "1:g.1000A>C"));
        mongoVariant.append("hgvs", hgvs);
    }

    @Test
    public void testConvertToDataModelTypeWithFiles() {
        variant.addSourceEntry(variantSourceEntry);

        // MongoDB object

        Document mongoFile = new Document(DocumentToVariantSourceEntryConverter.FILEID_FIELD, variantSourceEntry.getFileId())
                .append(DocumentToVariantSourceEntryConverter.STUDYID_FIELD, variantSourceEntry.getStudyId());
        mongoFile.append(DocumentToVariantSourceEntryConverter.ATTRIBUTES_FIELD,
                         new Document("QUAL", "0.01").append("AN", "2"));
        mongoFile.append(DocumentToVariantSourceEntryConverter.FORMAT_FIELD, variantSourceEntry.getFormat());
        Document genotypeCodes = new Document();
        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Arrays.asList(1));
        mongoFile.append(DocumentToVariantSourceEntryConverter.SAMPLES_FIELD, genotypeCodes);
        BasicDBList files = new BasicDBList();
        files.add(mongoFile);
        mongoVariant.append("files", files);

        List<String> sampleNames = Lists.newArrayList("NA001", "NA002");
        DocumentToVariantConverter converter = new DocumentToVariantConverter(
                new DocumentToVariantSourceEntryConverter(
                        VariantStorageManager.IncludeSrc.FULL,
                        new DocumentToSamplesConverter(sampleNames)),
                new DocumentToVariantStatsConverter());
        Variant converted = converter.convertToDataModelType(mongoVariant);
        assertEquals(variant, converted);
    }

    @Test
    public void testConvertToStorageTypeWithFiles() {

        variant.addSourceEntry(variantSourceEntry);

        // MongoDB object
        Document mongoFile = new Document(DocumentToVariantSourceEntryConverter.FILEID_FIELD, variantSourceEntry.getFileId())
                .append(DocumentToVariantSourceEntryConverter.STUDYID_FIELD, variantSourceEntry.getStudyId());
        mongoFile.append(DocumentToVariantSourceEntryConverter.ATTRIBUTES_FIELD,
                         new Document("QUAL", "0.01").append("AN", "2"));
        mongoFile.append(DocumentToVariantSourceEntryConverter.FORMAT_FIELD, variantSourceEntry.getFormat());
        Document genotypeCodes = new Document();
        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Arrays.asList(1));
        mongoFile.append(DocumentToVariantSourceEntryConverter.SAMPLES_FIELD, genotypeCodes);
//        mongoFile.append(DocumentToVariantConverter.STATS_FIELD, Collections.emptyList());
        BasicDBList files = new BasicDBList();
        files.add(mongoFile);
        mongoVariant.append("files", files);

        List<String> sampleNames = Lists.newArrayList("NA001", "NA002");
        DocumentToVariantConverter converter = new DocumentToVariantConverter(
                new DocumentToVariantSourceEntryConverter(
                        VariantStorageManager.IncludeSrc.FULL,
                        new DocumentToSamplesConverter(sampleNames)),
                null);
        Document converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsKey(DocumentToVariantConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(DocumentToVariantConverter.IDS_FIELD, variant.getIds());  //Add IDs
        assertEquals(mongoVariant, converted);
    }


    @Test
    public void testConvertToDataModelTypeWithoutFiles() {
        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Variant converted = converter.convertToDataModelType(mongoVariant);
        assertEquals(variant, converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutFiles() {
        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Document converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsKey(DocumentToVariantConverter.IDS_FIELD)); //IDs must be added manually.
        converted.put(DocumentToVariantConverter.IDS_FIELD, variant.getIds());  //Add IDs
        assertEquals(mongoVariant, converted);
    }

    /** @see DocumentToVariantConverter ids policy   */
    @Test
    public void testConvertToDataModelTypeNullIds() {
        mongoVariant.remove(DocumentToVariantConverter.IDS_FIELD);

        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Variant converted = converter.convertToDataModelType(mongoVariant);
        assertNotNull(converted.getIds());
        assertTrue(converted.getIds().isEmpty());

        variant.setIds(new HashSet<String>());
        assertEquals(variant, converted);
    }

    /** @see DocumentToVariantConverter ids policy   */
    @Test
    public void testConvertToStorageTypeNullIds() {
        variant.setIds(null);

        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Document converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsKey(DocumentToVariantConverter.IDS_FIELD)); //IDs must be added manually.

        mongoVariant.remove(DocumentToVariantConverter.IDS_FIELD);
        assertEquals(mongoVariant, converted);
    }

    /** @see DocumentToVariantConverter ids policy   */
    @Test
    public void testConvertToDataModelTypeEmptyIds() {
        mongoVariant.put(DocumentToVariantConverter.IDS_FIELD, new HashSet<String>());

        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Variant converted = converter.convertToDataModelType(mongoVariant);
        assertNotNull(converted.getIds());
        assertTrue(converted.getIds().isEmpty());

        variant.setIds(new HashSet<String>());
        assertEquals(variant, converted);
    }

    /** @see DocumentToVariantConverter ids policy   */
    @Test
    public void testConvertToStorageTypeEmptyIds() {
        variant.setIds(new HashSet<String>());

        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Document converted = converter.convertToStorageType(variant);
        assertFalse(converted.containsKey(DocumentToVariantConverter.IDS_FIELD)); //IDs must be added manually.

        mongoVariant.remove(DocumentToVariantConverter.IDS_FIELD);
        assertEquals(mongoVariant, converted);
    }

    @Test
    public void testBuildStorageId() {
        DocumentToVariantConverter converter = new DocumentToVariantConverter();

        // SNV
        Variant v1 = new Variant("1", 1000, 1000, "A", "C");
        assertEquals("1_1000_A_C", converter.buildStorageId(v1));

        // Indel
        Variant v2 = new Variant("1", 1000, 1002, "", "CA");
        assertEquals("1_1000__CA", converter.buildStorageId(v2));

        // Structural
        String alt = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        Variant v3 = new Variant("1", 1000, 1002, "TAG", alt);
        assertEquals("1_1000_TAG_" + new String(CryptoUtils.encryptSha1(alt)), converter.buildStorageId(v3));
    }


    @Test
    public void testConvertVariantWithCoordinatesStoredInLongObjectToDataModelType() {
        mongoVariant.append(DocumentToVariantConverter.START_FIELD, new Long(variant.getStart()))
                .append(DocumentToVariantConverter.END_FIELD, new Long(variant.getStart()));

        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        Variant converted = converter.convertToDataModelType(mongoVariant);
        assertEquals(variant, converted);
    }

    @Test
    public void testConvertVariantWithCoordinatesNotFittingInIntToDataModelTypeWillThrowException() {
        mongoVariant.append(DocumentToVariantConverter.START_FIELD, 12345678901L)
                    .append(DocumentToVariantConverter.END_FIELD, 12345678901L);

        DocumentToVariantConverter converter = new DocumentToVariantConverter();
        thrown.expect(ArithmeticException.class);
        converter.convertToDataModelType(mongoVariant);
    }
}
