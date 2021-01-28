package org.opencb.opencga.storage.mongodb.variant;

import org.bson.Document;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToVariantStatsConverterTest {
    
    private static Document mongoStats;
    private static VariantStats stats;
    
    @BeforeClass
    public static void setUpClass() {
        mongoStats = new Document(DocumentToVariantStatsConverter.MAF_FIELD, 0.1);
        mongoStats.append(DocumentToVariantStatsConverter.MGF_FIELD, 0.01);
        mongoStats.append(DocumentToVariantStatsConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(DocumentToVariantStatsConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(DocumentToVariantStatsConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD, 5);
        
        Document genotypes = new Document();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(DocumentToVariantStatsConverter.NUMGT_FIELD, genotypes);
        
        stats = new VariantStats(null, -1, null, null, Variant.VariantType.SNV, 0.1f, 0.01f, "A", "A/A", 10, 5, -1, -1, -1, -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
    }
    
    @Test
    public void testConvertToDataModelType() {
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        VariantStats converted = converter.convertToDataModelType(mongoStats);
        assertEquals(stats, converted);
    }
    
    @Test
    public void testConvertToStorageType() {
        DocumentToVariantStatsConverter converter = new DocumentToVariantStatsConverter();
        Document converted = converter.convertToStorageType(stats);
        
        assertEquals(stats.getMaf(), (float) converted.get(DocumentToVariantStatsConverter.MAF_FIELD), 1e-6);
        assertEquals(stats.getMgf(), (float) converted.get(DocumentToVariantStatsConverter.MGF_FIELD), 1e-6);
        assertEquals(stats.getMafAllele(), converted.get(DocumentToVariantStatsConverter.MAFALLELE_FIELD));
        assertEquals(stats.getMgfGenotype(), converted.get(DocumentToVariantStatsConverter.MGFGENOTYPE_FIELD));
        
        assertEquals(stats.getMissingAlleles(), converted.get(DocumentToVariantStatsConverter.MISSALLELE_FIELD));
        assertEquals(stats.getMissingGenotypes(), converted.get(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD));
        
        assertEquals(100, ((Document) converted.get(DocumentToVariantStatsConverter.NUMGT_FIELD)).get("0/0"));
        assertEquals(50, ((Document) converted.get(DocumentToVariantStatsConverter.NUMGT_FIELD)).get("0/1"));
        assertEquals(10, ((Document) converted.get(DocumentToVariantStatsConverter.NUMGT_FIELD)).get("1/1"));
    }
}
