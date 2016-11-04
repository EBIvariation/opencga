package org.opencb.opencga.storage.core.alignment.local;

import ga4gh.Reads;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentFilters;
import org.opencb.biodata.tools.alignment.AlignmentManager;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.AlignmentUtils;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptor implements AlignmentDBAdaptor {

    private static final int MINOR_CHUNK_SIZE = 1000;
    private static final int DEFAULT_CHUNK_SIZE = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 1000000;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    public DefaultAlignmentDBAdaptor() {
        this(DEFAULT_CHUNK_SIZE);
    }

    public DefaultAlignmentDBAdaptor(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public QueryResult getAllAlignmentsByRegion(List<Region> regions, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getCoverageByRegion(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAllIntervalFrequencies(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult<ReadAlignment> get(Path path, Query query, QueryOptions options) {
        try {
            StopWatch watch = new StopWatch();
            watch.start();

            FileUtils.checkFile(path);
            AlignmentManager alignmentManager = new AlignmentManager(path);

            AlignmentOptions alignmentOptions = parseQueryOptions(options);
            AlignmentFilters alignmentFilters = parseQuery(query);
            Region region = parseRegion(query);

            List<ReadAlignment> readAlignmentList;
            if (region != null) {
                readAlignmentList = alignmentManager.query(region, alignmentOptions, alignmentFilters, ReadAlignment.class);
            } else {
                readAlignmentList = alignmentManager.query(alignmentOptions, alignmentFilters, ReadAlignment.class);
            }
//            List<String> stringFormatList = new ArrayList<>(readAlignmentList.size());
//            for (Reads.ReadAlignment readAlignment : readAlignmentList) {
//                stringFormatList.add(readAlignment());
//            }
//            List<JsonFormat> list = alignmentManager.query(region, alignmentOptions, alignmentFilters, Reads.ReadAlignment.class);
            watch.stop();
            return new QueryResult("Get alignments", ((int) watch.getTime()), readAlignmentList.size(), readAlignmentList.size(),
                    null, null, readAlignmentList);
        } catch (Exception e) {
            e.printStackTrace();
            return new QueryResult<>();
        }
    }

    @Override
    public ProtoAlignmentIterator iterator(Path path) {
        return iterator(path, new Query(), new QueryOptions());
    }

    @Override
    public ProtoAlignmentIterator iterator(Path path, Query query, QueryOptions options) {
        return (ProtoAlignmentIterator) iterator(path, query, options, Reads.ReadAlignment.class);
    }

    @Override
    public <T> AlignmentIterator<T> iterator(Path path, Query query, QueryOptions options, Class<T> clazz) {
        try {
            FileUtils.checkFile(path);

            if (query == null) {
                query = new Query();
            }

            if (options == null) {
                options = new QueryOptions();
            }

            AlignmentManager alignmentManager = new AlignmentManager(path);
            AlignmentFilters alignmentFilters = parseQuery(query);
            AlignmentOptions alignmentOptions = parseQueryOptions(options);

            Region region = parseRegion(query);
            if (region != null) {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(alignmentManager.iterator(region, alignmentOptions,
                            alignmentFilters, Reads.ReadAlignment.class));
                }
            } else {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(alignmentManager.iterator(alignmentOptions, alignmentFilters,
                            Reads.ReadAlignment.class));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public QueryResult<Long> count(Path path, Query query, QueryOptions options) {
        StopWatch watch = new StopWatch();
        watch.start();

        ProtoAlignmentIterator iterator = iterator(path, query, options);
        long cont = 0;
        while (iterator.hasNext()) {
            iterator.next();
            cont++;
        }

        watch.stop();
        return new QueryResult<>("Get count", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(cont));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        FileUtils.checkFile(path);
        FileUtils.checkDirectory(workspace);

        Path statsPath = workspace.resolve(path.getFileName() + ".stats");
        AlignmentGlobalStats alignmentGlobalStats;

        if (statsPath.toFile().exists()) {
            // Read the file of stats
            ObjectMapper objectMapper = new ObjectMapper();
            alignmentGlobalStats = objectMapper.readValue(statsPath.toFile(), AlignmentGlobalStats.class);
        } else {
            AlignmentManager alignmentManager = new AlignmentManager(path);
            alignmentGlobalStats = alignmentManager.stats();
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectWriter objectWriter = objectMapper.typedWriter(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), alignmentGlobalStats);
        }

        watch.stop();
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(alignmentGlobalStats));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }

        if (options.size() == 0 && query.size() == 0) {
            return stats(path, workspace);
        }

        FileUtils.checkFile(path);

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        AlignmentManager alignmentManager = new AlignmentManager(path);
        AlignmentGlobalStats alignmentGlobalStats = alignmentManager.stats(region, alignmentOptions, alignmentFilters);

        watch.stop();
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(alignmentGlobalStats));
    }

    @Override
    public QueryResult<RegionCoverage> coverage(Path path, Path workspace) throws Exception {
        QueryOptions options = new QueryOptions();
        options.put("windowSize", DEFAULT_WINDOW_SIZE);
        options.put("contained", false);
        return coverage(path, workspace, new Query(), options);
    }

    @Override
    public QueryResult<RegionCoverage> coverage(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        FileUtils.checkFile(path);

        if (query == null) {
            query = new Query();
        }

        if (options == null) {
            options = new QueryOptions();
        }

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        int windowSize;
        RegionCoverage coverage;
        if (region != null) {
            if (region.getEnd() - region.getStart() > 50 * MINOR_CHUNK_SIZE) {
                // if region is too big then we calculate the mean. We need to protect this code!
                // and query SQLite database
                windowSize = options.getInt("windowSize", DEFAULT_WINDOW_SIZE);
                coverage = meanCoverage(path, workspace, region, windowSize);
            } else {
                // if region is small enough we calculate all coverage for all positions dynamically
                // calling the biodata alignment manager
                AlignmentManager alignmentManager = new AlignmentManager(path);
                coverage = alignmentManager.coverage(region, alignmentOptions, alignmentFilters);
            }
        } else {
            // if no region is given we set up the windowSize to default value,
            // we should return a few thousands mean values
            // and query SQLite database
            windowSize = DEFAULT_WINDOW_SIZE;
            SAMFileHeader fileHeader = AlignmentUtils.getFileHeader(path);
            SAMSequenceRecord seq = fileHeader.getSequenceDictionary().getSequences().get(0);
            int arraySize = Math.min(50 * MINOR_CHUNK_SIZE, seq.getSequenceLength());
//            System.out.println("size = " + size);
//            System.out.println("seq = " + seq);
//            System.exit(0);

            region = new Region(seq.getSequenceName(), 1, arraySize * MINOR_CHUNK_SIZE);
            coverage = meanCoverage(path, workspace, region, windowSize);
        }

        watch.stop();
        return new QueryResult("Region coverage", ((int) watch.getTime()), 1, 1, null, null, Arrays.asList(coverage));
    }


    private Region parseRegion(Query query) {
        Region region = null;
        String regionString = query.getString(QueryParams.REGION.key());
        if (regionString != null && !regionString.isEmpty()) {
            region = new Region(regionString);
        }
        return region;
    }

    private AlignmentFilters parseQuery(Query query) {
        AlignmentFilters alignmentFilters = AlignmentFilters.create();
        int minMapQ = query.getInt(QueryParams.MIN_MAPQ.key());
        if (minMapQ > 0) {
            alignmentFilters.addMappingQualityFilter(minMapQ);
        }
        return  alignmentFilters;
    }

    private AlignmentOptions parseQueryOptions(QueryOptions options) {
        AlignmentOptions alignmentOptions = new AlignmentOptions()
                .setContained(options.getBoolean(QueryParams.CONTAINED.key()));
        int limit = options.getInt(QueryParams.LIMIT.key());
        if (limit > 0) {
            alignmentOptions.setLimit(limit);
        }
        return alignmentOptions;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public DefaultAlignmentDBAdaptor setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    private RegionCoverage meanCoverage(Path bamPath, Path workspace, Region region, int windowSize) {
        windowSize = Math.max(windowSize / MINOR_CHUNK_SIZE * MINOR_CHUNK_SIZE, MINOR_CHUNK_SIZE);
        int size = (region.getEnd() - region.getStart() + 1) / windowSize;
        short[] values = new short[size];

        String absoluteBamPath = bamPath.toFile().getAbsolutePath();
        Path coverageDBPath = workspace.toAbsolutePath().resolve("coverage.db");

        try {
            Class.forName("org.sqlite.JDBC");
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + coverageDBPath);
            Statement stmt = connection.createStatement();

            String sql = "SELECT id FROM file where path = '" + absoluteBamPath + "';";
            ResultSet rs = stmt.executeQuery(sql);
            int fileId = -1;
            while (rs.next()) {
                fileId = rs.getInt("id");
                break;
            }

            // sanity check
            if (fileId == -1) {
                throw new SQLException("Internal error: file " + absoluteBamPath + " not found in the coverage DB");
            }

            sql = "SELECT c.start, c.end, mc.v1, mc.v2, mc.v3, mc.v4, mc.v5, mc.v6, mc.v7, mc.v8"
                    + " FROM chunk c, mean_coverage mc"
                    + " WHERE c.id = mc.chunk_id AND mc.file_id = " + fileId
                    + " AND c.chromosome = '" + region.getChromosome() + "' AND c.start <= " + region.getEnd()
                    + " AND c.end > " + region.getStart() + " ORDER by c.start ASC;";
            rs = stmt.executeQuery(sql);

            int chunksPerWindow = windowSize / MINOR_CHUNK_SIZE;
            int chunkCounter = 0;
            int coverageAccumulator = 0;
            int arrayPos = 0;

            int start = 0;
            long packedCoverages;
            byte[] meanCoverages;

            boolean first = true;
            while (rs.next()) {
                if (first) {
                    start = rs.getInt("start");
                    first = false;
                }
                for (int i = 0; i < 8; i++) {
                    packedCoverages = rs.getInt("v" + (i + 1));
                    meanCoverages = longToBytes(packedCoverages);
                    for (int j = 0; j < 8; j++) {
                        if (start <= region.getEnd() && (start + MINOR_CHUNK_SIZE) >= region.getStart()) {
                            coverageAccumulator += meanCoverages[j];
                            if (++chunkCounter >= chunksPerWindow) {
                                values[arrayPos++] = (short) Math.round(1.0f * coverageAccumulator / chunkCounter);
                                coverageAccumulator = 0;
                                chunkCounter = 0;
                            }
                        }
                        start += MINOR_CHUNK_SIZE;
                    }
                }
            }
            if (chunkCounter > 0) {
                values[arrayPos++] = (short) Math.round(1.0f * coverageAccumulator / chunkCounter);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new RegionCoverage(region, windowSize, values);
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
