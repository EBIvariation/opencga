package org.opencb.opencga.catalog;

import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.models.pedigree.Individual;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 29/01/15.
 */
public class CatalogSampleAnnotationsLoader {

    private final CatalogManager catalogManager;
    private static Logger logger = LoggerFactory.getLogger(CatalogFileManager.class);

    public CatalogSampleAnnotationsLoader(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    protected CatalogSampleAnnotationsLoader() {
        this.catalogManager = null;
    }

    public QueryResult<Sample> loadSampleAnnotations(File pedFile, Integer variableSetId, String sessionId) throws CatalogException {

        URI fileUri = catalogManager.getFileUri(pedFile);
        int studyId = catalogManager.getStudyIdByFileId(pedFile.getId());
        long auxTime;
        long startTime = System.currentTimeMillis();

        //Read Pedigree file
        Pedigree ped = readPedigree(fileUri.getPath());
        Map<String, Sample> sampleMap = new HashMap<>();

        //Take or infer the VariableSet
        VariableSet variableSet;
        if (variableSetId != null) {
            variableSet = catalogManager.getVariableSet(variableSetId, null, sessionId).getResult().get(0);
        } else {
            variableSet = getVariableSetFromPedFile(ped);
            CatalogSampleAnnotationsValidator.checkVariableSet(variableSet);
        }

        //Check VariableSet for all samples
        for (Individual individual : ped.getIndividuals().values()) {
            Map<String, Object> annotation = getAnnotation(individual, sampleMap, variableSet, ped.getFields());
            HashSet<Annotation> annotationSet = new HashSet<>(annotation.size());
            for (Map.Entry<String, Object> annotationEntry : annotation.entrySet()) {
                annotationSet.add(new Annotation(annotationEntry.getKey(), annotationEntry.getValue()));
            }
            try {
                CatalogSampleAnnotationsValidator.checkAnnotationSet(variableSet, new AnnotationSet("", variableSet.getId(), annotationSet, "", null), null);
            } catch (CatalogException e) {
                String message = "Validation with the variableSet {id: " + variableSetId + "} over ped File = {id: " + pedFile.getId() + ", name: \"" + pedFile.getName() + "\"} failed";
                logger.info(message);
                throw new CatalogException(message, e);
            }
        }

        /** Pedigree file validated. Add samples and VariableSet **/


        //Add VariableSet (if needed)
        if (variableSetId == null) {
            auxTime = System.currentTimeMillis();
            variableSet = catalogManager.createVariableSet(studyId, pedFile.getName(), true,
                    "Auto-generated VariableSet from File = {id: " + pedFile.getId() + ", name: \"" + pedFile.getName() + "\"}",
                    null, variableSet.getVariables(), sessionId).getResult().get(0);
            variableSetId = variableSet.getId();
            logger.debug("Added VariableSet = {id: {}} in {}ms", variableSetId, System.currentTimeMillis()-auxTime);
        }

        //Add Samples TODO: Check if sample exists
        auxTime = System.currentTimeMillis();
        for (Individual individual : ped.getIndividuals().values()) {
            QueryResult<Sample> sampleQueryResult = catalogManager.createSample(studyId, individual.getId(), pedFile.getName(),
                    "Sample loaded from the pedigree File = {id: " + pedFile.getId() + ", name: \"" + pedFile.getName() + "\" }"
                    , Collections.<String, Object>emptyMap(), null, sessionId);
            Sample sample = sampleQueryResult.getResult().get(0);
            sampleMap.put(individual.getId(), sample);
        }
        logger.debug("Added {} samples in {}ms", ped.getIndividuals().size(), System.currentTimeMillis()-auxTime);

        //Annotate Samples
        auxTime = System.currentTimeMillis();
        for (Map.Entry<String, Sample> entry : sampleMap.entrySet()) {
            Map<String, Object> annotations = getAnnotation(ped.getIndividuals().get(entry.getKey()), sampleMap, variableSet, ped.getFields());
            catalogManager.annotateSample(entry.getValue().getId(), "Pedigree annotation", variableSetId, annotations, Collections.<String, Object>emptyMap(), false, sessionId);
        }
        logger.debug("Annotated {} samples in {}ms", ped.getIndividuals().size(), System.currentTimeMillis() - auxTime);

        //TODO: Create Cohort

        QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, new QueryOptions("variableSetId", variableSetId), sessionId);
        return new QueryResult<>("loadPedigree", (int)(System.currentTimeMillis() - startTime),
                sampleMap.size(), sampleMap.size(), null, null, sampleQueryResult.getResult());
    }

    /**
     *
     * @param individual            Individual from Pedigree file
     * @param sampleMap             Map<String, Sample>, to relate "sampleName" with "sampleId"
     * @param variableSet           VariableSet to annotate
     * @param fields
     * @return
     */
    protected Map<String, Object> getAnnotation(Individual individual, Map<String, Sample> sampleMap, VariableSet variableSet, Map<String, Integer> fields) {
        if (sampleMap == null) {
            sampleMap = new HashMap<>();
        }
        Map<String, Object> annotations = new HashMap<>();
        for (Variable variable : variableSet.getVariables()) {
            switch (variable.getId()) {
                case "family":
                    annotations.put("family", individual.getFamily());
                    break;
                case "name":
                    annotations.put("name", individual.getId());
                    break;
                case "fatherName":
                    annotations.put("fatherName", individual.getFatherId());
                    break;
                case "motherName":
                    annotations.put("motherName", individual.getMotherId());
                    break;
                case "sex":
                    annotations.put("sex", individual.getSex());
                    break;
                case "phenotype":
                    annotations.put("phenotype", individual.getPhenotype());
                    break;
                case "id":
                    Sample sample = sampleMap.get(individual.getId());
                    if (sample != null) {
                        annotations.put("id", sample.getId());
                    } else {
                        annotations.put("id", -1);
                    }
                    break;
                case "fatherId":
                    Sample father = sampleMap.get(individual.getFatherId());
                    if (father != null) {
                        annotations.put("fatherId", father.getId());
                    }
                    break;
                case "motherId":
                    Sample mother = sampleMap.get(individual.getMotherId());
                    if (mother != null) {
                        annotations.put("motherId", mother.getId());
                    }
                    break;
                default:
                    annotations.put(variable.getId(), individual.getFields()[fields.get(variable.getId())]);
                    break;
            }
        }


        return annotations;
    }

    protected VariableSet getVariableSetFromPedFile(Pedigree ped) throws CatalogException {
        List<Variable> variableList = new LinkedList<>();

        String category = "PEDIGREE";
        variableList.add(new Variable("family", category, Variable.VariableType.TEXT,      null, true,
                Collections.<String>emptyList(), variableList.size(), null, "", null));
        variableList.add(new Variable("id", category, Variable.VariableType.NUMERIC,       null, true,
                Collections.<String>emptyList(), variableList.size(), null, "", null));
        variableList.add(new Variable("name", category, Variable.VariableType.TEXT,        null, true,
                Collections.<String>emptyList(), variableList.size(), null, "", null));
        variableList.add(new Variable("fatherId", category, Variable.VariableType.NUMERIC, null, false,
                Collections.<String>emptyList(), variableList.size(), null, "", null));
        variableList.add(new Variable("fatherName", category, Variable.VariableType.TEXT,  null, false,
                Collections.<String>emptyList(), variableList.size(), null, "", null));
        variableList.add(new Variable("motherId", category, Variable.VariableType.NUMERIC, null, false,
                Collections.<String>emptyList(), variableList.size(), null, "", null));
        variableList.add(new Variable("motherName", category, Variable.VariableType.TEXT,  null, false,
                Collections.<String>emptyList(), variableList.size(), null, "", null));

        Set<String> allowedSexValues = new HashSet<>();
        HashSet<String> allowedPhenotypeValues = new HashSet<>();
        for (Individual individual : ped.getIndividuals().values()) {
            allowedPhenotypeValues.add(individual.getPhenotype());
            allowedSexValues.add(individual.getSex());
        }
        variableList.add(new Variable("sex", category, Variable.VariableType.CATEGORICAL,   null, true,
                new LinkedList<>(allowedSexValues), variableList.size(), null, "", null));
        variableList.add(new Variable("phenotype", category, Variable.VariableType.CATEGORICAL,    null, true,
                new LinkedList<>(allowedPhenotypeValues), variableList.size(), null, "", null));


        int categoricalThreshold = (int) (ped.getIndividuals().size()*0.1);
        for (Map.Entry<String, Integer> entry : ped.getFields().entrySet()) {
            boolean isNumerical = true;
            Set<String> allowedValues = new HashSet<>();
            for (Individual individual : ped.getIndividuals().values()) {
                String s = individual.getFields()[entry.getValue()];
                if(isNumerical) {
                    try {
                        Double.parseDouble(s);
                    } catch (Exception e) {
                        isNumerical = false;
                    }
                }
                allowedValues.add(s);
            }
            Variable.VariableType type;
            if (allowedValues.size() < categoricalThreshold) {
                float meanSize = 0;
                for (String value : allowedValues) {
                    meanSize += value.length();
                }
                meanSize /= allowedValues.size();
                float deviation = 0;
                for (String value : allowedValues) {
                    deviation += (value.length() - meanSize) * (value.length() - meanSize);
                }
                deviation /= allowedValues.size();
                if (deviation < 10) {
                    type = Variable.VariableType.CATEGORICAL;
                } else {
                    if (isNumerical) {
                        type = Variable.VariableType.NUMERIC;
                    } else {
                        type = Variable.VariableType.TEXT;
                    }
                }
            } else {
                if (isNumerical) {
                    type = Variable.VariableType.NUMERIC;
                } else {
                    type = Variable.VariableType.TEXT;
                }
            }

            if (!type.equals(Variable.VariableType.CATEGORICAL)) {
                allowedValues.clear();
            }

            variableList.add(new Variable(entry.getKey(), category, type, null, false, new ArrayList<>(allowedValues),
                    variableList.size(), null, "", null));
        }

        VariableSet variableSet = new VariableSet(-1, "", false, "", new HashSet(variableList), null);
        return variableSet;
    }

    protected Pedigree readPedigree(String fileName) {
        PedigreeReader reader = new PedigreePedReader(fileName);

        reader.open();
        reader.pre();


        List<Pedigree> read = reader.read();


        reader.post();
        reader.close();
        return read.get(0);
    }


}
