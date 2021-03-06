package org.opencb.opencga.server;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.catalog.beans.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/cohorts")
@Api(value = "cohorts", description = "Cohorts")
public class CohortWSServer extends OpenCGAWSServer {

    public CohortWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
        params = uriInfo.getQueryParameters();
    }

    @GET
    @Path("/{cohortId}/info")
    @Produces("application/json")
    @ApiOperation(value = "Get cohort information")
    public Response infoSample(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId) {
        try {
            QueryResult<Cohort> queryResult = catalogManager.getCohort(cohortId, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{cohortId}/samples")
    @Produces("application/json")
    @ApiOperation(value = "Get samples from cohort")
    public Response getSamples(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId) {
        try {
            QueryOptions queryOptions = this.getQueryOptions();
            Cohort cohort = catalogManager.getCohort(cohortId, queryOptions, sessionId).first();
            queryOptions.put("id", cohort.getSamples());
            int studyId = catalogManager.getStudyIdByCohortId(cohortId);
            QueryResult<Sample> allSamples = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
            allSamples.setId("getCohortSamples");
            return createOkResponse(allSamples);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }


    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create a cohort")
    public Response createCohort(
            @ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "name", required = true) @QueryParam("name") String cohortName,
            @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") int variableSetId,
            @ApiParam(value = "description", required = false) @QueryParam("description") String cohortDescription,
            @ApiParam(value = "sampleIds", required = false) @QueryParam("sampleIds") String sampleIdsStr,
            @ApiParam(value = "variable", required = false) @QueryParam("variable") String variableName) {
        try {
            //QueryOptions queryOptions = getAllQueryOptions();
            List<Cohort> cohorts = new LinkedList<>();
            if (variableName != null && !variableName.isEmpty() && sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                return createErrorResponse("Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            int studyId = catalogManager.getStudyId(studyIdStr);
            if (sampleIdsStr != null && !sampleIdsStr.isEmpty()) {
                QueryOptions samplesQuery = new QueryOptions("include", "projects.studies.samples.id");
                samplesQuery.add("id", sampleIdsStr);
                cohorts.add(createCohort(studyId, cohortName, cohortDescription, samplesQuery).first());
            } else {
                VariableSet variableSet = catalogManager.getVariableSet(variableSetId, null, sessionId).first();
                Variable variable = null;
                for (Variable v : variableSet.getVariables()) {
                    if (v.getId().equals(variableName)) {
                        variable = v;
                        break;
                    }
                }
                if (variable == null) {
                    return createErrorResponse("Variable " + variable  + " does not exist. ");
                }
                if (variable.getType() != Variable.VariableType.CATEGORICAL) {
                    return createErrorResponse("Can only create cohorts by variable, when is a categorical variable");
                }
                for (String s : variable.getAllowedValues()) {
                    QueryOptions samplesQuery = new QueryOptions("include", "projects.studies.samples.id");
                    samplesQuery.add("annotation", variableName + ":" + s);
                    samplesQuery.add("variableSetId", variableSetId);
                    cohorts.add(createCohort(studyId, s, cohortDescription, samplesQuery).first());
                }
            }
            return createOkResponse(cohorts);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    private QueryResult<Cohort> createCohort(int studyId, String cohortName, String cohortDescription, QueryOptions queryOptions) throws CatalogException {
        QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
        List<Integer> sampleIds = new ArrayList<>(queryResult.getNumResults());
        for (Sample sample : queryResult.getResult()) {
            sampleIds.add(sample.getId());
        }
        return catalogManager.createCohort(studyId, cohortName, cohortDescription, sampleIds, null, sessionId);
    }

//
//    @GET
//    @Path("/create")
//    @Produces("application/json")
//    @ApiOperation(value = "Create a cohort")
//    public Response createCohort(
//            @ApiParam(value = "studyId", required = true) @QueryParam("studyId") int studyId,
//            @ApiParam(value = "cohortName", required = true) @QueryParam("cohortName") String cohortName,
//            @ApiParam(value = "cohortDescription", required = false) @QueryParam("cohortDescription") String cohortDescription) {
//        try {
//            QueryOptions queryOptions = getAllQueryOptions();
////            Object cohortInclude = queryOptions.remove("include");
////            Object cohortExclude = queryOptions.remove("exclude");
//            queryOptions.add("include", "projects.studies.samples.id");
//            QueryResult<Sample> queryResult = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
//            queryOptions.remove("include");
//            List<Integer> sampleIds = new ArrayList<>(queryResult.getNumResults());
//            for (Sample sample : queryResult.getResult()) {
//                sampleIds.add(sample.getId());
//            }
//            QueryResult<Cohort> cohort = catalogManager.createCohort(studyId, cohortName, cohortDescription, sampleIds, null, sessionId);
//
//            return createOkResponse(cohort);
//        } catch (CatalogException e) {
//            e.printStackTrace();
//            return createErrorResponse(e.getMessage());
//        }
//    }
//

    @DELETE
    @Path("/{cohortId}/delete")
    @Produces("application/json")
    @ApiOperation(value = "Delete cohort. PENDING")
    public Response deleteCohort(@ApiParam(value = "cohortId", required = true) @PathParam("cohortId") int cohortId) {
        return createErrorResponse("Unimplemented");
    }


}
