package org.graylog.plugins.enterprise.search.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.graylog.plugins.enterprise.search.Query;
import org.graylog.plugins.enterprise.search.QueryJob;
import org.graylog.plugins.enterprise.search.QueryResult;
import org.graylog.plugins.enterprise.search.db.QueryDbService;
import org.graylog.plugins.enterprise.search.db.QueryJobService;
import org.graylog.plugins.enterprise.search.engine.QueryEngine;
import org.graylog2.plugin.rest.PluginRestResource;
import org.graylog2.shared.rest.resources.RestResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

// TODO permission system
@Api(value = "Enterprise/Search", description = "Searching")
@Path("/search")
@Produces(MediaType.APPLICATION_JSON)
@RequiresAuthentication
public class SearchResource extends RestResource implements PluginRestResource {
    private static final Logger LOG = LoggerFactory.getLogger(SearchResource.class);

    private static final String BASE_PATH = "plugins/org.graylog.plugins.enterprise/search";

    private final QueryEngine queryEngine;
    private final QueryDbService queryDbService;
    private final QueryJobService queryJobService;
    private final ObjectMapper objectMapper;

    @Inject
    public SearchResource(QueryEngine queryEngine, QueryDbService queryDbService, QueryJobService queryJobService, ObjectMapper objectMapper) {
        this.queryEngine = queryEngine;
        this.queryDbService = queryDbService;
        this.queryJobService = queryJobService;
        this.objectMapper = objectMapper;
    }

    @POST
    @ApiOperation(value = "Create a search query", response = Query.class, code = 201)
    public Response createQuery(@ApiParam Query query) {

        // TODO validate query
        query = query.withSearchTypeIds();

        final Query saved = queryDbService.save(query);
        if (saved == null || saved.id() == null) {
            return Response.serverError().build();
        }
        LOG.info("Created new search object {}", saved.id());
        final Query annotated = saved.withInfo(queryEngine.parse(saved));
        //noinspection ConstantConditions
        return Response.created(URI.create(annotated.id())).entity(annotated).build();
    }

    @GET
    @ApiOperation(value = "Retrieve a search query")
    @Path("{id}")
    public Query getQuery(@ApiParam(name = "id") @PathParam("id") String queryId) {
        return queryDbService.get(queryId)
                .map(query -> query.withInfo(queryEngine.parse(query)))
                .orElseThrow(() -> new NotFoundException("No such search query " + queryId));
    }

    @GET
    @ApiOperation(value = "Get all current search queries in the system")
    public List<Query> getAllQueries() {
        // TODO should be paginated and limited to own (or visible queries)
        return queryDbService.streamAll()
                .map(query -> query.withInfo(queryEngine.parse(query)))
                .collect(Collectors.toList());
    }

    @POST
    @ApiOperation(value = "Execute the referenced search query asynchronously",
            notes = "Starts a new search, irrespective whether or not another is already running")
    @Path("{id}/execute")
    public Response executeQuery(@Context UriInfo uriInfo,
                                 @ApiParam(name = "id") @PathParam("id") String id,
                                 @ApiParam Map<String, Object> executionState) {
        Query query = getQuery(id);
        query = query.applyExecutionState(objectMapper, executionState);

        final QueryJob queryJob = queryJobService.create(query);

        queryEngine.execute(queryJob);

        return Response.created(URI.create(BASE_PATH + "/status/" + queryJob.getId()))
                .entity(ImmutableMap.of("job_id", queryJob.getId()))
                .build();
    }


    @GET
    @ApiOperation(value = "Retrieve the status of an executed query")
    @Path("status/{jobId}")
    public QueryResult jobStatus(@ApiParam(name = "jobId") @PathParam("jobId") String jobId) {
        final QueryJob queryJob = queryJobService.load(jobId).orElseThrow(NotFoundException::new);

        final CompletableFuture<QueryResult> future = queryJob.getResultFuture();

        return future.join();
    }
}
