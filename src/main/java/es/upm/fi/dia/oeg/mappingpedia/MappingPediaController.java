package es.upm.fi.dia.oeg.mappingpedia;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController;
import es.upm.fi.dia.oeg.mappingpedia.controller.DistributionController;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController;
import es.upm.fi.dia.oeg.mappingpedia.model.*;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import es.upm.fi.dia.oeg.mappingpedia.model.result.*;
import es.upm.fi.dia.oeg.mappingpedia.utility.*;
import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.test.Gen;
import org.apache.jena.ontology.OntModel;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
//@RequestMapping(value = "/mappingpedia")
@MultipartConfig(fileSizeThreshold = 20971520)
public class MappingPediaController {
    //static Logger logger = LogManager.getLogger("MappingPediaController");
    static Logger logger = LoggerFactory.getLogger("MappingPediaController");

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();


    private OntModel ontModel = MappingPediaEngine.ontologyModel();

    private GitHubUtility githubClient = MappingPediaEngine.githubClient();
    private CKANUtility ckanClient = MappingPediaEngine.ckanClient();
    private JenaClient jenaClient = MappingPediaEngine.jenaClient();
    private VirtuosoClient virtuosoClient = MappingPediaEngine.virtuosoClient();

    private DatasetController datasetController = new DatasetController(ckanClient, githubClient, virtuosoClient);
    private DistributionController distributionController = new DistributionController(ckanClient, githubClient, virtuosoClient);
    private MappingDocumentController mappingDocumentController = new MappingDocumentController(githubClient, virtuosoClient, jenaClient);
    private MappingExecutionController mappingExecutionController= new MappingExecutionController(ckanClient, githubClient, virtuosoClient, jenaClient);

    @RequestMapping(value="/greeting", method= RequestMethod.GET)
    public Greeting getGreeting(@RequestParam(value="name", defaultValue="World") String name) {
        logger.info("/greeting(GET) ...");
        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
    }

    @RequestMapping(value="/", method= RequestMethod.GET, produces={"application/ld+json"})
    public Inbox get() {
        logger.info("GET / ...");
        return new Inbox();
    }

    @RequestMapping(value="/", method= RequestMethod.HEAD, produces={"application/ld+json"})
    public ResponseEntity head() {
        logger.info("HEAD / ...");
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.LINK, "<http://mappingpedia-engine.linkeddata.es/inbox>; rel=\"http://www.w3.org/ns/ldp#inbox\"");

        return new ResponseEntity(headers, HttpStatus.CREATED);
    }

    @RequestMapping(value="/inbox", method= RequestMethod.POST)
    public GeneralResult postInbox(
            //@RequestParam(value="notification", required = false) Object notification)
            @RequestBody Object notification
    )
    {
        logger.info("POST /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }

    @RequestMapping(value="/inbox", method= RequestMethod.PUT)
    public GeneralResult putInbox(
            //@RequestParam(value="notification", defaultValue="") String notification
            @RequestBody Object notification
    )
    {
        logger.info("PUT /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }

    @RequestMapping(value="/mappingexecutions", method= RequestMethod.GET)
    public ListResult getMappingExecution(@RequestParam(value="mapping_document_sha", defaultValue="") String mappingDocumentSHA
            , @RequestParam(value="dataset_distribution_sha", defaultValue="") String datasetDistributionSHA) {

        logger.info("GET /mappingexecutions ...");
        logger.info("mappingDocumentSHA = " + mappingDocumentSHA);
        logger.info("datasetDistributionSHA = " + datasetDistributionSHA);

        return this.mappingExecutionController.findMappingExecutionURLByHash(
                mappingDocumentSHA, datasetDistributionSHA);
    }

    @RequestMapping(value="/greeting/{name}", method= RequestMethod.PUT)
    public Greeting putGreeting(@PathVariable("name") String name) {
        logger.info("/greeting(PUT) ...");
        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
    }

    @RequestMapping(value="/ontology/resource_details", method= RequestMethod.GET)
    public OntologyResource getOntologyResourceDetails(
            @RequestParam(value="resource") String resource) {
        logger.info("GET /ontology/resource_details ...");
        String uri = MappingPediaUtility.getClassURI(resource);

        return this.jenaClient.getDetails(uri);
    }

    @RequestMapping(value="/github_repo_url", method= RequestMethod.GET)
    public String getGitHubRepoURL() {
        logger.info("GET /github_repo_url ...");
        return MappingPediaEngine.mappingpediaProperties().githubRepository();
    }

    @RequestMapping(value="/ckan_datasets", method= RequestMethod.GET)
    public ListResult getCKANDatasets(@RequestParam(value="catalogUrl", required = false) String catalogUrl) {
        if(catalogUrl == null) {
            catalogUrl = MappingPediaEngine.mappingpediaProperties().ckanURL();
        }
        logger.info("GET /ckanDatasetList ...");
        return CKANUtility.getDatasetList(catalogUrl);
    }

    @RequestMapping(value="/virtuoso_enabled", method= RequestMethod.GET)
    public String getVirtuosoEnabled() {
        logger.info("GET /virtuosoEnabled ...");
        return MappingPediaEngine.mappingpediaProperties().virtuosoEnabled() + "";
    }

    @RequestMapping(value="/mappingpedia_graph", method= RequestMethod.GET)
    public String getMappingpediaGraph() {
        logger.info("/getMappingPediaGraph(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().graphName();
    }

    @RequestMapping(value="/ckan_api_action_organization_create", method= RequestMethod.GET)
    public String getCKANAPIActionOrganizationCreate() {
        logger.info("GET //ckanActionOrganizationCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionOrganizationCreate();
    }

    @RequestMapping(value="/ckan_api_action_package_create", method= RequestMethod.GET)
    public String getCKANAPIActionPpackageCreate() {
        logger.info("GET //ckanActionPackageCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionPackageCreate();
    }

    @RequestMapping(value="/ckan_api_action_resource_create", method= RequestMethod.GET)
    public String getCKANAPIActionResourceCreate() {
        logger.info("GET //getCKANActionResourceCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionResourceCreate();
    }

    @RequestMapping(value="/ckanResource", method= RequestMethod.POST)
    public Integer postCKANResource(
            @RequestParam(value="filePath", required = true) String filePath
            , @RequestParam(value="packageId", required = true) String packageId
    ) {
        logger.info("POST /ckanResource...");
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        CKANUtility ckanClient = new CKANUtility(ckanURL, ckanKey);
        File file = new File(filePath);
        try {
            if(!file.exists()) {
                String fileName = file.getName();
                file = new File(fileName);
                FileUtils.copyURLToFile(new URL(filePath), file);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        //return ckanUtility.createResource(file.getPath(), packageId);
        return null;
    }

    @RequestMapping(value="/dataset_language/{organizationId}", method= RequestMethod.POST)
    public Integer postDatasetLanguage(
            @PathVariable("organizationId") String organizationId
            , @RequestParam(value="dataset_language", required = true) String datasetLanguage
    ) {
        logger.info("POST /dataset_language ...");
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        CKANUtility ckanClient = new CKANUtility(ckanURL, ckanKey);
        return ckanClient.updateDatasetLanguage(organizationId, datasetLanguage);
    }

    @RequestMapping(value="/triples_maps", method= RequestMethod.GET)
    public ListResult getTriplesMaps() {
        logger.info("/triplesMaps ...");
        ListResult listResult = MappingPediaEngine.getAllTriplesMaps();
        //logger.info("listResult = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/mappings", method= RequestMethod.GET)
    public ListResult getMappings(
            @RequestParam(value="dataset_id", defaultValue = "", required = false) String datasetId
            , @RequestParam(value="ckan_package_id", defaultValue = "", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", defaultValue = "", required = false) String ckanPackageName
            , @RequestParam(value="distribution_id", defaultValue = "", required = false) String distributionId
    ) {
        logger.info("GET /mappings");
        logger.info("dataset_id = " + datasetId);
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("distribution_id = " + distributionId);

        ListResult listResult = null;
        if(!"".equals(datasetId.trim())
                || !"".equals(ckanPackageId.trim())
                || !"".equals(ckanPackageName.trim())
                ) {

            listResult = this.mappingDocumentController.findMappingDocumentsByDatasetId(
                    datasetId, ckanPackageId, ckanPackageName);
        } else if(!"".equalsIgnoreCase(distributionId.trim())) {
            listResult = this.mappingDocumentController.findMappingDocumentsByDistributionId(distributionId);
        }

        return listResult;
    }

    @RequestMapping(value="/properties", method= RequestMethod.GET)
    public ListResult getProperties(
            @RequestParam(value="class", required = false, defaultValue="Thing") String aClass
            , @RequestParam(value="direct", required = false, defaultValue="true") String direct
    )
    {
        logger.info("/properties ...");
        logger.info("this.jenaClient = " + this.jenaClient);

        ListResult listResult = this.jenaClient.getProperties(aClass, direct);

        return listResult;
    }

    @RequestMapping(value="/datasets", method= RequestMethod.GET)
    public ListResult getDatasets(
            @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName
    ) {
        logger.info("/datasets ...");
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);

        ListResult listResult;

        if(ckanPackageId != null && ckanPackageName == null) {
            listResult = this.datasetController.findDatasetsByCKANPackageId(ckanPackageId);
        } else if(ckanPackageId == null && ckanPackageName != null) {
            listResult = this.datasetController.findDatasetsByCKANPackageName(ckanPackageName);
        } else {
            listResult = this.datasetController.findAllDatasets();
        }
        logger.info("datasets result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/distributions", method= RequestMethod.GET)
    public ListResult getDistributions(
            @RequestParam(value="ckan_resource_id", required = false) String ckanResourceId
    ) {
        logger.info("/distributions ...");
        logger.info("ckan_resource_id = " + ckanResourceId);

        ListResult listResult = this.distributionController.findDistributionByCKANResourceId(ckanResourceId);

        logger.info("/distributions listResult = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/mapped_classes", method= RequestMethod.GET)
    public ListResult getMappedClasses(@RequestParam(value="prefix", required = false, defaultValue="schema.org") String prefix
            , @RequestParam(value="mapped_table", required = false) String mappedTable
            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
    ) {
        logger.info("/mapped_classes ...");
        logger.info("prefix = " + prefix);
        ListResult listResult = null;
        if(mappingDocumentId != null) {
            listResult = this.mappingDocumentController.findMappedClassesByMappingDocumentId(mappingDocumentId);
        } else if(mappedTable != null) {
            listResult = this.mappingDocumentController.findAllMappedClassesByTableName(prefix, mappedTable);
        } else {
            listResult = this.mappingDocumentController.findAllMappedClasses(prefix);
        }

        logger.info("mapped_classes result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/mapped_properties", method= RequestMethod.GET)
    public ListResult getMappedProperty(@RequestParam(value="prefix", required = false, defaultValue="schema.org") String prefix
    ) {
        logger.info("/mapped_properties ...");
        logger.info("prefix = " + prefix);
        ListResult listResult = this.mappingDocumentController.findAllMappedProperties(prefix);
        logger.info("mapped_properties result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/ogd/annotations", method= RequestMethod.GET)
    public ListResult getOGDAnnotations(
            //@RequestParam(value="searchType", defaultValue = "0") String searchType,
            @RequestParam(value="class", required = false) String searchedClass
            , @RequestParam(value="property", required = false) String searchedProperty
            , @RequestParam(value="subclass", required = false, defaultValue="true") String subclass

    ) {
        logger.info("/ogd/annotations(GET) ...");
        logger.info("searchedClass = " + searchedClass);
        logger.info("searchedProperty = " + searchedProperty);

        if("true".equalsIgnoreCase(subclass)) {
            logger.info("get all mapping documents by mapped class and its subclasses ...");
/*            ListResult listResult = this.mappingDocumentController.findMappingDocumentsByMappedClass(
                    searchClass, true);*/
            ListResult listResult = this.mappingDocumentController.findMappingDocumentsByMappedClassAndProperty(
                    searchedClass, searchedProperty, true);

            //logger.info("listResult = " + listResult);
            return listResult;
        } else {
            //ListResult listResult = this.mappingDocumentController.findMappingDocuments(searchType, searchTerm);
            ListResult listResult = this.mappingDocumentController.findMappingDocumentsByMappedClass(searchedClass);

            //logger.info("listResult = " + listResult);
            return listResult;
        }

    }


    @RequestMapping(value="/executions", method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions(
            @RequestParam(value="organization_id", required = false) String organizationId
            , @RequestParam(value="dataset_id", required = false) String datasetId

            , @RequestParam(value="ckan_resources_ids", required = false) String ckanResourcesIds
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String pDistributionDownloadURL
            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage
            , @RequestParam(value="mapping_document_hash", required = false) String mappingDocumentHash

            , @RequestParam(value="query_file", required = false) String queryFile
            , @RequestParam(value="output_filename", required = false) String outputFilename
            , @RequestParam(value="output_fileextension", required = false) String outputFileExtension
            , @RequestParam(value="output_mediatype", required = false) String outputMediaType

            , @RequestParam(value="db_username", required = false) String dbUserName
            , @RequestParam(value="db_password", required = false) String dbPassword
            , @RequestParam(value="db_name", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="database_driver", required = false) String databaseDriver
            , @RequestParam(value="database_type", required = false) String databaseType

            , @RequestParam(value="use_cache", required = false) String pUseCache
            , @RequestParam(value="callback_url", required = false) String callbackURL
            , @RequestParam(value="callback_field", required = false) String callbackField

    )
    {
        try {
            logger.info("\n\n\nPOST /executions");
            logger.info("organization_id = " + organizationId);
            logger.info("dataset_id = " + datasetId);
            logger.info("distributionDownloadURL = " + pDistributionDownloadURL);
            logger.info("ckan_resources_ids = " + ckanResourcesIds);
            logger.info("mapping_document_id = " + mappingDocumentId);
            logger.info("mappingDocumentDownloadURL = " + mappingDocumentDownloadURL);
            logger.info("distribution_encoding = " + distributionEncoding);
            logger.info("use_cache = " + pUseCache);
            logger.info("output_filename = " + outputFilename);
            logger.info("output_fileextension = " + outputFileExtension);
            logger.info("output_mediatype = " + outputMediaType);
            logger.info("callback_url = " + callbackURL);

            MappingDocument md;
            if(mappingDocumentId != null) {
                //md = new MappingDocument(mappingDocumentId);
                md = this.mappingDocumentController.findMappingDocumentsByMappingDocumentId(mappingDocumentId);
            } else {
                md = new MappingDocument();
            }
            logger.info("md.dctIdentifier() = " + md.dctIdentifier());

            if(mappingDocumentDownloadURL != null) {
                md.setDownloadURL(mappingDocumentDownloadURL);
            } else {
                if(mappingDocumentId != null) {
                    MappingDocument foundMappingDocument = this.mappingDocumentController.findMappingDocumentsByMappingDocumentId(mappingDocumentId);
                    md.setDownloadURL(foundMappingDocument.getDownloadURL());
                }
            }
            logger.debug("md.getDownloadURL() = " + md.getDownloadURL());
            String mdDownloadURL = md.getDownloadURL();

            if(mappingDocumentHash == null && mdDownloadURL != null) {
                md.hash_$eq(MappingPediaUtility.calculateHash(
                        mdDownloadURL, "UTF-8"));
            }
            logger.debug("md.sha = " + md.hash());

            if(pMappingLanguage != null) {
                md.mappingLanguage_$eq(pMappingLanguage);
            } else {
                String mappingLanguage = MappingDocumentController.detectMappingLanguage(mappingDocumentDownloadURL);
                md.mappingLanguage_$eq(mappingLanguage);
            }
            logger.debug("md.getMapping_language() = " + md.getMapping_language());

            String[] arrayDistributionDownloadURLs = null;
            if(pDistributionDownloadURL != null) {
                arrayDistributionDownloadURLs = pDistributionDownloadURL.split(",");
            }
            logger.debug("arrayDistributionDownloadURLs = " + Arrays.toString(arrayDistributionDownloadURLs));


            Agent organization;
            if(organizationId == null) {
                organization = new Agent();
            } else {
                organization = new Agent(organizationId);
            }

            Dataset dataset;
            if(datasetId == null) {
                dataset = new Dataset(organization);
            } else {
                dataset = new Dataset(organization, datasetId);
            }

/*            Dataset unannotatedDataset = new Dataset(organization);
            unannotatedDataset.superset_$eq(originalDataset);*/

            for(String distributionDownloadURL:arrayDistributionDownloadURLs) {
                String distributionDownloadURLTrimmed = distributionDownloadURL.trim();
                logger.debug("distributionDownloadURLTrimmed = " + distributionDownloadURLTrimmed);

                UnannotatedDistribution unannotatedDistribution = new UnannotatedDistribution(dataset);
                if(distributionAccessURL != null) {
                    unannotatedDistribution.dcatAccessURL_$eq(distributionAccessURL);
                }
                if(distributionDownloadURLTrimmed != null) {
                    unannotatedDistribution.dcatDownloadURL_$eq(distributionDownloadURLTrimmed);
                } else {
                    unannotatedDistribution.dcatDownloadURL_$eq(this.githubClient.getDownloadURL(distributionAccessURL));
                }
                if(fieldSeparator != null) {
                    unannotatedDistribution.csvFieldSeparator_$eq(fieldSeparator);
                }
                unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);


            }

            JDBCConnection jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                    , dbName, jdbc_url
                    , databaseDriver, databaseType);


            Boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);

            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(
                    md, dataset
                    , queryFile
                    , outputFilename, outputFileExtension, outputMediaType
                    ,  true, true
                    , true
                    , null, useCache
                    , callbackURL, callbackField
            );

            /*
        MappingExecution mappingExecution = new MappingExecution(md, dataset);
        mappingExecution.setStoreToCKAN("true");
        mappingExecution.outputFileName_$eq(outputFilename);
        mappingExecution.queryFilePath_$eq(queryFile);
        return MappingExecutionController.executeMapping2(mappingExecution);
*/
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = "Error occured: " + e.getMessage();
            logger.error("mapping execution failed: " + errorMessage);
            ExecuteMappingResult executeMappingResult = new ExecuteMappingResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
            );
            return executeMappingResult;
        }
    }

/*    //TODO REFACTOR THIS; MERGE /executions with /executions2
    //@RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingFilename:.+}"
//            , method= RequestMethod.POST)
    @RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingDocumentId}"
            , method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions1(
            @PathVariable("organization_id") String organizationId

            , @PathVariable("dataset_id") String datasetId
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage

            , @RequestParam(value="query_file", required = false) String queryFile
            , @RequestParam(value="output_filename", required = false) String outputFilename

            , @RequestParam(value="db_username", required = false) String dbUserName
            , @RequestParam(value="db_password", required = false) String dbPassword
            , @RequestParam(value="db_name", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="database_driver", required = false) String databaseDriver
            , @RequestParam(value="database_type", required = false) String databaseType

            , @RequestParam(value="use_cache", required = false) String pUseCache
            //, @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("POST /executions1/{organizationId}/{datasetId}/{mappingDocumentId}");
        logger.info("mapping_document_id = " + mappingDocumentId);

        Agent organization = new Agent(organizationId);

        Dataset dataset = new Dataset(organization, datasetId);
        Distribution distribution = new Distribution(dataset);
        if(distributionAccessURL != null) {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        if(distributionDownloadURL != null) {
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatDownloadURL_$eq(this.githubClient.getDownloadURL(distributionAccessURL));
        }
        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        distribution.dcatMediaType_$eq(distributionMediaType);
        dataset.addDistribution(distribution);


        MappingDocument md = new MappingDocument();
        if(mappingDocumentDownloadURL != null) {
            md.setDownloadURL(mappingDocumentDownloadURL);
        } else {
            if(mappingDocumentId != null) {
                MappingDocument foundMappingDocument = this.mappingDocumentController.findMappingDocumentsByMappingDocumentId(mappingDocumentId);
                md.setDownloadURL(foundMappingDocument.getDownloadURL());
            } else {
                //I don't know that to do here, Ahmad will handle
            }
        }

        if(pMappingLanguage != null) {
            md.mappingLanguage_$eq(pMappingLanguage);
        } else {
            String mappingLanguage = MappingDocumentController.detectMappingLanguage(mappingDocumentDownloadURL);
            logger.info("mappingLanguage = " + mappingLanguage);
            md.mappingLanguage_$eq(mappingLanguage);
        }


        JDBCConnection jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                , dbName, jdbc_url
                , databaseDriver, databaseType);


        Boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);
        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true, jdbcConnection
                    , useCache

            );
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = "Error occured: " + e.getMessage();
            logger.error("mapping execution failed: " + errorMessage);
            ExecuteMappingResult executeMappingResult = new ExecuteMappingResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error"
                    , null, null
                    , null
                    , null, null
                    , null
                    , null
                    , null, null
            );
            return executeMappingResult;
        }
    }*/

    @RequestMapping(value = "/mappings/{organization_id}", method= RequestMethod.POST)
    public AddMappingDocumentResult postMappings2(
            @PathVariable("organization_id") String organizationID

            , @RequestParam(value="dataset_id", required = false) String pDatasetID
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName

            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile", required = false) MultipartFile mappingFileMultipartFile
            , @RequestParam(value="mapping_document_file", required = false) MultipartFile mappingDocumentFileMultipartFile
            , @RequestParam(value="mapping_document_url", required = false) String pMappingDocumentDownloadURL1
            , @RequestParam(value="mappingDocumentDownloadURL", required = false) String pMappingDocumentDownloadURL2
            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="true") String generateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="") String mappingDocumentSubjects
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage1
            , @RequestParam(value="mappingLanguage", required = false) String pMappingLanguage2

            , @RequestParam(value="ckan_resource_id", required = false, defaultValue="") String ckanResourceId
    )
    {
        logger.info("[POST] /mappings/{organization_id}");
        logger.info("organization_id = " + organizationID);
        logger.info("dataset_id = " + pDatasetID);
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("mappingFile = " + mappingFileMultipartFile);
        logger.info("mapping_document_file = " + mappingDocumentFileMultipartFile);
        logger.info("mapping_document_url = " + pMappingDocumentDownloadURL1);


        try {
            String datasetId = pDatasetID;

            if(datasetId == null) {
                if(ckanPackageId != null) {
                    ListResult datasetsByCKANPackageId = this.datasetController.findDatasetsByCKANPackageId(ckanPackageId);
                    if(datasetsByCKANPackageId != null && datasetsByCKANPackageId.results().size() > 0) {
                        Dataset dataset = (Dataset) datasetsByCKANPackageId.results().iterator().next();
                        datasetId =  dataset.getId();
                    }
                }
            }

            if(datasetId == null) {
                ListResult datasetsByCKANPackageName = this.datasetController.findDatasetsByCKANPackageName(ckanPackageName);
                if(datasetsByCKANPackageName != null && datasetsByCKANPackageName.results().size() > 0) {
                    Dataset dataset = (Dataset) datasetsByCKANPackageName.results().iterator().next();
                    datasetId =  dataset.getId();
                }
            }

            if(datasetId == null) {
                logger.warn("datasetId = " + datasetId);
            }


            return this.postMappings1(organizationID
                    , datasetId, ckanPackageId, ckanPackageName
                    , manifestFileRef
                    , mappingFileMultipartFile, mappingDocumentFileMultipartFile, pMappingDocumentDownloadURL1, pMappingDocumentDownloadURL2
                    , replaceMappingBaseURI, generateManifestFile
                    , mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
                    , pMappingLanguage1, pMappingLanguage2
                    , ckanResourceId);

        } catch (Exception e) {
            e.printStackTrace();
            return new AddMappingDocumentResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
                    , null
                    , null, null
            );
        }
    }



    @RequestMapping(value = "/mappings/{organization_id}/{dataset_id}", method= RequestMethod.POST)
    public AddMappingDocumentResult postMappings1(
            @PathVariable("organization_id") String organizationID

            , @PathVariable("dataset_id") String datasetID
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName

            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef

            , @RequestParam(value="mappingFile", required = false) MultipartFile mappingFileMultipartFile
            , @RequestParam(value="mapping_document_file", required = false) MultipartFile mappingDocumentFileMultipartFile
            , @RequestParam(value="mapping_document_download_url", required = false) String pMappingDocumentDownloadURL1
            , @RequestParam(value="mappingDocumentDownloadURL", required = false) String pMappingDocumentDownloadURL2

            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="true") String generateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="") String mappingDocumentSubjects
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage1
            , @RequestParam(value="mappingLanguage", required = false) String pMappingLanguage2

            , @RequestParam(value="ckan_resource_id", required = false, defaultValue="") String ckanResourceId
    )
    {
        logger.info("[POST] /mappings/{organization_id}/{dataset_id}");
        logger.info("organization_id = " + organizationID);
        logger.info("dataset_id = " + datasetID);
        logger.debug("mapping_language = " + pMappingLanguage1);
        logger.debug("mappingLanguage = " + pMappingLanguage2);
        logger.debug("ckanPackageId = " + ckanPackageId);
        logger.debug("ckanResourceId = " + ckanResourceId);

        try {
            /*
            Agent organization = new Agent(organizationID);
            Dataset dataset = new Dataset(organization, datasetID);
            */
            Dataset dataset = Dataset.apply(organizationID, datasetID);
            dataset.ckanPackageId_$eq(ckanPackageId);

            MappingDocument mappingDocument = new MappingDocument();
            mappingDocument.ckanPackageId_$eq(ckanPackageId);
            mappingDocument.ckanResourceId_$eq(ckanResourceId);
            mappingDocument.dctSubject_$eq(mappingDocumentSubjects);
            mappingDocument.dctCreator_$eq(mappingDocumentCreator);
            mappingDocument.setTitle(mappingDocumentTitle, mappingDocument.dctIdentifier());
            mappingDocument.setMappingLanguage(pMappingLanguage1, pMappingLanguage2);

            mappingDocument.setFile(mappingDocumentFileMultipartFile, mappingFileMultipartFile
                    , dataset.dctIdentifier());
            if(mappingDocument.mappingLanguage() == null && mappingDocument.mappingDocumentFile() != null) {
                String inferredMappingLanguage = MappingDocumentController.detectMappingLanguage(
                        mappingDocument.mappingDocumentFile());
                mappingDocument.mappingLanguage_$eq(inferredMappingLanguage);
            }

            mappingDocument.setDownloadURL(pMappingDocumentDownloadURL1, pMappingDocumentDownloadURL2);
            if(mappingDocument.mappingLanguage() == null && mappingDocument.getDownloadURL() != null) {
                String inferredMappingLanguage = MappingDocumentController.detectMappingLanguage(
                        mappingDocument.getDownloadURL());
                mappingDocument.mappingLanguage_$eq(inferredMappingLanguage);
            }

            logger.info("mappingDocument.mappingLanguage() = " + mappingDocument.mappingLanguage());
            return mappingDocumentController.addNewMappingDocument(dataset, manifestFileRef
                    , replaceMappingBaseURI, generateManifestFile, mappingDocument
            );
        } catch (Exception e) {
            e.printStackTrace();

            return new AddMappingDocumentResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
                    , null
                    , null, null
            );
        }

    }

    @RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.GET)
    public GeneralResult getMapping(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("mappingDirectory") String mappingDirectory
            , @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("GET /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
        return MappingPediaEngine.getMapping(mappingpediaUsername, mappingDirectory, mappingFilename);
    }

    @RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.PUT)
    public GeneralResult putMappings(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("mappingDirectory") String mappingDirectory
            , @PathVariable("mappingFilename") String mappingFilename
            , @RequestParam(value="mappingFile") MultipartFile mappingFileRef
    )
    {
        logger.info("PUT /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
        return MappingPediaEngine.updateExistingMapping(mappingpediaUsername, mappingDirectory, mappingFilename
                , mappingFileRef);
    }

    @RequestMapping(value = "/datasets_mappings_execute", method= RequestMethod.POST)
    public AddDatasetMappingExecuteResult postDatasetsAndMappingsThenExecute(
            @RequestParam("organization_id") String organizationID

            , @RequestParam(value="dataset_title", required = false) String datasetTitle
            , @RequestParam(value="dataset_keywords", required = false) String datasetKeywords
            , @RequestParam(value="dataset_language", required = false, defaultValue="en") String datasetLanguage
            , @RequestParam(value="dataset_description", required = false) String datasetDescription

            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distribution_file", required = false) MultipartFile distributionMultipartFile
            , @RequestParam(value="distribution_media_type", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding

            , @RequestParam(value="mapping_document_access_url", required = false) String mappingDocumentAccessURL
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_document_file", required = false) MultipartFile mappingDocumentMultipartFile
            , @RequestParam(value="mapping_document_subject", required = false, defaultValue="") String mappingDocumentSubject
            , @RequestParam(value="mapping_document_title", required = false) String mappingDocumentTitle
            , @RequestParam(value="mapping_language", required = false, defaultValue="r2rml") String mappingLanguage

            , @RequestParam(value="execute_mapping", required = false, defaultValue="true") String executeMapping
            , @RequestParam(value="query_file_download_url", required = false) String queryFileDownloadURL
            , @RequestParam(value="output_file_name", required = false) String outputFilename
            , @RequestParam(value="output_fileextension", required = false) String outputFileExtension
            , @RequestParam(value="output_mediatype", required = false) String outputMediaType

            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile

            , @RequestParam(value="use_cache", required = false, defaultValue="true") String pUseCache
            , @RequestParam(value="callback_url", required = false) String callbackURL
            , @RequestParam(value="callback_field", required = false) String callbackField
    )
    {
        logger.info("[POST] /datasets_mappings_execute");

        Agent organization = new Agent(organizationID);

        Dataset dataset = new Dataset(organization);
        if(datasetTitle == null) {
            dataset.dctTitle_$eq(dataset.dctIdentifier());
        } else {
            dataset.dctTitle_$eq(datasetTitle);
        }
        if(datasetDescription == null) {
            dataset.dctDescription_$eq(dataset.dctIdentifier());
        } else {
            dataset.dctDescription_$eq(datasetDescription);
        }
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        UnannotatedDistribution unannotatedDistribution = null;
        if(distributionDownloadURL != null ||  distributionMultipartFile != null) {
            unannotatedDistribution = new UnannotatedDistribution(dataset);

            if(distributionAccessURL == null) {
                unannotatedDistribution.dcatAccessURL_$eq(distributionDownloadURL);
            } else {
                unannotatedDistribution.dcatAccessURL_$eq(distributionAccessURL);
            }
            unannotatedDistribution.dcatDownloadURL_$eq(distributionDownloadURL);

            if(distributionMultipartFile != null) {
                unannotatedDistribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                        distributionMultipartFile , dataset.dctIdentifier()));
            }

            unannotatedDistribution.dctDescription_$eq("Distribution for the dataset: " + dataset.dctIdentifier());
            unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);
            unannotatedDistribution.encoding_$eq(distributionEncoding);
            dataset.addDistribution(unannotatedDistribution);
        }


        AddDatasetResult addDatasetResult = this.datasetController.addDataset(
                dataset, manifestFileRef, generateManifestFile, "true");
        int addDatasetResultStatusCode = addDatasetResult.getStatus_code();
        if(addDatasetResultStatusCode >= 200 && addDatasetResultStatusCode < 300) {
            MappingDocument mappingDocument = new MappingDocument();
            mappingDocument.dctSubject_$eq(mappingDocumentSubject);
            mappingDocument.dctCreator_$eq(organizationID);
            mappingDocument.accessURL_$eq(mappingDocumentAccessURL);
            if(mappingDocumentTitle == null) {
                mappingDocument.dctTitle_$eq(dataset.dctIdentifier());
            } else {
                mappingDocument.dctTitle_$eq(mappingDocumentTitle);
            }
            mappingDocument.mappingLanguage_$eq(mappingLanguage);
            if(mappingDocumentMultipartFile != null) {
                File mappingDocumentFile = MappingPediaUtility.multipartFileToFile(mappingDocumentMultipartFile, dataset.dctIdentifier());
                mappingDocument.mappingDocumentFile_$eq(mappingDocumentFile);
            }

            mappingDocument.setDownloadURL(mappingDocumentDownloadURL);


            AddMappingDocumentResult addMappingDocumentResult = mappingDocumentController.addNewMappingDocument(dataset, manifestFileRef
                    , "true", generateManifestFile, mappingDocument);
            int addMappingDocumentResultStatusCode = addMappingDocumentResult.getStatus_code();

            boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);
            if("true".equalsIgnoreCase(executeMapping)) {
                if(addMappingDocumentResultStatusCode >= 200 && addMappingDocumentResultStatusCode < 300) {
                    try {
                        ExecuteMappingResult executeMappingResult =
                                this.mappingExecutionController.executeMapping(
                                        mappingDocument
                                        , dataset
                                        , queryFileDownloadURL
                                        , outputFilename, outputFileExtension, outputMediaType

                                        , true
                                        , true
                                        , true

                                        , null
                                        , useCache
                                        , callbackURL, callbackField

                                );

                        return new AddDatasetMappingExecuteResult (HttpURLConnection.HTTP_OK, addDatasetResult, addMappingDocumentResult, executeMappingResult);



                    } catch (Exception e){
                        e.printStackTrace();
                        return new AddDatasetMappingExecuteResult (HttpURLConnection.HTTP_INTERNAL_ERROR, addDatasetResult, addMappingDocumentResult, null);

                    }
                } else {
                    return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR, addDatasetResult, addMappingDocumentResult, null);
                }
            } else {
                return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR,addDatasetResult, addMappingDocumentResult, null);
            }

        } else {
            return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR, addDatasetResult, null, null);
        }
    }

    @RequestMapping(value = "/datasets/{organization_id}", method= RequestMethod.POST)
    public AddDatasetResult postDatasets(
            @PathVariable("organization_id") String organizationId

            //FIELDS RELATED TO DATASET/PACKAGE
            , @RequestParam(value="dataset_id", required = false) String datasetID
            , @RequestParam(value="dataset_title", required = false) String pDatasetTitle1
            , @RequestParam(value="datasetTitle", required = false) String pDatasetTitle2
            , @RequestParam(value="dataset_keywords", required = false) String pDatasetKeywords1
            , @RequestParam(value="datasetKeywords", required = false) String pDatasetKeywords2
            , @RequestParam(value="dataset_category", required = false) String datasetCategory
            , @RequestParam(value="dataset_language", required = false) String pDatasetLanguage1
            , @RequestParam(value="datasetLanguage", required = false) String pDatasetLanguage2
            , @RequestParam(value="dataset_description", required = false) String pDatasetDescription1
            , @RequestParam(value="datasetDescription", required = false) String pDatasetDescription2
            , @RequestParam(value="source", required = false, defaultValue = "") String ckanSource
            , @RequestParam(value="version", required = false, defaultValue = "") String ckanVersion
            , @RequestParam(value="author_name", required = false, defaultValue = "") String ckanAuthorName
            , @RequestParam(value="author_email", required = false, defaultValue = "") String ckanAuthorEmail
            , @RequestParam(value="maintainer_name", required = false, defaultValue = "") String ckanMaintainerName
            , @RequestParam(value="maintainer_email", required = false, defaultValue = "") String ckanMaintainerEmail
            , @RequestParam(value="temporal", required = false, defaultValue = "") String ckanTemporal
            , @RequestParam(value="spatial", required = false, defaultValue = "") String ckanSpatial
            , @RequestParam(value="accrual_periodicity", required = false, defaultValue = "") String ckanAccrualPeriodicity
            , @RequestParam(value="access_right", required = false, defaultValue = "") String accessRight
            , @RequestParam(value="provenance", required = false, defaultValue = "") String provenance
            , @RequestParam(value="dataset_license", required = false) String datasetLicense

            //FIELDS RELATED TO DISTRIBUTION/RESOURCE
            , @RequestParam(value="distribution_file", required = false) MultipartFile pDistributionFile1
            , @RequestParam(value="distributionFile", required = false) MultipartFile pDistributionFile2
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding

            //FIELDS RELATED TO PROV
            , @RequestParam(value="was_attributed_to", required = false) String provWasAttributedTo
            , @RequestParam(value="was_generated_by", required = false) String provWasGeneratedBy
            , @RequestParam(value="was_derived_from", required = false) String provWasDerivedFrom
            , @RequestParam(value="specialization_of", required = false) String provSpecializationOf
            , @RequestParam(value="had_primary_source", required = false) String provHadPrimarySource
            , @RequestParam(value="was_revision_of", required = false) String provWasRevisionOf
            , @RequestParam(value="was_influenced_by", required = false) String provWasInfluencedBy


            //OTHER FIELDS
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="store_to_ckan", required = false, defaultValue = "true") String pStoreToCKAN
    )
    {
        logger.info("[POST] /datasets/{organization_id}");
        logger.info("organization_id = " + organizationId);
        logger.debug("dataset_id = " + datasetID);
        logger.info("distribution_download_url = " + distributionDownloadURL);
        logger.info("distribution_file = " + pDistributionFile1);
        logger.info("datasetLicense = " + datasetLicense);

        logger.info("was_attributed_to = " + provWasAttributedTo);
        logger.info("was_generated_by = " + provWasGeneratedBy);
        logger.info("was_derived_from = " + provWasDerivedFrom);
        logger.info("specialization_of = " + provSpecializationOf);
        logger.info("had_primary_source = " + provHadPrimarySource);
        logger.info("was_revision_of = " + provWasRevisionOf);
        logger.info("was_influenced_by = " + provWasInfluencedBy);

        try {
            Dataset dataset = Dataset.apply(organizationId, datasetID);
            dataset.setTitle(pDatasetTitle1, pDatasetTitle2);
            dataset.setDescription(pDatasetDescription1, pDatasetDescription2);
            dataset.setKeywords(pDatasetKeywords1, pDatasetKeywords2);
            dataset.setLanguage(pDatasetLanguage1, pDatasetLanguage2);
            dataset.mvpCategory_$eq(datasetCategory);
            dataset.dctSource_$eq(ckanSource);
            dataset.ckanVersion_$eq(ckanVersion);
            dataset.setAuthor(ckanAuthorName, ckanAuthorEmail);
            dataset.setMaintainer(ckanMaintainerName, ckanMaintainerEmail);
            dataset.ckanTemporal_$eq(ckanTemporal);
            dataset.ckanSpatial_$eq(ckanSpatial);
            dataset.ckanAccrualPeriodicity_$eq(ckanAccrualPeriodicity);
            dataset.dctAccessRight_$eq(accessRight);
            dataset.dctProvenance_$eq(provenance);
            dataset.ckanPackageLicense_$eq(datasetLicense);

            dataset.provWasAttributedTo_$eq(provWasAttributedTo);
            dataset.provWasGeneratedBy_$eq(provWasGeneratedBy);
            dataset.provWasDerivedFrom_$eq(provWasDerivedFrom);
            dataset.provSpecializationOf_$eq(provSpecializationOf);
            dataset.provHadPrimarySource_$eq(provHadPrimarySource);
            dataset.provWasRevisionOf_$eq(provWasRevisionOf);
            dataset.provWasInfluencedBy_$eq(provWasInfluencedBy);


            if(distributionDownloadURL != null || pDistributionFile1 != null
                    || pDistributionFile2 != null) {
                Distribution distribution = new UnannotatedDistribution(dataset);
                distribution.setDistributionFile(pDistributionFile1, pDistributionFile2);
                distribution.dcatDownloadURL_$eq(distributionDownloadURL);
                distribution.setDescription(distributionDescription);
                distribution.dcatMediaType_$eq(distributionMediaType);
                distribution.encoding_$eq(distributionEncoding);
                dataset.addDistribution(distribution);
            }

            return this.datasetController.addDataset(dataset, manifestFileRef
                    , generateManifestFile, pStoreToCKAN);
        } catch(Exception e) {
            return new AddDatasetResult(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
                    , null
                    , null, null
                    , null, null
            );
        }

    }

    //LEGACY ENDPOINT, use /distributions/{organizationID}/{datasetID} instead
    @RequestMapping(value = "/datasets/{organization_id}/{dataset_id}", method= RequestMethod.POST)
    public AddDistributionResult postDatasets(
            @PathVariable("organization_id") String organizationId
            , @PathVariable("dataset_id") String datasetId
            , @RequestParam(value="datasetFile", required = false) MultipartFile distributionFileRef
            , @RequestParam(value="datasetTitle", required = false) String distributionTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetPublisher", required = false) String datasetPublisher
            , @RequestParam(value="datasetLanguage", required = false) String datasetLanguage
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile

            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType


            , @RequestParam(value="datasetDescription", required = false) String distributionDescription
            , @RequestParam(value="store_to_ckan", defaultValue = "true") String pStoreToCKAN
    )
    {
        logger.info("[POST] /datasets/{organization_id}/{dataset_id}");
        logger.info("organization_id = " + organizationId);
        logger.info("dataset_id = " + datasetId);

        Agent organization = new Agent(organizationId);

        Dataset dataset = new Dataset(organization, datasetId);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new UnannotatedDistribution(dataset);
        if(distributionTitle == null) {
            distribution.dctTitle_$eq(distribution.dctIdentifier());
        } else {
            distribution.dctTitle_$eq(distributionTitle);
        }
        if(distributionDescription == null) {
            distribution.dctDescription_$eq(distribution.dctIdentifier());
        } else {
            distribution.dctDescription_$eq(distributionDescription);
        }
        if(distributionAccessURL == null) {
            distribution.dcatAccessURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.dcatMediaType_$eq(distributionMediaType);
        if(distributionFileRef != null) {
            distribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                    distributionFileRef , dataset.dctIdentifier()));
        }
        dataset.addDistribution(distribution);

        boolean storeToCKAN = true;
        if("false".equalsIgnoreCase("pStoreToCKAN")
                || "true".equalsIgnoreCase(pStoreToCKAN)) {
            storeToCKAN = false;
        }

        return this.distributionController.addDistribution(distribution, manifestFileRef
                , generateManifestFile, storeToCKAN);
    }

    @RequestMapping(value = "/distributions/{organization_id}/{dataset_id}", method= RequestMethod.POST)
    public AddDistributionResult postDistributions(
            @PathVariable("organization_id") String organizationID
            , @PathVariable("dataset_id") String datasetID
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile
            //, @RequestParam(value="datasetFile", required = false) MultipartFile datasetMultipartFile
            , @RequestParam(value="distribution_file", required = false) MultipartFile distributionMultipartFile
            , @RequestParam(value="distribution_title", required = false) String distributionTitle
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding
            , @RequestParam(value="store_to_ckan", defaultValue = "true") String pStoreToCKAN
            , @RequestParam(value="distribution_language", required = false) String distributionLanguage
            , @RequestParam(value="distribution_license", required = false) String distributionLicense
            , @RequestParam(value="distribution_rights", required = false) String distributionRights

    )
    {
        logger.info("[POST] /distributions/{organization_id}/{dataset_id}");
        logger.info("organization_id = " + organizationID);
        logger.info("dataset_id = " + datasetID);
        logger.info("distribution_download_url = " + distributionDownloadURL);
        logger.info("distribution_file = " + distributionMultipartFile);

        Agent organization = new Agent(organizationID);

        Dataset dataset = new Dataset(organization, datasetID);

        Distribution distribution = new UnannotatedDistribution(dataset);
        distribution.setTitle(distributionTitle);
        distribution.setDescription(distributionDescription);
        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.setAccessURL(distributionAccessURL, distributionDownloadURL);
        distribution.dcatMediaType_$eq(distributionMediaType);
        if(distributionMultipartFile != null) {
            distribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                    distributionMultipartFile , dataset.dctIdentifier()));
        }
        distribution.encoding_$eq(distributionEncoding);
        distribution.setLanguage(distributionLanguage);
        distribution.dctLicense_$eq(distributionLicense);
        distribution.dctRights_$eq(distributionRights);
        dataset.addDistribution(distribution);

        boolean storeToCKAN = true;
        if("false".equalsIgnoreCase("pStoreToCKAN")
                || "no".equalsIgnoreCase(pStoreToCKAN)) {
            storeToCKAN = false;
        }

        return this.distributionController.addDistribution(distribution, manifestFileRef
                , generateManifestFile, storeToCKAN);
    }

    @RequestMapping(value = "/queries/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
    public GeneralResult postQueries(
            @RequestParam("queryFile") MultipartFile queryFileRef
            , @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("datasetID") String datasetID
    )
    {
        logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
        return MappingPediaEngine.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
    }


    @RequestMapping(value = "/rdf_file", method= RequestMethod.POST)
    public GeneralResult postRDFFile(
            @RequestParam("rdfFile") MultipartFile fileRef
            , @RequestParam(value="graphURI") String graphURI)
    {
        logger.info("/storeRDFFile...");
        return MappingPediaEngine.storeRDFFile(fileRef, graphURI);
    }

    @RequestMapping(value="/ogd/utility/subclasses", method= RequestMethod.GET)
    public ListResult getSubclassesDetails(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclasses ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSchemaOrgSubclassesDetail(aClass) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/subclassesSummary", method= RequestMethod.GET)
    public ListResult getSubclassesSummary(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSubclassesSummary(aClass) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/superclassesSummary", method= RequestMethod.GET)
    public ListResult getSuperclassesSummary(@RequestParam(value="aClass") String aClass) {
        logger.info("GET /ogd/utility/superclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = jenaClient.getSuperclasses(aClass);
        return result;
    }

    @RequestMapping(value="/ogd/instances", method= RequestMethod.GET)
    public ListResult getOGDInstances(@RequestParam(value="aClass") String aClass
            ,@RequestParam(value="maximum_results", defaultValue = "2") String pMaxMappingDocuments
            ,@RequestParam(value="use_cache", defaultValue = "true") String pUseCache

    ) {
        logger.info("GET /ogd/instances ...");
        logger.info("Getting instances of the class:" + aClass);
        logger.info("pMaxMappingDocuments = " + pMaxMappingDocuments);
        logger.info("use_cache = " + pUseCache);


        int maxMappingDocuments = 2;
        boolean useCache = true;
        try {
            maxMappingDocuments = Integer.parseInt(pMaxMappingDocuments);
            useCache = MappingPediaUtility.stringToBoolean(pUseCache);

        } catch (Exception e) {
            logger.error("invalid value for maximum_mapping_documents!");
        }
        ListResult result = mappingExecutionController.getInstances(
                aClass, maxMappingDocuments, useCache) ;
        return result;
    }

}