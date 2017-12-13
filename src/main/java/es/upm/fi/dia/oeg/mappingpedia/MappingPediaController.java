package es.upm.fi.dia.oeg.mappingpedia;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
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
import org.apache.jena.ontology.OntModel;
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

    private DatasetController datasetController = new DatasetController(ckanClient, githubClient);
    private DistributionController distributionController = new DistributionController(ckanClient, githubClient);
    private MappingDocumentController mappingDocumentController = new MappingDocumentController(githubClient, virtuosoClient, jenaClient);
    private MappingExecutionController mappingExecutionController= new MappingExecutionController(ckanClient, githubClient, virtuosoClient, jenaClient);

    @RequestMapping(value="/greeting", method= RequestMethod.GET)
    public Greeting getGreeting(@RequestParam(value="name", defaultValue="World") String name) {
        logger.info("/greeting(GET) ...");
        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
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
            , @RequestParam(value="distribution_id", defaultValue = "", required = false) String distributionId
    ) {
        ListResult listResult = null;
        if(!"".equalsIgnoreCase(datasetId.trim())) {
            logger.info("/findMappingDocumentsByDatasetId...");
            logger.info("dataset_id = " + datasetId);


            listResult = this.mappingDocumentController.findMappingDocumentsByDatasetId(datasetId);
        } else if(!"".equalsIgnoreCase(distributionId.trim())) {
            logger.info("/findMappingDocumentsByDistributionId...");
            logger.info("distribution_id = " + distributionId);


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
    public ListResult getDatasets() {
        logger.info("/datasets ...");
        ListResult listResult = DatasetController.findDatasets();
        logger.info("datasets result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/mapped_classes", method= RequestMethod.GET)
    public ListResult getMappedClasses(@RequestParam(value="prefix", required = false, defaultValue="schema.org") String prefix
                                       , @RequestParam(value="mapped_table", required = false) String mappedTable
    ) {
        logger.info("/mapped_classes ...");
        logger.info("prefix = " + prefix);
        ListResult listResult = null;
        if(mappedTable == null) {
             listResult = this.mappingDocumentController.findAllMappedClasses(prefix);
        } else {
            listResult = this.mappingDocumentController.findAllMappedClassesByTableName(prefix, mappedTable);
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



    //TODO REFACTOR THIS; MERGE /executions with /executions2
    @RequestMapping(value="/executions2", method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions2(
            @RequestParam(value="organization_id", required = false) String organizationId
            , @RequestParam(value="dataset_id", required = false) String datasetId
            //, @RequestParam(value="datasetDistributionURL", required = false) String datasetDistributionURL
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = true) String distributionDownloadURL

            , @RequestParam(value="query_file", required = false) String queryFile
            , @RequestParam(value="output_filename", required = false) String outputFilename
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId

            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType

            , @RequestParam(value="db_username", required = false) String dbUserName
            , @RequestParam(value="db_password", required = false) String dbPassword
            , @RequestParam(value="db_name", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="database_driver", required = false) String databaseDriver
            , @RequestParam(value="database_type", required = false) String databaseType
    )
    {
        logger.info("POST /executions2");

        Organization organization;
        if(organizationId == null) {
            organization = new Organization();
        } else {
            organization = new Organization(organizationId);
        }

        Dataset dataset;
        if(datasetId == null) {
            dataset = new Dataset(organization);
        } else {
            dataset = new Dataset(organization, datasetId);
        }
        Distribution distribution = new Distribution(dataset);

        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.dcatAccessURL_$eq(distributionAccessURL);

        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        distribution.dcatMediaType_$eq(distributionMediaType);
        dataset.addDistribution(distribution);





        MappingDocument md = new MappingDocument();
        if(pMappingLanguage != null) {
            md.mappingLanguage_$eq(pMappingLanguage);
        } else {
            String mappingLanguage = MappingDocumentController.detectMappingLanguage(mappingDocumentDownloadURL);
            logger.info("mappingLanguage = " + mappingLanguage);
            md.mappingLanguage_$eq(mappingLanguage);
        }


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


        MappingExecution mappingExecution = new MappingExecution(md, dataset);
        mappingExecution.setStoreToCKAN("true");
        mappingExecution.outputFileName_$eq(outputFilename);
        mappingExecution.queryFilePath_$eq(queryFile);

        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true
                    , null);
            //return MappingExecutionController.executeMapping2(mappingExecution);
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

    }

    //TODO REFACTOR THIS; MERGE /executions with /executions2
    @RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingFilename:.+}", method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions1(
            @PathVariable("organizationId") String organizationId
            , @PathVariable("datasetId") String datasetId
            , @PathVariable("mappingFilename") String mappingFilename
            , @RequestParam(value="datasetFile", required = false) String datasetFile
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL

            , @RequestParam(value="queryFile", required = false) String queryFile
            , @RequestParam(value="outputFilename", required = false) String outputFilename
            , @RequestParam(value="mappingLanguage", required = false) String pMappingLanguage
            , @RequestParam(value="fieldSeparator", required = false) String fieldSeparator

            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId

            , @RequestParam(value="distributionMediaType", required = false
            , defaultValue="text/csv") String distributionMediaType

            , @RequestParam(value="dbUserName", required = false) String dbUserName
            , @RequestParam(value="dbPassword", required = false) String dbPassword
            , @RequestParam(value="dbName", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="databaseDriver", required = false) String databaseDriver
            , @RequestParam(value="databaseType", required = false) String databaseType
    )
    {
        logger.info("POST /executions/{organizationId}/{datasetId}/{mappingFilename}");
        Organization organization = new Organization(organizationId);
        Dataset dataset = new Dataset(organization, datasetId);
        Distribution distribution = new Distribution(dataset);
        distribution.dcatMediaType_$eq(distributionMediaType);
        if(distributionAccessURL != null) {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        } else {
            distribution.dcatAccessURL_$eq(datasetFile);
        }
        if(distributionDownloadURL != null) {
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatDownloadURL_$eq(this.githubClient.getDownloadURL(distributionAccessURL));
        }

        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        dataset.addDistribution(distribution);

        //String githubRepo = MappingPediaEngine.mappingpediaProperties().githubRepoContents()
        //String mappingBlobURL = githubRepo + "/blob/master/" + organizationId + "/" + datasetId + "/" + mappingFilename;

        String mappingDocumentDownloadURL = GitHubUtility.generateDownloadURL(organizationId, datasetId, mappingFilename);
        MappingDocument md = new MappingDocument();
        md.setDownloadURL(mappingDocumentDownloadURL);
        String mappingLanguage = MappingDocumentController.detectMappingLanguage(pMappingLanguage);
        logger.info("mappingLanguage = " + mappingLanguage);
        md.mappingLanguage_$eq(mappingLanguage);

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
        //return MappingExecutionController.executeMapping2(md, dataset, queryFile, outputFilename, true);

        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            JDBCConnection jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                    , dbName, jdbc_url
                    , databaseDriver, databaseType);

            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true, jdbcConnection
            );
            //return MappingExecutionController.executeMapping2(mappingExecution);
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


    }

    @RequestMapping(value = "/mappings/{organizationID}", method= RequestMethod.POST)
    public AddMappingDocumentResult postMappings(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile", required = false) MultipartFile mappingFileRef
            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="") String mappingDocumentSubjects
            , @RequestParam(value="mappingLanguage", required = false, defaultValue="r2rml") String mappingLanguage

    )
    {
        logger.info("[POST] /mappings/{organizationID}");
        Organization organization = new Organization(organizationID);
        Dataset dataset = new Dataset(organization);
        MappingDocument mappingDocument = new MappingDocument();
        mappingDocument.dctSubject_$eq(mappingDocumentSubjects);
        mappingDocument.dctCreator_$eq(mappingDocumentCreator);
        if(mappingDocumentTitle == null) {
            mappingDocument.dctTitle_$eq(dataset.dctIdentifier());
        } else {
            mappingDocument.dctTitle_$eq(mappingDocumentTitle);
        }
        if(mappingLanguage == null) {
            mappingDocument.mappingLanguage_$eq(MappingPediaConstant.MAPPING_LANGUAGE_R2RML());
        } else {
            mappingDocument.mappingLanguage_$eq(mappingLanguage);
        }
        if(mappingFileRef != null) {
            File mappingDocumentFile = MappingPediaUtility.multipartFileToFile(mappingFileRef , dataset.dctIdentifier());
            mappingDocument.mappingDocumentFile_$eq(mappingDocumentFile);
        }

        return mappingDocumentController.addNewMappingDocument(dataset, manifestFileRef
                , replaceMappingBaseURI, generateManifestFile, mappingDocument
        );
    }

    @RequestMapping(value = "/mappings/{organizationID}/{datasetID}", method= RequestMethod.POST)
    public AddMappingDocumentResult postMappings(
            @PathVariable("organizationID") String organizationID
            , @PathVariable("datasetID") String datasetID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile", required = false) MultipartFile mappingFileMultipartFile
            , @RequestParam(value="mapping_document_file", required = false) MultipartFile mappingDocumentFileMultipartFile
            , @RequestParam(value="mappingDocumentDownloadURL", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="true") String generateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="") String mappingDocumentSubjects
            , @RequestParam(value="mappingLanguage", required = false) String pMappingLanguage

    )
    {
        logger.info("[POST] /mappings/{mappingpediaUsername}/{datasetID}");
        logger.info("pMappingLanguage = " + pMappingLanguage);

        Organization organization = new Organization(organizationID);
        Dataset dataset = new Dataset(organization, datasetID);

        MappingDocument mappingDocument = new MappingDocument();
        mappingDocument.dctSubject_$eq(mappingDocumentSubjects);
        mappingDocument.dctCreator_$eq(mappingDocumentCreator);
        if(mappingDocumentTitle == null) {
            mappingDocument.dctTitle_$eq(dataset.dctIdentifier());
        } else {
            mappingDocument.dctTitle_$eq(mappingDocumentTitle);
        }

        //mappingDocument.mappingLanguage_$eq(mappingLanguage);
        if(pMappingLanguage != null) {
            mappingDocument.mappingLanguage_$eq(pMappingLanguage);
        }

        File mappingDocumentFile = null;
        if(mappingDocumentFileMultipartFile != null) {
            mappingDocumentFile = MappingPediaUtility.multipartFileToFile(
                    mappingDocumentFileMultipartFile, dataset.dctIdentifier());
        } else if(mappingFileMultipartFile != null) {
            mappingDocumentFile = MappingPediaUtility.multipartFileToFile(
                    mappingFileMultipartFile , dataset.dctIdentifier());
        }
        if(mappingDocumentFile != null) {
            mappingDocument.mappingDocumentFile_$eq(mappingDocumentFile);
            if(pMappingLanguage == null) {
                String mappingLanguage = MappingDocumentController.detectMappingLanguage(
                        mappingDocumentFile.getAbsolutePath());
                mappingDocument.mappingLanguage_$eq(mappingLanguage);
            }
        }

        if(mappingDocumentDownloadURL != null) {
            mappingDocument.setDownloadURL(mappingDocumentDownloadURL);
            if(pMappingLanguage == null) {
                String mappingLanguage = MappingDocumentController.detectMappingLanguage(
                        mappingDocumentDownloadURL);
                mappingDocument.mappingLanguage_$eq(mappingLanguage);
            }
        }



        logger.info("mappingDocument.mappingLanguage() = " + mappingDocument.mappingLanguage());
        return mappingDocumentController.addNewMappingDocument(dataset, manifestFileRef
                , replaceMappingBaseURI, generateManifestFile, mappingDocument
        );
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
    public AddDatasetMappingExecuteResult postDatastsMappingsExecute(
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

            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile
    )
    {
        logger.info("[POST] /datasets_mappings_execute");

        Organization organization = new Organization(organizationID);

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

        if(distributionDownloadURL != null ||  distributionMultipartFile != null) {
            Distribution distribution = new Distribution(dataset);

            if(distributionAccessURL == null) {
                distribution.dcatAccessURL_$eq(distributionDownloadURL);
            } else {
                distribution.dcatAccessURL_$eq(distributionAccessURL);
            }
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);

            if(distributionMultipartFile != null) {
                distribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                        distributionMultipartFile , dataset.dctIdentifier()));
            }

            distribution.dctDescription_$eq("Distribution for the dataset: " + dataset.dctIdentifier());
            distribution.dcatMediaType_$eq(distributionMediaType);
            distribution.encoding_$eq(distributionEncoding);
            dataset.addDistribution(distribution);
        }


        AddDatasetResult addDatasetResult = this.datasetController.addDataset(dataset, manifestFileRef, generateManifestFile);
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
            if("true".equalsIgnoreCase(executeMapping)) {
                if(addMappingDocumentResultStatusCode >= 200 && addMappingDocumentResultStatusCode < 300) {
                    try {
                        ExecuteMappingResult executeMappingResult = this.mappingExecutionController.executeMapping(
                                mappingDocument
                                , dataset
                                , queryFileDownloadURL
                                , outputFilename

                                , true
                                , true
                                , true

                                , null
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

    @RequestMapping(value = "/datasets/{organizationID}", method= RequestMethod.POST)
    public AddDatasetResult postDatasets(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="datasetFile", required = false) MultipartFile datasetMultipartFile
            , @RequestParam(value="distribution_file", required = false) MultipartFile distributionMultipartFile
            , @RequestParam(value="datasetTitle", required = false) String datasetTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetLanguage", required = false) String datasetLanguage
            , @RequestParam(value="datasetDescription", required = false) String datasetDescription
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
            , @RequestParam(value="distributionEncoding", required = false, defaultValue="UTF-8") String distributionEncoding
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile
    )
    {
        logger.info("[POST] /datasets/{organizationID}");
        Organization organization = new Organization(organizationID);

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

        if(distributionDownloadURL != null || datasetMultipartFile != null || distributionMultipartFile != null) {
            Distribution distribution = new Distribution(dataset);

            if(distributionAccessURL == null) {
                distribution.dcatAccessURL_$eq(distributionDownloadURL);
            } else {
                distribution.dcatAccessURL_$eq(distributionAccessURL);
            }
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);

            if(distributionMultipartFile != null) {
                distribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                        distributionMultipartFile , dataset.dctIdentifier()));
            } else if(datasetMultipartFile != null){
                distribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                        datasetMultipartFile , dataset.dctIdentifier()));
            }

            if(distributionDescription == null) {
                distribution.dctDescription_$eq("Original Dataset");
            } else {
                distribution.dctDescription_$eq(distributionDescription);
            }

            distribution.dcatMediaType_$eq(distributionMediaType);
            distribution.encoding_$eq(distributionEncoding);
            dataset.addDistribution(distribution);

        }


        return this.datasetController.addDataset(dataset, manifestFileRef, generateManifestFile);
    }

    //LEGACY ENDPOINT, use /distributions/{organizationID}/{datasetID} instead
    @RequestMapping(value = "/datasets/{organizationID}/{datasetID}", method= RequestMethod.POST)
    public AddDistributionResult postDatasets(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile
            , @RequestParam(value="datasetFile", required = false) MultipartFile distributionFileRef
            , @RequestParam(value="datasetTitle", required = false) String distributionTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetPublisher", required = false) String datasetPublisher
            , @RequestParam(value="datasetLanguage", required = false) String datasetLanguage
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionDownloadURL", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @PathVariable("datasetID") String datasetID
            , @RequestParam(value="datasetDescription", required = false) String distributionDescription
    )
    {
        logger.info("[POST] /datasets/{organizationID}/{datasetID}");
        Organization organization = new Organization(organizationID);

        Dataset dataset = new Dataset(organization, datasetID);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new Distribution(dataset);
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

        return this.distributionController.addDistribution(distribution, manifestFileRef, generateManifestFile);
    }

    @RequestMapping(value = "/distributions/{organizationID}/{datasetID}", method= RequestMethod.POST)
    public AddDistributionResult postDistributions(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String generateManifestFile
            , @RequestParam(value="datasetFile", required = false) MultipartFile datasetMultipartFile
            , @RequestParam(value="distribution_file", required = false) MultipartFile distributionMultipartFile
            , @RequestParam(value="datasetTitle", required = false) String distributionTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetPublisher", required = false) String datasetPublisher
            , @RequestParam(value="datasetLanguage", required = false) String datasetLanguage
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionDownloadURL", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @PathVariable("datasetID") String datasetID
            , @RequestParam(value="datasetDescription", required = false) String distributionDescription
    )
    {
        logger.info("[POST] /datasets/{organizationID}/{datasetID}");
        Organization organization = new Organization(organizationID);

        Dataset dataset = new Dataset(organization, datasetID);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new Distribution(dataset);
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
        if(distributionMultipartFile != null) {
            distribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                    distributionMultipartFile , dataset.dctIdentifier()));
        } else {
            distribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                    datasetMultipartFile , dataset.dctIdentifier()));
        }
        dataset.addDistribution(distribution);

        return this.distributionController.addDistribution(distribution, manifestFileRef, generateManifestFile);
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
    public ListResult getOGDInstances(@RequestParam(value="aClass") String aClass,
                                   @RequestParam(value="outputType", defaultValue = "0") String outputType,
                                   @RequestParam(value="inputType", defaultValue = "0") String inputType


    ) {
        logger.info("GET /ogd/instances ...");
        logger.info("Getting instances of the class:" + aClass);
        ListResult result = mappingExecutionController.getInstances(aClass, jenaClient) ;
        return result;
    }

}