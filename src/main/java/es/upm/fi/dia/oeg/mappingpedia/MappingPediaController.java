package es.upm.fi.dia.oeg.mappingpedia;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;
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
import es.upm.fi.dia.oeg.mappingpedia.utility.CKANClient;
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility;
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility;
import org.apache.commons.io.FileUtils;
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

    private GitHubUtility githubClient = MappingPediaEngine.githubClient();
    private CKANClient ckanClient = MappingPediaEngine.ckanClient();

    private DatasetController datasetController = new DatasetController(ckanClient, githubClient);
    private DistributionController distributionController = new DistributionController(ckanClient, githubClient);
    private MappingDocumentController mappingDocumentController = new MappingDocumentController(githubClient);
    private MappingExecutionController mappingExecutionController= new MappingExecutionController(ckanClient, githubClient);

    @RequestMapping(value="/greeting", method= RequestMethod.GET)
    public Greeting greetingGET(@RequestParam(value="name", defaultValue="World") String name) {
        logger.info("/greeting(GET) ...");
        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
    }

    @RequestMapping(value="/greeting/{name}", method= RequestMethod.PUT)
    public Greeting greetingPUT(@PathVariable("name") String name) {
        logger.info("/greeting(PUT) ...");
        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
    }

    @RequestMapping(value="/githubRepoURL", method= RequestMethod.GET)
    public String getGitHubRepoURL() {
        logger.info("/githubRepo(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().githubRepository();
    }

    @RequestMapping(value="/ckanDatasetList", method= RequestMethod.GET)
    public ListResult getCKANDatasetList(@RequestParam(value="catalogUrl", required = false) String catalogUrl) {
        if(catalogUrl == null) {
            catalogUrl = MappingPediaEngine.mappingpediaProperties().ckanURL();
        }
        logger.info("GET /ckanDatasetList ...");
        return CKANClient.getDatasetList(catalogUrl);
    }

    @RequestMapping(value="/virtuosoEnabled", method= RequestMethod.GET)
    public String getVirtuosoEnabled() {
        logger.info("GET /virtuosoEnabled ...");
        return MappingPediaEngine.mappingpediaProperties().virtuosoEnabled() + "";
    }

    @RequestMapping(value="/mappingpediaGraph", method= RequestMethod.GET)
    public String getMappingPediaGraph() {
        logger.info("/getMappingPediaGraph(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().graphName();
    }

    @RequestMapping(value="/ckanActionOrganizationCreate", method= RequestMethod.GET)
    public String getCKANActionOrganizationCreate() {
        logger.info("GET //ckanActionOrganizationCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionOrganizationCreate();
    }

    @RequestMapping(value="/ckanActionPackageCreate", method= RequestMethod.GET)
    public String ckanActionPackageCreate() {
        logger.info("GET //ckanActionPackageCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionPackageCreate();
    }

    @RequestMapping(value="/ckanActionResourceCreate", method= RequestMethod.GET)
    public String getCKANActionResourceCreate() {
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

        CKANClient ckanClient = new CKANClient(ckanURL, ckanKey);
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

    @RequestMapping(value="/triplesMaps", method= RequestMethod.GET)
    public ListResult getTriplesMaps() {
        logger.info("/triplesMaps ...");
        ListResult listResult = MappingPediaEngine.getAllTriplesMaps();
        //logger.info("listResult = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/mappings/findMappingDocumentsByDatasetId", method= RequestMethod.GET)
    public ListResult findMappingDocumentsByDatasetId(
            @RequestParam(value="datasetId", defaultValue = "") String datasetId
    ) {
        logger.info("/findMappingDocumentsByDatasetId...");
        ListResult listResult = MappingDocumentController.findMappingDocumentsByDatasetId(datasetId);
        logger.info("findMappingDocumentsByDatasetId result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/ogd/annotations", method= RequestMethod.GET)
    public ListResult getAnnotations(@RequestParam(value="searchType", defaultValue = "0") String searchType,
                                          @RequestParam(value="searchTerm", required = false) String searchTerm
    ) {
        logger.info("/ogd/annotations(GET) ...");
        logger.info("searchType = " + searchType);
        logger.info("searchTerm = " + searchTerm);
        if("subclass".equalsIgnoreCase(searchType)) {
            logger.info("get all mapping documents by mapped class and its subclasses ...");
            ListResult listResult = MappingDocumentController.findMappingDocumentsByMappedSubClass(searchTerm);
            //logger.info("listResult = " + listResult);
            return listResult;
        } else {
            ListResult listResult = MappingDocumentController.findMappingDocuments(searchType, searchTerm);
            //logger.info("listResult = " + listResult);
            return listResult;
        }

    }



    @RequestMapping(value="/executions2", method= RequestMethod.POST)
    public ExecuteMappingResult executeMappingWithoutPathVariables(
            @RequestParam(value="organizationId", required = false) String organizationId
            , @RequestParam(value="datasetId", required = false) String datasetId
            , @RequestParam(value="datasetDistributionURL", required = false) String datasetDistributionURL
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL

            , @RequestParam(value="queryFile", required = false) String queryFile
            , @RequestParam(value="outputFilename", required = false) String outputFilename
            , @RequestParam(value="mappingLanguage", required = false, defaultValue="r2rml") String mappingLanguage
            , @RequestParam(value="fieldSeparator", required = false) String fieldSeparator

            , @RequestParam(value="mappingURL", required = false) String mappingURL
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL

            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType

            , @RequestParam(value="dbUserName", required = false) String dbUserName
            , @RequestParam(value="dbPassword", required = false) String dbPassword
            , @RequestParam(value="dbName", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="databaseDriver", required = false) String databaseDriver
            , @RequestParam(value="databaseType", required = false) String databaseType
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
        if(distributionDownloadURL != null) {
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatDownloadURL_$eq(datasetDistributionURL);
        }
        distribution.dcatAccessURL_$eq(distributionAccessURL);

        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        distribution.dcatMediaType_$eq(distributionMediaType);
        dataset.addDistribution(distribution);


        MappingDocument md = new MappingDocument();
        md.mappingLanguage_$eq(mappingLanguage);
        if(mappingDocumentDownloadURL != null) {
            md.setDownloadURL(mappingDocumentDownloadURL);
        } else {
            md.setDownloadURL(mappingURL);
        }


        MappingExecution mappingExecution = new MappingExecution(md, dataset);
        mappingExecution.setStoreToCKAN("true");
        mappingExecution.outputFileName_$eq(outputFilename);
        mappingExecution.queryFilePath_$eq(queryFile);

        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true
                    , dbUserName, dbPassword
                    , dbName, jdbc_url
                    , databaseDriver, databaseType);
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

    @RequestMapping(value="/executions/{organizationId}/{datasetId}/{mappingFilename:.+}", method= RequestMethod.POST)
    public ExecuteMappingResult executeMappingWithPathVariables(
            @PathVariable("organizationId") String organizationId
            , @PathVariable("datasetId") String datasetId
            , @PathVariable("mappingFilename") String mappingFilename
            , @RequestParam(value="datasetFile", required = false) String datasetFile
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL

            , @RequestParam(value="queryFile", required = false) String queryFile
            , @RequestParam(value="outputFilename", required = false) String outputFilename
            , @RequestParam(value="mappingLanguage", required = false, defaultValue="r2rml") String mappingLanguage
            , @RequestParam(value="fieldSeparator", required = false) String fieldSeparator

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
        md.mappingLanguage_$eq(mappingLanguage);


        //return MappingExecutionController.executeMapping2(md, dataset, queryFile, outputFilename, true);

        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true
                    , dbUserName, dbPassword
                    , dbName, jdbc_url
                    , databaseDriver, databaseType
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
    public AddMappingDocumentResult addNewMappingDocumentWithoutDatasetId(
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
    public AddMappingDocumentResult addNewMappingDocumentWithDatasetID(
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
            , @RequestParam(value="mappingLanguage", required = false, defaultValue="r2rml") String mappingLanguage

    )
    {
        logger.info("[POST] /mappings/{mappingpediaUsername}/{datasetID}");
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
        mappingDocument.mappingLanguage_$eq(mappingLanguage);
        if(mappingDocumentFileMultipartFile != null) {
            File mappingDocumentFile = MappingPediaUtility.multipartFileToFile(mappingDocumentFileMultipartFile, dataset.dctIdentifier());
            mappingDocument.mappingDocumentFile_$eq(mappingDocumentFile);
        } else {
            if(mappingFileMultipartFile != null) {
                File mappingDocumentFile = MappingPediaUtility.multipartFileToFile(mappingFileMultipartFile , dataset.dctIdentifier());
                mappingDocument.mappingDocumentFile_$eq(mappingDocumentFile);
            }
        }

        mappingDocument.setDownloadURL(mappingDocumentDownloadURL);


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
    public GeneralResult updateExistingMapping(
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

    @RequestMapping(value = "/datasets/{organizationID}", method= RequestMethod.POST)
    public AddDatasetResult addNewDataset(
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
        logger.info("[POST] /datasets/{mappingpediaUsername}");
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
    public AddDistributionResult addNewDataset(
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
    public AddDistributionResult addNewDistribution(
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
    public GeneralResult addNewQuery(
            @RequestParam("queryFile") MultipartFile queryFileRef
            , @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("datasetID") String datasetID
    )
    {
        logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
        return MappingPediaEngine.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
    }


    @RequestMapping(value = "/storeRDFFile")
    public GeneralResult storeRDFFile(
            @RequestParam("rdfFile") MultipartFile fileRef
            , @RequestParam(value="graphURI") String graphURI)
    {
        logger.info("/storeRDFFile...");
        return MappingPediaEngine.storeRDFFile(fileRef, graphURI);
    }

    @RequestMapping(value="/ogd/utility/subclasses", method= RequestMethod.GET)
    public ListResult getSubclassesDetail(
            @RequestParam(value="aClass") String aClass,
            @RequestParam(value="outputType", defaultValue = "0") String outputType,
            @RequestParam(value="inputType", defaultValue = "0") String inputType
    ) {
        logger.info("GET /ogd/utility/subclasses ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSchemaOrgSubclassesDetail(aClass, outputType, inputType) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/subclassesSummary", method= RequestMethod.GET)
    public ListResult getSubclassesSummary(
            @RequestParam(value="aClass") String aClass,
            @RequestParam(value="outputType", defaultValue = "0") String outputType,
            @RequestParam(value="inputType", defaultValue = "0") String inputType
    ) {
        logger.info("GET /ogd/utility/subclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSubclassesLocalNames(aClass, outputType, inputType) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/instances", method= RequestMethod.GET)
    public ListResult getInstances(@RequestParam(value="aClass") String aClass,
                                   @RequestParam(value="outputType", defaultValue = "0") String outputType,
                                   @RequestParam(value="inputType", defaultValue = "0") String inputType


    ) {
        logger.info("GET /ogd/instances ...");
        logger.info("Getting instances of the class:" + aClass);
        ListResult result = mappingExecutionController.getInstances(aClass, outputType, inputType) ;
        return result;
    }

}