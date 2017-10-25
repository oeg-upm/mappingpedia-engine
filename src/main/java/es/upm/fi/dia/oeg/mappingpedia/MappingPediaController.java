package es.upm.fi.dia.oeg.mappingpedia;

import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController;
import es.upm.fi.dia.oeg.mappingpedia.model.*;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import es.upm.fi.dia.oeg.mappingpedia.model.result.*;
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility;
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


    @RequestMapping(value="/triplesMaps", method= RequestMethod.GET)
    public ListResult getTriplesMaps() {
        logger.info("/triplesMaps ...");
        ListResult listResult = MappingPediaEngine.getAllTriplesMaps();
        logger.info("listResult = " + listResult);

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
    public ListResult getMappingDocuments(@RequestParam(value="searchType", defaultValue = "0") String searchType,
                                          @RequestParam(value="searchTerm", required = false) String searchTerm
    ) {
        logger.info("/ogd/annotations(GET) ...");
        logger.info("searchType = " + searchType);
        logger.info("searchTerm = " + searchTerm);
        ListResult listResult = MappingDocumentController.findMappingDocuments(searchType, searchTerm);
        logger.info("listResult = " + listResult);
        return listResult;
    }



    @RequestMapping(value="/executions2", method= RequestMethod.POST)
    public ExecuteMappingResult executeMapping2(
            @RequestParam("mappingURL") String mappingURL
            , @RequestParam(value="mappingLanguage", required = false) String mappingLanguage
            , @RequestParam("datasetDistributionURL") String datasetDistributionURL
            , @RequestParam(value="fieldSeparator", required = false) String fieldSeparator
            , @RequestParam(value="queryFile", required = false) String queryFile
            , @RequestParam(value="outputFilename", required = false) String outputFilename
            , @RequestParam(value="organizationId", required = false) String organizationId
            , @RequestParam(value="datasetId", required = false) String datasetId
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
        distribution.dcatDownloadURL_$eq(datasetDistributionURL);
        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        dataset.addDistribution(distribution);


        MappingDocument md = new MappingDocument();
        md.mappingLanguage_$eq(mappingLanguage);
        md.setDownloadURL(mappingURL);

        MappingExecution mappingExecution = new MappingExecution(md, dataset);
        mappingExecution.setStoreToCKAN("true");
        mappingExecution.outputFileName_$eq(outputFilename);
        mappingExecution.queryFilePath_$eq(queryFile);

        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return MappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename, true);
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
            );
            return executeMappingResult;
        }

    }

    @RequestMapping(value="/executions/{organizationId}/{datasetId}/{mappingFilename:.+}", method= RequestMethod.POST)
    public ExecuteMappingResult executeMapping1(
            @PathVariable("organizationId") String organizationId
            , @PathVariable("datasetId") String datasetId
            , @PathVariable("mappingFilename") String mappingFilename
            , @RequestParam(value="datasetFile") String datasetFile
            , @RequestParam(value="queryFile", required = false) String queryFile
            , @RequestParam(value="outputFilename", required = false) String outputFilename
    )
    {
        logger.info("POST /executions/{organizationId}/{datasetId}/{mappingFilename}");
        Organization organization = new Organization(organizationId);
        Dataset dataset = new Dataset(organization, datasetId);
        Distribution distribution = new Distribution(dataset);
        distribution.dcatDownloadURL_$eq(datasetFile);

        //String githubRepo = MappingPediaEngine.mappingpediaProperties().githubRepoContents()
        //String mappingBlobURL = githubRepo + "/blob/master/" + organizationId + "/" + datasetId + "/" + mappingFilename;

        String mappingDocumentDownloadURL = GitHubUtility.generateDownloadURL(organizationId, datasetId, mappingFilename);
        MappingDocument md = new MappingDocument();
        md.setDownloadURL(mappingDocumentDownloadURL);


        //return MappingExecutionController.executeMapping2(md, dataset, queryFile, outputFilename, true);

        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return MappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename, true);
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
            );
            return executeMappingResult;
        }


    }

    @RequestMapping(value = "/mappings/{organizationID}", method= RequestMethod.POST)
    public AddMappingDocumentResult uploadNewMapping(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile") MultipartFile mappingFileRef
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
        mappingDocument.subject_$eq(mappingDocumentSubjects);
        mappingDocument.creator_$eq(mappingDocumentCreator);
        if(mappingDocumentTitle == null) {
            mappingDocument.title_$eq(dataset.dctIdentifier());
        } else {
            mappingDocument.title_$eq(mappingDocumentTitle);
        }
        if(mappingLanguage == null) {
            mappingDocument.mappingLanguage_$eq(MappingPediaConstant.MAPPING_LANGUAGE_R2RML());
        } else {
            mappingDocument.mappingLanguage_$eq(mappingLanguage);
        }
        mappingDocument.multipartFile_$eq(mappingFileRef);

        return MappingDocumentController.uploadNewMapping(dataset, manifestFileRef
                , replaceMappingBaseURI, generateManifestFile, mappingDocument
        );
    }

    @RequestMapping(value = "/mappings/{organizationID}/{datasetID}", method= RequestMethod.POST)
    public AddMappingDocumentResult uploadNewMapping(
            @PathVariable("organizationID") String organizationID
            , @PathVariable("datasetID") String datasetID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile") MultipartFile mappingFileRef
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
        mappingDocument.subject_$eq(mappingDocumentSubjects);
        mappingDocument.creator_$eq(mappingDocumentCreator);
        if(mappingDocumentTitle == null) {
            mappingDocument.title_$eq(dataset.dctIdentifier());
        } else {
            mappingDocument.title_$eq(mappingDocumentTitle);
        }
        if(mappingLanguage == null) {
            mappingDocument.mappingLanguage_$eq(MappingPediaConstant.MAPPING_LANGUAGE_R2RML());
        } else {
            mappingDocument.mappingLanguage_$eq(mappingLanguage);
        }
        mappingDocument.multipartFile_$eq(mappingFileRef);


        return MappingDocumentController.uploadNewMapping(dataset, manifestFileRef
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
    public AddDatasetResult uploadNewDataset(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="datasetFile", required = true) MultipartFile datasetFileRef
            , @RequestParam(value="datasetTitle", required = false) String datasetTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetLanguage", required = false) String datasetLanguage
            , @RequestParam(value="datasetDescription", required = false) String datasetDescription
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionDownloadURL", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
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
        dataset.dctDescription_$eq(datasetDescription);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new Distribution(dataset);
        if(distributionAccessURL == null) {
            distribution.dcatAccessURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.dcatMediaType_$eq(distributionMediaType);
        distribution.ckanFileRef_$eq(datasetFileRef);
        if(distributionDescription == null) {
            distribution.ckanDescription_$eq("Original Dataset");
        } else {
            distribution.ckanDescription_$eq(distributionDescription);
        }
        dataset.addDistribution(distribution);


        return DatasetController.addDataset(dataset, manifestFileRef, generateManifestFile);
    }

    @RequestMapping(value = "/datasets/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
    public AddDatasetResult addNewDataset(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
            , @RequestParam(value="datasetFile", required = false) MultipartFile datasetFileRef
            , @RequestParam(value="datasetTitle") String datasetTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetPublisher") String datasetPublisher
            , @RequestParam(value="datasetLanguage") String datasetLanguage
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionDownloadURL", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @PathVariable("datasetID") String datasetID
            , @RequestParam(value="datasetDescription", required = false) String datasetDescription
    )
    {
        logger.info("[POST] /datasets/{mappingpediaUsername}/{datasetID}");
        Organization organization = new Organization(datasetPublisher);

        Dataset dataset = new Dataset(organization, datasetID);
        if(datasetTitle == null) {
            dataset.dctTitle_$eq(datasetID);
        } else {
            dataset.dctTitle_$eq(datasetTitle);
        }
        dataset.dctDescription_$eq(datasetDescription);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new Distribution(dataset);
        if(distributionAccessURL == null) {
            distribution.dcatAccessURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.dcatMediaType_$eq(distributionMediaType);
        distribution.ckanFileRef_$eq(datasetFileRef);
        dataset.addDistribution(distribution);

        return DatasetController.addDataset(dataset, manifestFileRef, generateManifestFile);
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
        logger.info("result = " + result);
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
        logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/instances", method= RequestMethod.GET)
    public ListResult getInstances(@RequestParam(value="aClass") String aClass,
                                   @RequestParam(value="outputType", defaultValue = "0") String outputType,
                                   @RequestParam(value="inputType", defaultValue = "0") String inputType


    ) {
        logger.info("GET /ogd/instances ...");
        logger.info("Getting instances of the class:" + aClass);
        ListResult result = MappingPediaEngine.getInstances(aClass, outputType, inputType) ;
        return result;
    }

}