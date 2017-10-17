package es.upm.fi.dia.oeg.mappingpedia;

import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController;
import es.upm.fi.dia.oeg.mappingpedia.model.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
//@RequestMapping(value = "/mappingpedia")
@MultipartConfig(fileSizeThreshold = 20971520)
public class MappingPediaController {
    static Logger logger = LogManager.getLogger("MappingPediaController");

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
        return MappingPediaEngine.mappingpediaProperties().githubRepo();
    }

    @RequestMapping(value="/mappingpediaGraph", method= RequestMethod.GET)
    public String getMappingPediaGraph() {
        logger.info("/getMappingPediaGraph(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().graphName();
    }

    @RequestMapping(value="/triplesMaps", method= RequestMethod.GET)
    public ListResult getTriplesMaps() {
        logger.info("/triplesMaps ...");
        ListResult listResult = MappingPediaEngine.getAllTriplesMaps();
        logger.info("listResult = " + listResult);

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


    @RequestMapping(value="/githubRepoContentsURL", method= RequestMethod.GET)
    public String getGitHubRepoContentsURL() {
        logger.info("/githubRepoContentsURL(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().githubRepoContents();
    }

    @RequestMapping(value="/executions2", method= RequestMethod.POST)
    public MappingPediaExecutionResult executeMapping2(
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
            //return MappingExecutionController.executeMapping2(md, queryFile, outputFilename, dataset, "true");
            return MappingExecutionController.executeMapping2(mappingExecution);
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = "Error occured: " + e.getMessage();
            logger.error("mapping execution failed: " + errorMessage);
            MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
                    , null, null, errorMessage, HttpURLConnection.HTTP_INTERNAL_ERROR, null);
            return executionResult;
        }

    }

    @RequestMapping(value="/executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.POST)
    public MappingPediaExecutionResult executeMapping(@PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("mappingDirectory") String mappingDirectory
            , @PathVariable("mappingFilename") String mappingFilename
            , @RequestParam(value="datasetFile") String datasetFile
            , @RequestParam(value="queryFile", required = false) String queryFile
            , @RequestParam(value="outputFilename", required = false) String outputFilename
    )
    {
        logger.info("POST /executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
        return MappingExecutionController.executeMapping1(mappingpediaUsername, mappingDirectory, mappingFilename
                , datasetFile, queryFile, outputFilename);
    }

    @RequestMapping(value = "/mappings/{organizationID}", method= RequestMethod.POST)
    public MappingPediaExecutionResult uploadNewMapping(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile") MultipartFile mappingFileRef
            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="Mapping Document Title") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="Mapping Document Creator") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="Mapping Document Subjects") String mappingDocumentSubjects
            , @RequestParam(value="mappingLanguage", required = false) String mappingLanguage

    )
    {
        logger.info("[POST] /mappings/{organizationID}");
        Organization organization = new Organization(organizationID);
        Dataset dataset = new Dataset(organization);
        MappingDocument mappingDocument = new MappingDocument();
        mappingDocument.subject_$eq(mappingDocumentSubjects);
        mappingDocument.creator_$eq(mappingDocumentCreator);
        mappingDocument.title_$eq(mappingDocumentTitle);
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
    public MappingPediaExecutionResult uploadNewMapping(
            @PathVariable("organizationID") String organizationID
            , @PathVariable("datasetID") String datasetID
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="mappingFile") MultipartFile mappingFileRef
            , @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
            , @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
            , @RequestParam(value="mappingDocumentTitle", defaultValue="Mapping Document Title") String mappingDocumentTitle
            , @RequestParam(value="mappingDocumentCreator", defaultValue="Mapping Document Creator") String mappingDocumentCreator
            , @RequestParam(value="mappingDocumentSubjects", defaultValue="Mapping Document Subjects") String mappingDocumentSubjects
            , @RequestParam(value="mappingLanguage", required = false) String mappingLanguage

    )
    {
        logger.info("[POST] /mappings/{mappingpediaUsername}/{datasetID}");
        Organization organization = new Organization(organizationID);
        Dataset dataset = new Dataset(organization, datasetID);
        MappingDocument mappingDocument = new MappingDocument();
        mappingDocument.subject_$eq(mappingDocumentSubjects);
        mappingDocument.creator_$eq(mappingDocumentCreator);
        mappingDocument.title_$eq(mappingDocumentTitle);
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
    public MappingPediaExecutionResult getMapping(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("mappingDirectory") String mappingDirectory
            , @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("GET /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
        return MappingPediaEngine.getMapping(mappingpediaUsername, mappingDirectory, mappingFilename);
    }

    @RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.PUT)
    public MappingPediaExecutionResult updateExistingMapping(
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
    public MappingPediaExecutionResult uploadNewDataset(
            @PathVariable("organizationID") String organizationID
            , @RequestParam(value="datasetFile", required = false) MultipartFile datasetFileRef
            , @RequestParam(value="datasetTitle", defaultValue="Dataset Title") String datasetTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetLanguage", defaultValue="en") String datasetLanguage
            , @RequestParam(value="datasetDescription", required = false) String datasetDescription
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionDownloadURL", required = true) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = true) String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", defaultValue="true") String generateManifestFile
    )
    {
        logger.info("[POST] /datasets/{mappingpediaUsername}");
        Organization organization = new Organization(organizationID);

        Dataset dataset = new Dataset(organization);
        dataset.dctTitle_$eq(datasetTitle);
        dataset.dctDescription_$eq(datasetDescription);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new Distribution(dataset);
        distribution.dcatAccessURL_$eq(distributionAccessURL);
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
    public MappingPediaExecutionResult addNewDataset(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
            , @RequestParam(value="datasetFile", required = false) MultipartFile datasetFileRef
            , @RequestParam(value="datasetTitle", defaultValue="Dataset Title") String datasetTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetPublisher", defaultValue="Dataset Publisher") String datasetPublisher
            , @RequestParam(value="datasetLanguage", defaultValue="Dataset Language") String datasetLanguage
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionDownloadURL", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false) String distributionMediaType
            , @PathVariable("datasetID") String datasetID
            , @RequestParam(value="datasetDescription", required = false) String datasetDescription
    )
    {
        logger.info("[POST] /datasets/{mappingpediaUsername}/{datasetID}");
        Organization organization = new Organization(datasetPublisher);

        Dataset dataset = new Dataset(organization, datasetID);
        dataset.dctTitle_$eq(datasetTitle);
        dataset.dctDescription_$eq(datasetDescription);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new Distribution(dataset);
        distribution.dcatAccessURL_$eq(distributionAccessURL);
        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.dcatMediaType_$eq(distributionMediaType);
        distribution.ckanFileRef_$eq(datasetFileRef);
        dataset.addDistribution(distribution);

        return DatasetController.addDataset(dataset, manifestFileRef, generateManifestFile);
    }

    @RequestMapping(value = "/queries/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
    public MappingPediaExecutionResult addNewQuery(
            @RequestParam("queryFile") MultipartFile queryFileRef
            , @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("datasetID") String datasetID
    )
    {
        logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
        return MappingPediaEngine.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
    }


    @RequestMapping(value = "/storeRDFFile")
    public MappingPediaExecutionResult storeRDFFile(
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