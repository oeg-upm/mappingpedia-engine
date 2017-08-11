package es.upm.fi.dia.oeg.mappingpedia;

import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import es.upm.fi.dia.oeg.mappingpedia.model.ListResult;
import es.upm.fi.dia.oeg.mappingpedia.model.MappingPediaExecutionResult;
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
		return MappingPediaProperties.githubRepo();
	}

	@RequestMapping(value="/mappingpediaGraph", method= RequestMethod.GET)
	public String getMappingPediaGraph() {
		logger.info("/getMappingPediaGraph(GET) ...");
		return MappingPediaProperties.graphName();
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
		ListResult listResult = MappingPediaEngine.findMappingDocuments(searchType, searchTerm);
		logger.info("listResult = " + listResult);
		return listResult;
	}


	@RequestMapping(value="/githubRepoContentsURL", method= RequestMethod.GET)
	public String getGitHubRepoContentsURL() {
		logger.info("/githubRepoContentsURL(GET) ...");
		return MappingPediaProperties.githubRepoContents();
	}

	@RequestMapping(value="/executions2", method= RequestMethod.POST)
	public MappingPediaExecutionResult executeMapping2(
			@RequestParam("mappingURL") String mappingURL
			, @RequestParam(value="mappingLanguage", required = false) String mappingLanguage
			, @RequestParam("datasetDistributionURL") String datasetDistributionURL
			, @RequestParam(value="fieldSeparator", required = false) String fieldSeparator
			, @RequestParam(value="queryFile", required = false) String queryFile
			, @RequestParam(value="outputFilename", required = false) String outputFilename
	)
	{
		logger.info("POST /executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
		try {
			return MappingPediaEngine.executeMapping2(mappingURL, mappingLanguage, datasetDistributionURL, fieldSeparator
					, queryFile, outputFilename);
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			logger.error("mapping execution failed: " + errorMessage);
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
					, null, null, errorMessage, HttpURLConnection.HTTP_INTERNAL_ERROR);
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
		return MappingPediaEngine.executeMapping(mappingpediaUsername, mappingDirectory, mappingFilename
				, datasetFile, queryFile, outputFilename);
	}

	@RequestMapping(value = "/mappings/{mappingpediaUsername}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
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
		logger.info("[POST] /mappings/{mappingpediaUsername}");
		String datasetID = null;

		return MappingPediaEngine.uploadNewMapping(mappingpediaUsername, datasetID, manifestFileRef, mappingFileRef
				, replaceMappingBaseURI, generateManifestFile
				, mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
			//, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
				, mappingLanguage
		);
	}

	@RequestMapping(value = "/mappings/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
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
		return MappingPediaEngine.uploadNewMapping(mappingpediaUsername, datasetID, manifestFileRef, mappingFileRef
			    , replaceMappingBaseURI, generateManifestFile
			    , mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
			    //, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
				, mappingLanguage
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

	@RequestMapping(value = "/datasets/{mappingpediaUsername}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewDataset(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
			, @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
			, @RequestParam(value="datasetFile", required = false) MultipartFile datasetFileRef
			, @RequestParam(value="datasetTitle", defaultValue="Dataset Title") String datasetTitle
			, @RequestParam(value="datasetKeywords", defaultValue="Dataset Keywords") String datasetKeywords
			, @RequestParam(value="datasetPublisher", defaultValue="Dataset Publisher") String datasetPublisher
			, @RequestParam(value="datasetLanguage", defaultValue="Dataset Language") String datasetLanguage
			, @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
			, @RequestParam(value="distributionDownloadURL", required = false) String distributionDownloadURL
			, @RequestParam(value="distributionMediaType", required = false) String distributionMediaType
	)
	{
		logger.info("[POST] /datasets/{mappingpediaUsername}");
		logger.debug("mappingpediaUsername = " + mappingpediaUsername);

		return MappingPediaEngine.addDatasetFile(datasetFileRef, manifestFileRef, generateManifestFile, mappingpediaUsername
				, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
				, distributionAccessURL, distributionDownloadURL, distributionMediaType
		);
	}

	@RequestMapping(value = "/datasets/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
	public MappingPediaExecutionResult addNewDataset(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
			, @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
			, @RequestParam(value="datasetFile", required = false) MultipartFile datasetFileRef
			, @RequestParam(value="datasetTitle", defaultValue="Dataset Title") String datasetTitle
			, @RequestParam(value="datasetKeywords", defaultValue="Dataset Keywords") String datasetKeywords
			, @RequestParam(value="datasetPublisher", defaultValue="Dataset Publisher") String datasetPublisher
			, @RequestParam(value="datasetLanguage", defaultValue="Dataset Language") String datasetLanguage
			, @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
			, @RequestParam(value="distributionDownloadURL", required = false) String distributionDownloadURL
			, @RequestParam(value="distributionMediaType", required = false) String distributionMediaType
			, @PathVariable("datasetID") String datasetID
	)
	{
		logger.info("[POST] /datasets/{mappingpediaUsername}/{datasetID}");
		return MappingPediaEngine.addDatasetFileWithID(datasetFileRef, manifestFileRef, generateManifestFile, mappingpediaUsername
			, datasetID
			, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
			, distributionAccessURL, distributionDownloadURL, distributionMediaType
		);
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
	public ListResult getSubclasses(@RequestParam(value="aClass") String aClass,
									@RequestParam(value="outputType", defaultValue = "0") String outputType,
									@RequestParam(value="inputType", defaultValue = "0") String inputType


	) {
		logger.info("GET /ogd/utility/subclasses ...");
		logger.info("aClass = " + aClass);
		ListResult result = MappingPediaEngine.getSchemaOrgSubclasses(aClass, outputType, inputType) ;
		logger.info("result = " + result);
		return result;
	}

	@RequestMapping(value="/ogd/instances", method= RequestMethod.GET)
	public ListResult getInstances(@RequestParam(value="aClass") String aClass,
									@RequestParam(value="outputType", defaultValue = "0") String outputType,
									@RequestParam(value="inputType", defaultValue = "0") String inputType


	) {
		logger.info("GET /ogd/instances ...");
		logger.info("aClass = " + aClass);
		ListResult result = MappingPediaEngine.getInstances(aClass, outputType, inputType) ;
		logger.info("result = " + result);
		return result;
	}

}