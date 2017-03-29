package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.File;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;

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

	@RequestMapping(value="/githubRepoContentsURL", method= RequestMethod.GET)
	public String getGitHubRepoContentsURL() {
		logger.info("/githubRepoContentsURL(GET) ...");
		return MappingPediaProperties.githubRepoContents();
	}

	@RequestMapping(value="/executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.POST)
	public MappingPediaExecutionResult executeMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @PathVariable("mappingDirectory") String mappingDirectory
			, @PathVariable("mappingFilename") String mappingFilename
			, @RequestParam(value="datasetFile") String datasetFile
			, @RequestParam(value="queryFile") String queryFile
			, @RequestParam(value="outputFilename", required = false) String outputFilename
	)
	{
		logger.info("POST /executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
		return MappingPediaR2RML.executeMapping(mappingpediaUsername, mappingDirectory, mappingFilename
				, datasetFile, queryFile, outputFilename);
	}

	@RequestMapping(value = "/mappings/{mappingpediaUsername}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
			, @RequestParam(value="mappingFile") MultipartFile mappingFileRef
			, @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
	)
	{
		logger.info("[POST] /mappings/{mappingpediaUsername}");
		logger.debug("mappingpediaUsername = " + mappingpediaUsername);

		// Path where the uploaded files will be stored.
		String uuid = UUID.randomUUID().toString();
		logger.debug("uuid = " + uuid);
		return this.uploadNewMapping(mappingpediaUsername, uuid, manifestFileRef, mappingFileRef, replaceMappingBaseURI);
	}

	@RequestMapping(value = "/mappings/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @PathVariable("datasetID") String datasetID
			, @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
			, @RequestParam(value="mappingFile") MultipartFile mappingFileRef
			, @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI

	)
	{
		logger.info("/mappings/{mappingpediaUsername}/{datasetID}");
		logger.debug("mappingpediaUsername = " + mappingpediaUsername);
		logger.debug("datasetID = " + datasetID);

		String newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS() + datasetID + "/";

		try {
			String manifestFilePath = null;
			if(manifestFileRef != null) {
				File manifestFile = MappingPediaUtility.multipartFileToFile(manifestFileRef, datasetID);
				manifestFilePath = manifestFile.getPath();
			}

			File mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, datasetID);
			String mappingFilePath = mappingFile.getPath();


			MappingPediaRunner.run(manifestFilePath, null, mappingFilePath, null, "false"
					, Application.mappingpediaR2RML, replaceMappingBaseURI, newMappingBaseURI);

			String commitMessage = "add new mapping by mappingpedia-engine";
			String mappingContent = MappingPediaRunner.getMappingContent(manifestFilePath, null, mappingFilePath, null);
			String base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent);

			logger.info("storing mapping file in github ...");
			HttpResponse<JsonNode> response = GitHubUtility.putEncodedContent(
					MappingPediaProperties.githubUser(), MappingPediaProperties.githubAccessToken()
					, mappingpediaUsername, datasetID, mappingFile.getName()
					, commitMessage, base64EncodedContent
			);
			logger.debug("response.getHeaders = " + response.getHeaders());
			logger.debug("response.getBody = " + response.getBody());

			int responseStatus = response.getStatus();
			logger.debug("responseStatus = " + responseStatus);
			String responseStatusText = response.getStatusText();
			logger.debug("responseStatusText = " + responseStatusText);
			if(HttpURLConnection.HTTP_CREATED == responseStatus) {
				String githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.debug("githubMappingURL = " + githubMappingURL);
				logger.info("mapping inserted.");
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(manifestFilePath, null, githubMappingURL
						, null ,null, responseStatusText, responseStatus);
				return executionResult;
			} else {
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(manifestFilePath, null, null
						, null , null , responseStatusText, responseStatus);
				return executionResult;
			}
		} catch (Exception e) {
			String errorMessage = e.getMessage();
			logger.error("error uploading a new mapping file: " + errorMessage);
			Integer errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
					,null , null, errorMessage, errorCode);
			return executionResult;
		}
	}

	@RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.GET)
	public MappingPediaExecutionResult getMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
		, @PathVariable("mappingDirectory") String mappingDirectory
		, @PathVariable("mappingFilename") String mappingFilename
	)
	{
		logger.info("GET /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
		logger.debug("mappingpediaUsername = " + mappingpediaUsername);
		logger.debug("mappingDirectory = " + mappingDirectory);
		logger.debug("mappingFilename = " + mappingFilename);
		HttpResponse<JsonNode> response = GitHubUtility.getFile(
				MappingPediaProperties.githubUser(), MappingPediaProperties.githubAccessToken()
			, mappingpediaUsername, mappingDirectory, mappingFilename
		);
		int responseStatus = response.getStatus();
		logger.debug("responseStatus = " + responseStatus);
		String responseStatusText = response.getStatusText();
		logger.debug("responseStatusText = " + responseStatusText);
		if(HttpURLConnection.HTTP_OK == responseStatus) {
			String githubMappingURL = response.getBody().getObject().getString("url");
			logger.debug("githubMappingURL = " + githubMappingURL);
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, githubMappingURL
					, null, null, responseStatusText, responseStatus);
			return executionResult;			
		} else {
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
					, null, null, responseStatusText, responseStatus);
			return executionResult;			
		}
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
		logger.debug("mappingpediaUsername = " + mappingpediaUsername);
		logger.debug("mappingDirectory = " + mappingDirectory);
		logger.debug("mappingFilename = " + mappingFilename);
		logger.debug("mappingFileRef = " + mappingFileRef);

		try {
			File mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, mappingDirectory);
			String mappingFilePath = mappingFile.getPath();
			logger.debug("mapping file path = " + mappingFilePath);

			String commitMessage = "Mapping modification by mappingpedia-engine.Application";
			String mappingContent = MappingPediaRunner.getMappingContent(null, null, mappingFilePath, null);
			String base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent);
			HttpResponse<JsonNode> response = GitHubUtility.putEncodedContent(
					MappingPediaProperties.githubUser(), MappingPediaProperties.githubAccessToken()
					, mappingpediaUsername, mappingDirectory, mappingFilename
					, commitMessage, base64EncodedContent
			);
			int responseStatus = response.getStatus();
			logger.debug("responseStatus = " + responseStatus);
			String responseStatusText = response.getStatusText();
			logger.debug("responseStatusText = " + responseStatusText);
			if(HttpURLConnection.HTTP_OK == responseStatus) {
				String githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.debug("githubMappingURL = " + githubMappingURL);
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, githubMappingURL
						, null, null , responseStatusText, responseStatus);
				return executionResult;
			} else {
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
						, null, null, responseStatusText, responseStatus);
				return executionResult;
			}
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "error processing the uploaded mapping file: " + e.getMessage();
			logger.error(errorMessage);
			Integer errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
					, null,null, errorMessage, errorCode);
			return executionResult;
		}
	}

	@RequestMapping(value = "/datasets/{mappingpediaUsername}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewDataset(
			@RequestParam("datasetFile") MultipartFile datasetFileRef
			, @PathVariable("mappingpediaUsername") String mappingpediaUsername
	)
	{
		logger.info("[POST] /datasets/{mappingpediaUsername}");
		logger.debug("mappingpediaUsername = " + mappingpediaUsername);

		// Path where the uploaded files will be stored.
		String datasetID = UUID.randomUUID().toString();
		return this.addNewDataset(datasetFileRef
				, mappingpediaUsername
				, datasetID);
	}

	@RequestMapping(value = "/queries/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
	public MappingPediaExecutionResult addNewQuery(
			@RequestParam("queryFile") MultipartFile queryFileRef
			, @PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @PathVariable("datasetID") String datasetID
	)
	{
		logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
		return MappingPediaR2RML.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
	}

	@RequestMapping(value = "/datasets/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
	public MappingPediaExecutionResult addNewDataset(
			@RequestParam("datasetFile") MultipartFile datasetFileRef
			, @PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @PathVariable("datasetID") String datasetID
	)
	{
		logger.info("[POST] /datasets/{mappingpediaUsername}/{datasetID}");
		return MappingPediaR2RML.addDatasetFile(datasetFileRef, mappingpediaUsername, datasetID);

		/*
		logger.debug("mappingpediaUsername = " + mappingpediaUsername);
		logger.debug("datasetID = " + datasetID);
		try {
			File datasetFile = MappingPediaUtility.multipartFileToFile(datasetFileRef, datasetID);

			logger.info("storing a dataset file in github ...");
			String commitMessage = "Add a new dataset by mappingpedia-engine";
			HttpResponse<JsonNode> response = GitHubUtility.putEncodedFile(
					MappingPediaProperties.githubUser(), MappingPediaProperties.githubAccessToken()
					, mappingpediaUsername, datasetID, datasetFile.getName()
					, commitMessage, datasetFile
			);
			logger.debug("response.getHeaders = " + response.getHeaders());
			logger.debug("response.getBody = " + response.getBody());

			int responseStatus = response.getStatus();
			logger.debug("responseStatus = " + responseStatus);
			String responseStatusText = response.getStatusText();
			logger.debug("responseStatusText = " + responseStatusText);
			if(HttpURLConnection.HTTP_CREATED == responseStatus) {
				String datasetURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.debug("githubMappingURL = " + datasetURL);
				logger.info("dataset stored.");
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, datasetURL, null
						, responseStatusText, responseStatus);
				return executionResult;				
			} else {
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
						, responseStatusText, responseStatus);
				return executionResult;
			}
		} catch (Exception e) {
			String errorMessage = e.getMessage();
			logger.error("error uploading a new mapping file: " + errorMessage);
			Integer errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
					, errorMessage, errorCode);
			return executionResult;
		}
		*/
	}
	

	@RequestMapping(value = "/storeRDFFile")
	public MappingPediaExecutionResult storeRDFFile(@RequestParam("rdfFile") MultipartFile fileRef
			, @RequestParam(value="graphURI") String graphURI)
	{
		logger.info("/storeRDFFile...");
		
		try {
			File file = MappingPediaUtility.multipartFileToFile(fileRef);
			String filePath = file.getPath();
			logger.info("file path = " + filePath);

			MappingPediaUtility.store(filePath, graphURI);
			Integer errorCode=HttpURLConnection.HTTP_CREATED;
			String status="success, file uploaded to: " + filePath;
			logger.info("mapping inserted.");
			
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null
					, null, filePath, null,null, status, errorCode);
			return executionResult;			
			
		} catch(Exception e) {
			String errorMessage = "error processing uploaded file: " + e.getMessage();
			logger.error(errorMessage);
			Integer errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
			String status="failed, error message = " + e.getMessage();
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null
					, null, null, null,null, status, errorCode);
			return executionResult;
		}
	}

}