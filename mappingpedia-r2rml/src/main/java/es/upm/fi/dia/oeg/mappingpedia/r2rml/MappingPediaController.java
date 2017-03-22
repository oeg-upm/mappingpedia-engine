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

	@RequestMapping(value="/executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.POST)
	public MappingPediaExecutionResult executeMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
		, @PathVariable("mappingDirectory") String mappingDirectory
		, @PathVariable("mappingFilename") String mappingFilename
		, @RequestParam(value="datasetFile") String datasetFile
		, @RequestParam(value="outputFilename", required = false) String outputFilename
	)
	{
		logger.info("POST /executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
		return MappingPediaR2RML.executeMapping(mappingpediaUsername, mappingDirectory, mappingFilename, datasetFile, outputFilename);
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
		logger.info("mappingpediaUsername = " + mappingpediaUsername);

		// Path where the uploaded files will be stored.
		String uuid = UUID.randomUUID().toString();
		logger.info("uuid = " + uuid);
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
		logger.info("mappingpediaUsername = " + mappingpediaUsername);
		logger.info("datasetID = " + datasetID);

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
			logger.info("response.getHeaders = " + response.getHeaders());
			logger.info("response.getBody = " + response.getBody());

			int responseStatus = response.getStatus();
			logger.info("responseStatus = " + responseStatus);
			String responseStatusText = response.getStatusText();
			logger.info("responseStatusText = " + responseStatusText);
			if(HttpURLConnection.HTTP_CREATED == responseStatus) {
				String githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.info("githubMappingURL = " + githubMappingURL);
				logger.info("mapping inserted.");
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(manifestFilePath, null, githubMappingURL
						, responseStatusText, responseStatus);
				return executionResult;
			} else {
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(manifestFilePath, null, null
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
	}

	@RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.GET)
	public MappingPediaExecutionResult getMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
		, @PathVariable("mappingDirectory") String mappingDirectory
		, @PathVariable("mappingFilename") String mappingFilename
	)
	{
		logger.info("GET /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
		logger.info("mappingpediaUsername = " + mappingpediaUsername);
		logger.info("mappingDirectory = " + mappingDirectory);
		logger.info("mappingFilename = " + mappingFilename);
		HttpResponse<JsonNode> response = GitHubUtility.getFile(
				MappingPediaProperties.githubUser(), MappingPediaProperties.githubAccessToken()
			, mappingpediaUsername, mappingDirectory, mappingFilename
		);
		int responseStatus = response.getStatus();
		logger.info("responseStatus = " + responseStatus);
		String responseStatusText = response.getStatusText();
		logger.info("responseStatusText = " + responseStatusText);
		if(HttpURLConnection.HTTP_OK == responseStatus) {
			String githubMappingURL = response.getBody().getObject().getString("url");
			logger.info("githubMappingURL = " + githubMappingURL);
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, githubMappingURL
					, responseStatusText, responseStatus);
			return executionResult;			
		} else {
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
					, responseStatusText, responseStatus);
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
		logger.info("mappingpediaUsername = " + mappingpediaUsername);
		logger.info("mappingDirectory = " + mappingDirectory);
		logger.info("mappingFilename = " + mappingFilename);
		//logger.info("mappingFileExtension = " + mappingFileExtension);
		logger.info("mappingFileRef = " + mappingFileRef);

		try {
			
			//FileInputStream mappingReader = null;
			
			/*
			if(mappingFileRef != null) {
				mappingReader = (FileInputStream) mappingFileRef.getInputStream();
			}
			*/
			//File mappingFile = MappingPediaUtility.materializeFileInputStream(mappingReader, mappingDirectory, mappingFilename);
			File mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, mappingDirectory);
			String mappingFilePath = mappingFile.getPath();
			logger.info("mapping file path = " + mappingFilePath);

			String commitMessage = "Mapping modification by mappingpedia-engine.Application";
			String mappingContent = MappingPediaRunner.getMappingContent(null, null, mappingFilePath, null);
			String base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent);
			HttpResponse<JsonNode> response = GitHubUtility.putEncodedContent(
					MappingPediaProperties.githubUser(), MappingPediaProperties.githubAccessToken()
					, mappingpediaUsername, mappingDirectory, mappingFilename
					, commitMessage, base64EncodedContent
			);
			int responseStatus = response.getStatus();
			logger.info("responseStatus = " + responseStatus);
			String responseStatusText = response.getStatusText();
			logger.info("responseStatusText = " + responseStatusText);
			if(HttpURLConnection.HTTP_OK == responseStatus) {
				String githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.info("githubMappingURL = " + githubMappingURL);
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, githubMappingURL
						, responseStatusText, responseStatus);
				return executionResult;
			} else {
				MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
						, responseStatusText, responseStatus);
				return executionResult;
			}
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "error processing the uploaded mapping file: " + e.getMessage();
			logger.error(errorMessage);
			Integer errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null
					, errorMessage, errorCode);
			return executionResult;
		}
	}

/*
	@RequestMapping(value = "/mappings", method= RequestMethod.PUT)
	public MappingPediaExecutionResult updateMapping(@PathVariable("mappingURL") String mappingURL) {
		logger.info("in PUT of /mappings");
		logger.info("mappingURL = " + mappingURL);
		String sha = GitHubUtility.getSHA(mappingURL, Application.prop.githubUser(), Application.prop.githubAccessToken());
		MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(sha, sha, "success!", 0);
		return executionResult;
	}
*/

	@RequestMapping(value = "/datasets/{mappingpediaUsername}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewDataset(
			@RequestParam("datasetFile") MultipartFile datasetFileRef
			, @PathVariable("mappingpediaUsername") String mappingpediaUsername
	)
	{
		logger.info("[POST] /datasets/{mappingpediaUsername}");
		logger.info("mappingpediaUsername = " + mappingpediaUsername);

		// Path where the uploaded files will be stored.
		String datasetID = UUID.randomUUID().toString();
		return this.addNewDataset(datasetFileRef
				, mappingpediaUsername
				, datasetID);
	}
	
	@RequestMapping(value = "/datasets/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
	public MappingPediaExecutionResult addNewDataset(
			@RequestParam("datasetFile") MultipartFile datasetFileRef
			, @PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @PathVariable("datasetID") String datasetID
	)
	{
		logger.info("[POST] /datasets/{mappingpediaUsername}/{datasetID}");
		logger.info("mappingpediaUsername = " + mappingpediaUsername);
		logger.info("datasetID = " + datasetID);
		
		try {
			File datasetFile = MappingPediaUtility.multipartFileToFile(datasetFileRef, datasetID);

			logger.info("storing a dataset file in github ...");
			String commitMessage = "Add a new dataset by mappingpedia-engine";
			HttpResponse<JsonNode> response = GitHubUtility.putEncodedFile(
					MappingPediaProperties.githubUser(), MappingPediaProperties.githubAccessToken()
					, mappingpediaUsername, datasetID, datasetFile.getName()
					, commitMessage, datasetFile
			);
			logger.info("response.getHeaders = " + response.getHeaders());
			logger.info("response.getBody = " + response.getBody());

			int responseStatus = response.getStatus();
			logger.info("responseStatus = " + responseStatus);
			String responseStatusText = response.getStatusText();
			logger.info("responseStatusText = " + responseStatusText);
			if(HttpURLConnection.HTTP_CREATED == responseStatus) {
				String datasetURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.info("githubMappingURL = " + datasetURL);
				logger.info("dataset inserted.");
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
			Integer errorCode=0;
			String status="success, file uploaded to: " + filePath;
			logger.info("mapping inserted.");
			
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null, status, errorCode);
			return executionResult;			
			
		} catch(Exception e) {
			String errorMessage = "error processing uploaded file: " + e.getMessage();
			logger.error(errorMessage);
			Integer errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
			String status="failed, error message = " + e.getMessage();
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, null, null, status, errorCode);
			return executionResult;
		}
	}

/*
	private static void materializeFileInputStream(FileInputStream source, File dest) throws IOException {
		FileChannel sourceChannel = null;
		FileChannel destChannel = null;
		FileOutputStream fos = new FileOutputStream(dest);
		try {
			sourceChannel = source.getChannel();
			destChannel = fos.getChannel();
			destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
		} finally{
			sourceChannel.close();
			destChannel.close();
			fos.close();
		}
	}

	private static File materializeFileInputStream(FileInputStream source, String uuidDirectoryName, String fileName)
			throws IOException {
		//create upload directory is not exist
		String uploadDirectoryPath = "upload-dir";
		File outputDirectory = new File(uploadDirectoryPath);
		if(!outputDirectory.exists()) {
			outputDirectory.mkdirs();
		}

		//create uuid directory
		String uuidDirectoryPath = uploadDirectoryPath + "/" + uuidDirectoryName;
		logger.info("upload directory path = " + uuidDirectoryPath);
		File uuidDirectory = new File(uuidDirectoryPath);
		if(!uuidDirectory.exists()) {
			uuidDirectory.mkdirs();
		}


		// Now create the output files on the server.
		String uploadedFilePath = uuidDirectory + "/" + fileName;
		File dest = new File(uploadedFilePath);
		dest.createNewFile();

		MappingPediaController.materializeFileInputStream(source, dest);

		return dest;
	}
*/

}