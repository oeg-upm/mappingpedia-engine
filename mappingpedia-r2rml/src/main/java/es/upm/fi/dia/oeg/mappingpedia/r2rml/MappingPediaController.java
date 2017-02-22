package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import scala.Option;

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

	@RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}/{mappingFileExtension}", method= RequestMethod.PUT)
	public MappingPediaExecutionResult updateMapping(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
		, @PathVariable("mappingDirectory") String mappingDirectory
		, @PathVariable("mappingFilename") String mappingFilename
		, @PathVariable("mappingFileExtension") String mappingFileExtension
		, @RequestParam(value="mappingFile") MultipartFile mappingFileRef
	)
	{
		logger.info("PUT /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}/{mappingFileExtension}");
		logger.info("mappingpediaUsername = " + mappingpediaUsername);
		logger.info("mappingDirectory = " + mappingDirectory);
		logger.info("mappingFilename = " + mappingFilename);
		logger.info("mappingFileExtension = " + mappingFileExtension);
		logger.info("mappingFileRef = " + mappingFileRef);
		String status="";

		FileInputStream mappingReader = null;
		String mappingFilePath = null;
		String githubMappingURL = null;
		try {
			if(mappingFileRef != null) {
				mappingReader = (FileInputStream) mappingFileRef.getInputStream();
			}
			File mappingFile = MappingPediaUtility.materializeFileInputStream(mappingReader, mappingDirectory, mappingFilename);
			mappingFilePath = mappingFile.getPath();
			logger.info("mapping file path = " + mappingFilePath);
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "error processing the uploaded mapping file.";
			logger.error(errorMessage);
			status += errorMessage + "\n";
		}

		String sha = GitHubUtility.getSHA(mappingpediaUsername, mappingDirectory, mappingFilename, mappingFileExtension
				, Application.prop.githubUser(), Application.prop.githubAccessToken());
		logger.info("sha = " + sha);

		String commitMessage = "Mapping modification by mappingpedia-engine.Application";
		String mappingContent = MappingPediaRunner.getMappingContent(null, null, mappingFilePath, null);
		String base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent);
		HttpResponse<JsonNode> response = GitHubUtility.putEncodedFile(mappingDirectory, mappingFilename + "." + mappingFileExtension
				, commitMessage, base64EncodedContent
				, Application.prop.githubUser(), Application.prop.githubAccessToken(), mappingpediaUsername, Option.apply(sha));
		int responseStatus = response.getStatus();
		logger.info("response.getStatusText() = " + response.getStatusText());
		if(HttpURLConnection.HTTP_OK == responseStatus) {
			githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
			logger.info("githubMappingURL = " + githubMappingURL);
		}
		MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(null, githubMappingURL
				, response.getStatusText(), response.getStatus());
		return executionResult;
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

	@RequestMapping(value = {"/upload/{mappingpediaUsername}", "/mappings/{mappingpediaUsername}"}, method= RequestMethod.PUT)
	public MappingPediaExecutionResult uploadFile(
			@RequestParam("manifestFile") MultipartFile manifestFileRef
			, @RequestParam("mappingFile") MultipartFile mappingFileRef
			, @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI
			//, @RequestParam(value="mappingpediaUsername", defaultValue="mappingpediaTestUser") String mappingpediaUsername
			, @PathVariable("mappingpediaUsername") String mappingpediaUsername
	)
	{
		logger.info("[PUT] /mappings/{mappingpediaUsername}");
		logger.info("manifestFileRef = " + manifestFileRef);
		logger.info("mappingFileRef = " + mappingFileRef);
		logger.info("mappingpediaUsername = " + mappingpediaUsername);
		String status="";
		Integer errorCode=-1;

		// Create the input stream to uploaded files to read data from it.
		FileInputStream manifestReader = null;
		try {
			if(manifestFileRef != null) {
				manifestReader = (FileInputStream) manifestFileRef.getInputStream();	
			}
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "error processing the uploaded manifest file.";
			logger.error(errorMessage);
			status += errorMessage + "\n";
		}
		
		FileInputStream mappingReader = null;
		try {
			if(mappingFileRef != null) {
				mappingReader = (FileInputStream) mappingFileRef.getInputStream();	
			}
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "error processing the uploaded mapping file.";
			logger.error(errorMessage);
			status += errorMessage + "\n";
		}

		if(manifestReader == null || mappingReader == null) {
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult("", "", status, errorCode);
			return executionResult;			
		} else {
			// Get names of uploaded files.
			String manifestFileName = manifestFileRef.getOriginalFilename();
			String mappingFileName = mappingFileRef.getOriginalFilename();

			// Path where the uploaded files will be stored.
			String uuid = UUID.randomUUID().toString();

			String manifestFilePath = null;
			String githubMappingURL = null;
			String mappingFilePath = null;
			try {
				File manifestFile = MappingPediaUtility.materializeFileInputStream(manifestReader, uuid, manifestFileName);
				manifestFilePath = manifestFile.getPath();
				logger.info("manifest file path = " + manifestFilePath);

				File mappingFile = MappingPediaUtility.materializeFileInputStream(mappingReader, uuid, mappingFileName);
				mappingFilePath = mappingFile.getPath();
				logger.info("mapping file path = " + mappingFilePath);

				String newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS() + uuid + "/";
				MappingPediaRunner.run(manifestFilePath, null, mappingFilePath, null, "false"
						, Application.mappingpediaR2RML, replaceMappingBaseURI, newMappingBaseURI);

				String filename = mappingFileName;
				if(filename == null) {
					filename = uuid + ".ttl";
				}
				String commitMessage = "Commit From mappingpedia-engine.Application";
				String mappingContent = MappingPediaRunner.getMappingContent(manifestFilePath, null, mappingFilePath, null);
				String base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent);
				HttpResponse<JsonNode> response = GitHubUtility.putEncodedFile(uuid, filename, commitMessage, base64EncodedContent
						, Application.prop.githubUser(), Application.prop.githubAccessToken(), mappingpediaUsername, null);
				int responseStatus = response.getStatus();
				if(HttpURLConnection.HTTP_CREATED == responseStatus) {
					githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
					logger.info("githubMappingURL = " + githubMappingURL);
				}
				errorCode=0;
				status="success!";
				logger.info("mapping inserted.");
			} catch (Exception e){
				e.printStackTrace();
				errorCode=-1;
				status="failed, error message = " + e.getMessage();
			}

			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(manifestFilePath, githubMappingURL
					, status, errorCode);
			return executionResult;			

		}
	}

	@RequestMapping(value = "/storeRDFFile")
	public MappingPediaExecutionResult storeRDFFile(@RequestParam("rdfFile") MultipartFile fileRef
			, @RequestParam(value="graphURI") String graphURI)
	{
		logger.info("/storeRDFFile...");
		String status="";
		Integer errorCode=-1;

		// Create the input stream to uploaded files to read data from it.
		FileInputStream reader = null;
		try {
			if(fileRef != null) {
				reader = (FileInputStream) fileRef.getInputStream();
			}
		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = "error processing the uploaded file.";
			logger.error(errorMessage);
			status += errorMessage + "\n";
		}

		if(reader == null) {
			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult("", "", status, errorCode);
			return executionResult;
		} else {
			// Get names of uploaded files.
			String fileName = fileRef.getOriginalFilename();

			// Path where the uploaded files will be stored.
			String uuid = UUID.randomUUID().toString();

			String filePath = null;
			try {
				File file = MappingPediaUtility.materializeFileInputStream(reader, uuid, fileName);
				filePath = file.getPath();
				logger.info("file path = " + filePath);

				MappingPediaUtility.store(filePath, graphURI);
				errorCode=0;
				status="success!";
				logger.info("mapping inserted.");
			} catch (Exception e){
				e.printStackTrace();
				errorCode=-1;
				status="failed, error message = " + e.getMessage();
			}

			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(filePath, "", status, errorCode);
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
