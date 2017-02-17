package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
//@RequestMapping(value = "/mappingpedia")
@MultipartConfig(fileSizeThreshold = 20971520)
public class MappingPediaController {
	static Logger logger = LogManager.getLogger("MappingPediaController");

	private static final String template = "Hello, %s!";
	private final AtomicLong counter = new AtomicLong();

	@RequestMapping("/greeting")
	public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
		logger.info("/greeting ...");
		return new Greeting(counter.incrementAndGet(),
				String.format(template, name));
	}

	@RequestMapping(value = "/upload")
	public MappingPediaExecutionResult uploadFile(
			@RequestParam("manifestFile") MultipartFile manifestFileRef
			, @RequestParam("mappingFile") MultipartFile mappingFileRef
			, @RequestParam(value="replaceMappingBaseURI", defaultValue="true") String replaceMappingBaseURI)
	{
		logger.info("/upload ...");
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

				errorCode=0;
				status="success!";
				logger.info("mapping inserted.");
			} catch (Exception e){
				e.printStackTrace();
				errorCode=-1;
				status="failed, error message = " + e.getMessage();
			}

			MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(manifestFilePath, mappingFilePath
					, status, errorCode);
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
