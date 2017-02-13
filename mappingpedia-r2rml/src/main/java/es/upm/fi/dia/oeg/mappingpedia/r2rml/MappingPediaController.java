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

		// Get names of uploaded files.
		String manifestFileName = manifestFileRef.getOriginalFilename();
		String mappingFileName = mappingFileRef.getOriginalFilename();

		// Path where the uploaded files will be stored.
		String uuid = UUID.randomUUID().toString();
		String uploadDirectoryPath1 = "upload-dir";
		File outputDirectory1 = new File(uploadDirectoryPath1);
		if(!outputDirectory1.exists()) {
			outputDirectory1.mkdirs();
		}
		String uploadDirectoryPath2 = uploadDirectoryPath1 + "/" + uuid;
		logger.info("upload directory path = " + uploadDirectoryPath2);
		File outputDirectory2 = new File(uploadDirectoryPath2);

		// Now create the output files on the server.
		String manifestFilePath = uploadDirectoryPath2 + "/" + manifestFileName;
		File manifestFile = new File(manifestFilePath);
		logger.info("manifest file path = " + manifestFilePath);
		String mappingFilePath = uploadDirectoryPath2 + "/" + mappingFileName;
		File mappingFile = new File(mappingFilePath);
		logger.info("mapping file path = " + mappingFilePath);


		FileInputStream manifestReader = null;
		FileInputStream mappingReader = null;
		try {
			outputDirectory2.mkdir();
			manifestFile.createNewFile();
			mappingFile.createNewFile();


			// Create the input stream to uploaded files to read data from it.
			manifestReader = (FileInputStream) manifestFileRef.getInputStream();
			mappingReader = (FileInputStream) mappingFileRef.getInputStream();

			MappingPediaController.copyFileUsingChannel(manifestReader, manifestFile);
			MappingPediaController.copyFileUsingChannel(mappingReader, mappingFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		String status=null;
		Integer errorCode=-1;
		String newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS() + uuid + "/";
		try {
			//Application.mappingpediaR2RML.insertMappingFromManifestFilePath(manifestFile.getPath());
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

	private static void copyFileUsingChannel(FileInputStream source, File dest) throws IOException {
		FileChannel sourceChannel = null;
		FileChannel destChannel = null;
		FileOutputStream fos = new FileOutputStream(dest);
		try {
			sourceChannel = source.getChannel();
			destChannel = fos.getChannel();
			destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
		}finally{
			sourceChannel.close();
			destChannel.close();
			fos.close();
		}
	}
}
