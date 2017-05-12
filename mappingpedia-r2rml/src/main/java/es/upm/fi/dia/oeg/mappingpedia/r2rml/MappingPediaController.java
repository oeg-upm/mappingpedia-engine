package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.File;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunnerFactory;
import scala.collection.JavaConverters.*;
import scala.collection.JavaConversions.*;

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
		ListResult listResult = MappingPediaR2RML.getAllTriplesMaps();
		logger.info("listResult = " + listResult);

		return listResult;
	}

	@RequestMapping(value="/ogd/annotations", method= RequestMethod.GET)
	public ListResult getMappingDocuments(@RequestParam(value="mappedClass", required = false) String mappedClass) {
		logger.info("/ogd/annotations(GET) ...");
		if(mappedClass == null) {
			ListResult listResult = MappingPediaR2RML.findAllMappingDocuments();
			logger.info("listResult = " + listResult);
			return listResult;
		} else {
			ListResult listResult = MappingPediaR2RML.findMappingDocumentsByMappedClass(mappedClass);
			logger.info("listResult = " + listResult);
			return listResult;

		}
	}


	@RequestMapping(value="/githubRepoContentsURL", method= RequestMethod.GET)
	public String getGitHubRepoContentsURL() {
		logger.info("/githubRepoContentsURL(GET) ...");
		return MappingPediaProperties.githubRepoContents();
	}

	@RequestMapping(value="/executions/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.POST)
	public MappingPediaExecutionResult executeMapping(@PathVariable("mappingpediaUsername") String mappingpediaUsername
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
		, @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
		, @RequestParam(value="mappingDocumentTitle", defaultValue="Mapping Document Title") String mappingDocumentTitle
		, @RequestParam(value="mappingDocumentCreator", defaultValue="Mapping Document Creator") String mappingDocumentCreator
		, @RequestParam(value="mappingDocumentSubjects", defaultValue="Mapping Document Subjects") String mappingDocumentSubjects

	)
	{
		logger.info("[POST] /mappings/{mappingpediaUsername}");
		return MappingPediaR2RML.uploadNewMapping(mappingpediaUsername, manifestFileRef, mappingFileRef
			, replaceMappingBaseURI, generateManifestFile
			, mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
			//, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
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
	)
	{
		logger.info("[POST] /mappings/{mappingpediaUsername}/{datasetID}");
		return MappingPediaR2RML.uploadNewMapping(mappingpediaUsername, datasetID, manifestFileRef, mappingFileRef
			, replaceMappingBaseURI, generateManifestFile
			, mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
			//, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
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
		return MappingPediaR2RML.getMapping(mappingpediaUsername, mappingDirectory, mappingFilename);
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
		return MappingPediaR2RML.updateExistingMapping(mappingpediaUsername, mappingDirectory, mappingFilename
				, mappingFileRef);
	}

	@RequestMapping(value = "/datasets/{mappingpediaUsername}", method= RequestMethod.POST)
	public MappingPediaExecutionResult uploadNewDataset(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
			, @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
			, @RequestParam("datasetFile") MultipartFile datasetFileRef
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

		return MappingPediaR2RML.addDatasetFile(datasetFileRef, manifestFileRef, generateManifestFile, mappingpediaUsername
				, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
				, distributionAccessURL, distributionDownloadURL, distributionMediaType
		);
	}

	@RequestMapping(value = "/datasets/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
	public MappingPediaExecutionResult addNewDataset(
			@PathVariable("mappingpediaUsername") String mappingpediaUsername
			, @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
			, @RequestParam(value="generateManifestFile", defaultValue="false") String generateManifestFile
			, @RequestParam("datasetFile") MultipartFile datasetFileRef
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
		return MappingPediaR2RML.addDatasetFileWithID(datasetFileRef, manifestFileRef, generateManifestFile, mappingpediaUsername
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
		return MappingPediaR2RML.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
	}


	@RequestMapping(value = "/storeRDFFile")
	public MappingPediaExecutionResult storeRDFFile(
			@RequestParam("rdfFile") MultipartFile fileRef
			, @RequestParam(value="graphURI") String graphURI)
	{
		logger.info("/storeRDFFile...");
		return MappingPediaR2RML.storeRDFFile(fileRef, graphURI);
	}

}