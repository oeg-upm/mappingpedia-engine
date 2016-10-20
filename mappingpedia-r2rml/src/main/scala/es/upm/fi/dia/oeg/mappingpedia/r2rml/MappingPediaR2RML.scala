package es.upm.fi.dia.oeg.mappingpedia.r2rml

import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.vocabulary.DC_11
import java.io.File
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.graph.Triple
import scala.collection.mutable.ListBuffer
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.R2RMLConstants
import com.hp.hpl.jena.rdf.model.RDFList
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.RDFNode
import org.apache.log4j.Logger

class MappingPediaR2RML {
	val logger : Logger = Logger.getLogger("MappingPediaR2RML");
	
	private val MANIFEST_FILE_LANGUAGE = "TURTLE";
	private val R2RML_FILE_LANGUAGE = "TURTLE";
	private var r2rmlTriples : List[Triple] = Nil;
	private var manifestTriples : List[Triple] = Nil;
	private var graphName : String = null;
	
	def readManifestFile(manifestFilePath : String) = {
		logger.info("Reading manifest file : " + manifestFilePath);
		val manifestFile = new File(manifestFilePath);

		val manifestModel = ModelFactory.createDefaultModel();
		val inManifestModel = FileManager.get().open( manifestFilePath );
		manifestModel.read(inManifestModel, null, MANIFEST_FILE_LANGUAGE);	  
		
		val r2rmlResources = manifestModel.listResourcesWithProperty(
				RDF.`type`, MappingPediaConstant.R2RML_CLASS);
		if(r2rmlResources != null) {
			val r2rmlResource = r2rmlResources.nextResource();
			graphName = r2rmlResource.toString();
			
			val mappingDocumentTitle = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
					r2rmlResource, DC_11.title);

			val mappingDocumentId = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
					r2rmlResource, DC_11.identifier);

			val testPurpose = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
					r2rmlResource, MappingPediaConstant.TEST_PURPOSE_PROPERTY);

			val rdb2rdftestMappingDocumentFilePath = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
					r2rmlResource, MappingPediaConstant.MAPPING_DOCUMENT_PROPERTY).toString();

			var mappingDocumentFile = new File(rdb2rdftestMappingDocumentFilePath.toString());
			val isMappingDocumentFilePathAbsolute = mappingDocumentFile.isAbsolute();
			var r2rmlMappingDocumentPath : String = null; 
			if(isMappingDocumentFilePathAbsolute) {
				r2rmlMappingDocumentPath = rdb2rdftestMappingDocumentFilePath
			} else {
			  if(manifestFile.isAbsolute()) {
				r2rmlMappingDocumentPath = manifestFile.getParentFile().toString() + File.separator + mappingDocumentFile; 
			  } else {
			    r2rmlMappingDocumentPath = rdb2rdftestMappingDocumentFilePath
			  }
			}
			
			logger.info("Reading R2RML Mapping document : " + r2rmlMappingDocumentPath);
			val r2rmlDocumentModel = ModelFactory.createDefaultModel();
			val inR2rmlDocumentModel = FileManager.get().open( r2rmlMappingDocumentPath );
			r2rmlDocumentModel.read(inR2rmlDocumentModel, null, R2RML_FILE_LANGUAGE);
			val baseNS : String = r2rmlDocumentModel.getNsPrefixURI("");
			logger.info("baseNS = " + baseNS);
			
			val r2rmlMappingDocumentStatements = r2rmlDocumentModel.listStatements();
			if(r2rmlMappingDocumentStatements != null) {
			  while(r2rmlMappingDocumentStatements.hasNext()) {
			    val r2rmlMappingDocumentStatement : Statement = r2rmlMappingDocumentStatements.nextStatement();
			    val r2rmlMappingDocumentTriple = r2rmlMappingDocumentStatement.asTriple();
			    r2rmlTriples = r2rmlTriples ::: List(r2rmlMappingDocumentTriple);
			  }
			}

			val triplesMapResources = r2rmlDocumentModel.listResourcesWithProperty(
				RDF.`type`, R2RMLConstants.R2RML_TRIPLESMAP_CLASS);
			if(triplesMapResources != null) {
			  val triplesMaplist : RDFNode = manifestModel.createList(triplesMapResources);
			  r2rmlResource.addProperty(MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMaplist);
			}

		}

		//put this after we process triples Map List!
		val r2rmlMappingDocumentStatements = manifestModel.listStatements();
		if(r2rmlMappingDocumentStatements != null) {
		  while(r2rmlMappingDocumentStatements.hasNext()) {
		    val r2rmlMappingDocumentTriple = r2rmlMappingDocumentStatements.next().asTriple();
		    manifestTriples = manifestTriples ::: List(r2rmlMappingDocumentTriple);
		  }
		}
		
		logger.debug("manifestTriples = " + manifestTriples);
		logger.debug("r2rmlTriples = " + r2rmlTriples);
	}

	def getR2rmlTriples : List[Triple] = {
		return r2rmlTriples;
	}
	
	def getManifestTriples : List[Triple] = {
		return manifestTriples;
	}	
	
	def getGraphName : String = {
		return graphName;
	}	
}
