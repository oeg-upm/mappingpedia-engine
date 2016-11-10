package es.upm.fi.dia.oeg.mappingpedia.r2rml

import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.vocabulary.DC_11;

object MappingPediaConstant {
  //RDB2RDF Test related constants
	private val RDB2RDFTEST_NS = "http://purl.org/NET/rdb2rdf-test#";
	private val RDB2RDFTEST_R2RML_URI = RDB2RDFTEST_NS + "R2RML";
	val RDB2RDFTEST_R2RML_CLASS = ResourceFactory.createResource(RDB2RDFTEST_R2RML_URI);
	private val RDB2RDFTEST_MAPPING_DOCUMENT_URI = RDB2RDFTEST_NS + "mappingDocument";
	val RDB2RDFTEST_MAPPING_DOCUMENT_PROPERTY = ResourceFactory.createProperty(RDB2RDFTEST_MAPPING_DOCUMENT_URI);
	
	//W3C Test Description related constants
	private val TEST_NS = "http://www.w3.org/2006/03/test-description#";
	private val TEST_PURPOSE_URI = TEST_NS + "purpose";
	val TEST_PURPOSE_PROPERTY = ResourceFactory.createProperty(TEST_PURPOSE_URI);

	//R2RML related constants
	val R2RML_NS = "http://www.w3.org/ns/r2rml#";
	val R2RML_TRIPLESMAP_URI = R2RML_NS + "TriplesMap";
	val R2RML_TRIPLESMAP_CLASS = ResourceFactory.createResource(R2RML_TRIPLESMAP_URI);

	//Mappingpedia related constants
	private val MAPPINGPEDIA_NS = "http://mappingpedia.linkeddata.es/vocabulary#";
	private val MAPPINGPEDIA_INSTANCE_NS = "http://mappingpedia.linkeddata.es/instance#";
	val DEFAULT_MAPPINGPEDIA_GRAPH = "http://mappingpedia.linkeddata.es/graph/data";
	private val HAS_TRIPLES_MAPS_URI = MAPPINGPEDIA_NS + "hasTriplesMaps";
	val HAS_TRIPLES_MAPS_PROPERTY = ResourceFactory.createProperty(HAS_TRIPLES_MAPS_URI);
	private val DEFAULT_MAPPINGDOCUMENT_PROPERTY = "http://purl.org/NET/rdb2rdf-test##mappingDocument";
  private val MAPPINGPEDIAVOCAB_R2RML_URI = MAPPINGPEDIA_NS + "R2RML";
	val MAPPINGPEDIAVOCAB_R2RML_CLASS = ResourceFactory.createResource(MAPPINGPEDIAVOCAB_R2RML_URI);

}