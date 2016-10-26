package es.upm.fi.dia.oeg.mappingpedia.r2rml

import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.vocabulary.DC_11;

object MappingPediaConstant {
	private val RDB2RDFTEST_NS = "http://purl.org/NET/rdb2rdf-test#";
	private val TEST_NS = "http://www.w3.org/2006/03/test-description#";
	private val MAPPINGPEDIA_NS = "http://mappingpedia.linkeddata.es/vocabulary#";

	private val R2RML_URI = RDB2RDFTEST_NS + "R2RML";
	val R2RML_CLASS = ResourceFactory.createResource(R2RML_URI);
	private val MAPPING_DOCUMENT_URI = RDB2RDFTEST_NS + "mappingDocument";
	val MAPPING_DOCUMENT_PROPERTY = ResourceFactory.createProperty(MAPPING_DOCUMENT_URI);
	private val TEST_PURPOSE_URI = TEST_NS + "purpose";
	val TEST_PURPOSE_PROPERTY = ResourceFactory.createProperty(TEST_PURPOSE_URI);

	private val HAS_TRIPLES_MAPS_URI = MAPPINGPEDIA_NS + "hasTriplesMaps";
	val HAS_TRIPLES_MAPS_PROPERTY = ResourceFactory.createProperty(HAS_TRIPLES_MAPS_URI);

	val R2RML_NS = "http://www.w3.org/ns/r2rml#";
	val R2RML_TRIPLESMAP_URI = R2RML_NS + "TriplesMap";
	val R2RML_TRIPLESMAP_CLASS = ResourceFactory.createResource(R2RML_TRIPLESMAP_URI);


}