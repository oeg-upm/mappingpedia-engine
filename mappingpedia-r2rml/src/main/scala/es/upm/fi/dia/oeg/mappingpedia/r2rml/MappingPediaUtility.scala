package es.upm.fi.dia.oeg.mappingpedia.r2rml

import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.Literal
import scala.None

object MappingPediaUtility {
    def getFirstPropertyObjectValueLiteral(resource:Resource, property:Property): Literal = {
    	val it = resource.listProperties(property);
    	var result: Literal = null;
    	if(it != null && it.hasNext()) {
    		val statement = it.next();
    		val objectNode = statement.getObject();
    		result = objectNode.asLiteral()
    	}
    	return result;
    }
}