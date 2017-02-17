package es.upm.fi.dia.oeg.mappingpedia.r2rml

import java.util.Properties

class MappingPediaProperties extends Properties {
  		var virtuosoJDBC:String = null;
  		var virtuosoUser:String = null;
  		var virtuosoPwd:String = null;
  		var graphName:String = null;
  		var clearGraph:Boolean = false;

	def setVirtuosoJDBC(pVirtuosoJDBC:String) = {this.virtuosoJDBC = pVirtuosoJDBC; }

}

