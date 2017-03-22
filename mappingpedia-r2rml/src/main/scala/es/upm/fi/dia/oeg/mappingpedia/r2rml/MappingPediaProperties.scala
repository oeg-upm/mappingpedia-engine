package es.upm.fi.dia.oeg.mappingpedia.r2rml

import java.util.Properties

object MappingPediaProperties extends Properties {
	var virtuosoJDBC:String = null;
	var virtuosoUser:String = null;
	var virtuosoPwd:String = null;
	var graphName:String = null;
	var clearGraph:Boolean = false;
	var githubUser:String = null;
	var githubAccessToken:String = null;
	var githubRepo:String = null;
	var githubRepoContents:String = null;

	//def setVirtuosoJDBC(pVirtuosoJDBC:String) = {this.virtuosoJDBC = pVirtuosoJDBC; }

}

