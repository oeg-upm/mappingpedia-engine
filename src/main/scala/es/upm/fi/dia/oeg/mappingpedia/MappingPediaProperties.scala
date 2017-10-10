package es.upm.fi.dia.oeg.mappingpedia

import java.io.{InputStream, Reader}
import java.util.Properties

import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by freddy on 10/08/2017.
  */
class MappingPediaProperties(is:InputStream) extends Properties {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	super.load(is);

	//VIRTUOSO
	val virtuosoJDBC:String = this.getProperty("vjdbc")
	val virtuosoUser:String = this.getProperty("usr")
	val virtuosoPwd:String = this.getProperty("pwd")
	val graphName:String = this.getProperty("graphname")
	val clearGraphPropertyValue = this.getProperty("cleargraph")
	val clearGraph = if (clearGraphPropertyValue == null) { false }
	else {
		if ("true".equalsIgnoreCase(clearGraphPropertyValue) || "yes".equalsIgnoreCase(clearGraphPropertyValue)) { true }
		else { false }
	}


	//GITHUB
	val githubUser:String = this.getProperty("github.mappingpedia.username")
	val githubAccessToken:String = this.getProperty("github.mappingpedia.accesstoken")
	val githubRepoPropertyValue = this.getProperty("github.mappingpedia.repository")
	val githubRepo = if (githubRepoPropertyValue == null) {
		MappingPediaConstant.DEFAULT_GITHUB_REPOSITORY
	} else {
		githubRepoPropertyValue
	}
	val githubRepoContentsPropertyValue = this.getProperty("github.mappingpedia.repository.contents")
	val githubRepoContents = if (githubRepoContentsPropertyValue == null) {
		MappingPediaConstant.DEFAULT_GITHUB_REPOSITORY_CONTENTS
	} else {
		githubRepoContentsPropertyValue
	}


	//CKAN
	var ckanKey:String = null;
	var ckanActionOrganizationCreate:String=null;
	var ckanActionPackageCreate:String=null;
	var ckanActionResourceCreate:String=null;

}
