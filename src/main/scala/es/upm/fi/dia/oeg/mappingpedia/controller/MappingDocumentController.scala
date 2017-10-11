package es.upm.fi.dia.oeg.mappingpedia.controller

import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.logger
import es.upm.fi.dia.oeg.mappingpedia.model.{ListResult, MappingDocument}
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility
import virtuoso.jena.driver.{VirtModel, VirtuosoQueryExecutionFactory}

object MappingDocumentController {
  def findMappingDocuments(queryString:String) : ListResult = {
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);

    logger.info("Executing query=\n" + queryString)

    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    var results:List[MappingDocument] = List.empty;
    try {
      val rs = qexec.execSelect
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val id = MappingPediaUtility.getStringOrElse(qs, "md", null);
        val title = MappingPediaUtility.getStringOrElse(qs, "title", null);
        val dataset = MappingPediaUtility.getStringOrElse(qs, "dataset", null);
        val filePath = MappingPediaUtility.getStringOrElse(qs, "filePath", null);
        val creator = MappingPediaUtility.getStringOrElse(qs, "creator", null);
        val distribution = MappingPediaUtility.getStringOrElse(qs, "distribution", null);
        val distributionAccessURL = MappingPediaUtility.getStringOrElse(qs, "accessURL", null);
        val mappingDocumentURL = MappingPediaUtility.getStringOrElse(qs, "mappingDocumentURL", null);
        val mappingLanguage = MappingPediaUtility.getOptionString(qs, "mappingLanguage");


        val md = new MappingDocument(id, title, dataset, filePath, creator, distribution
          , distributionAccessURL, mappingDocumentURL, mappingLanguage);
        results = md :: results;
      }
    } finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

  def findAllMappingDocuments() : ListResult = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues:Map[String,String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappingDocuments.rq")

    MappingDocumentController.findMappingDocuments(queryString);

  }

  def findMappingDocumentsByMappedClass(mappedClass:String) : ListResult = {
    logger.info("findMappingDocumentsByMappedClass:" + mappedClass)
    val queryTemplateFile = "templates/findTriplesMapsByMappedClass.rq";

    val mapValues:Map[String,String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedClass" -> mappedClass
      //, "$mappedProperty" -> mappedProperty
    );

    val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedProperty(mappedProperty:String) : ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedProperty.rq";

    val mapValues:Map[String,String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedProperty" -> mappedProperty
    );

    val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedColumn(mappedColumn:String) : ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedColumn.rq";

    val mapValues:Map[String,String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedColumn" -> mappedColumn
    );

    val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedTable(mappedTable:String) : ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedTable.rq";

    val mapValues:Map[String,String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedTable" -> mappedTable
    );

    val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocuments(searchType:String, searchTerm:String) : ListResult = {
    val result:ListResult = if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_CLASS.equals(searchType) && searchTerm != null) {

      val listResult = MappingDocumentController.findMappingDocumentsByMappedClass(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_PROPERTY.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedProperty:" + searchTerm)
      val listResult = MappingDocumentController.findMappingDocumentsByMappedProperty(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_TABLE.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedTable:" + searchTerm)
      val listResult = MappingDocumentController.findMappingDocumentsByMappedTable(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_COLUMN.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedColumn:" + searchTerm)
      val listResult = MappingDocumentController.findMappingDocumentsByMappedColumn(searchTerm)
      listResult
    } else {
      logger.info("findAllMappingDocuments")
      val listResult = MappingDocumentController.findAllMappingDocuments
      listResult
    }
    logger.info("result = " + result)

    result;
  }

}
