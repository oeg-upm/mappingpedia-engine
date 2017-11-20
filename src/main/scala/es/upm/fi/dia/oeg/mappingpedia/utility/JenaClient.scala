package es.upm.fi.dia.oeg.mappingpedia.utility

import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.model.OntologyClass
import es.upm.fi.dia.oeg.mappingpedia.model.result.ListResult
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import org.apache.jena.rdf.model.ModelFactory
import org.slf4j.{Logger, LoggerFactory}
import org.apache.jena.ontology.{OntClass, OntModel, OntModelSpec}

import scala.collection.JavaConversions._

class JenaClient(val ontModel:OntModel) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def getSubclassesDetail(cls:OntClass, ontModel:OntModel) : ListResult = {
    logger.info("Retrieving subclasses of = " + cls.getURI)
    val clsSubClasses:List[OntClass] = cls.listSubClasses(false).toList.toList;

    val clsSuperclasses:List[OntClass] = cls.listSuperClasses(true).toList.toList;

    logger.info("clsSubClasses = " + clsSubClasses.mkString(","))

    val resultHead:OntologyClass = new OntologyClass(cls, clsSuperclasses, clsSubClasses);

    val resultTail:List[OntologyClass] = clsSubClasses.map(clsSubClass => {
      val tail = new OntologyClass(clsSubClass
        , clsSubClass.listSuperClasses(false).toList.toList
        , clsSubClass.listSubClasses(false).toList.toList
      );
      tail
    })

    val result = resultHead :: resultTail;

    val listResult = new ListResult(result.size, result);
    listResult;
  }

  def getSuperclasses(aClass:String, ontModel:OntModel) : ListResult = {
    logger.info(s"aClass = $aClass");
    val isLocalName = if(aClass.contains("/")) { false } else { true }
    logger.info(s"isLocalName = $isLocalName");
    val classIRI = if(isLocalName) {
      "http://schema.org/" + aClass
    } else {
      aClass
    }
    logger.info(s"classIRI = $classIRI");

    val resource = ontModel.getResource(classIRI);
    val cls = resource.as(classOf[OntClass])
    this.getSuperclasses(cls, ontModel);

  }

  def getSuperclasses(cls:OntClass, ontModel:OntModel) : ListResult = {

    logger.info("Retrieving superclasses of = " + cls.getURI)
    val clsSuperclasses:List[OntClass] = cls.listSuperClasses(true).toList.toList;
    val result = clsSuperclasses.map(clsSuperClass => {
      clsSuperClass.getLocalName
    })

    val listResult = new ListResult(result.size, result);
    listResult;
  }

  def getSubclassesDetail(aClass:String, ontModel:OntModel) : ListResult  = {
    logger.info(s"aClass = $aClass");
    val isLocalName = if(aClass.contains("/")) { false } else { true }
    logger.info(s"isLocalName = $isLocalName");
    val classIRI = if(isLocalName) {
      "http://schema.org/" + aClass
    } else {
      aClass
    }
    logger.info(s"classIRI = $classIRI");
    val resource = ontModel.getResource(classIRI);
    val cls = resource.as(classOf[OntClass])
    this.getSubclassesDetail(cls, ontModel);
  }

  def getSubclassesLocalNames(aClass:String, ontModel:OntModel) : ListResult = {
    val subclassesDetail= this.getSubclassesDetail(aClass, ontModel)


    if(subclassesDetail != null) {
      val subclassesInList:Iterable[String] = subclassesDetail.results.map(
        result => result.asInstanceOf[OntologyClass].getLocalName).toList.distinct
      val result = new ListResult(subclassesInList.size, subclassesInList);
      result
    } else {
      null
    }

  }

}

object JenaClient {
  def loadSchemaOrgOntology(ontologyFileName:String, ontologyFormat:String) : OntModel = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass);

    //val ontologyFileName = "tree.jsonld";
    //val ontologyFormat = "JSON-LD";
    val ontModelSpec = OntModelSpec.RDFS_MEM_TRANS_INF;

    val defaultModel = MappingPediaEngine.virtuosoClient.readModelFromFile(ontologyFileName, ontologyFormat);
    val rdfsModel = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM_TRANS_INF, defaultModel)
    rdfsModel;
  }

}
