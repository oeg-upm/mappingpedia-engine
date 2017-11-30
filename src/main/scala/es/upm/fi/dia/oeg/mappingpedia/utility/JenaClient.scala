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

  val mapNormalizedTerms:Map[String, String] = {
    val subclassesLocalNames = this.getSubclassesLocalNames("http://schema.org/Thing").results.asInstanceOf[Iterable[String]];
    subclassesLocalNames.flatMap(subclassesLocalName => {
      val normalizedLocalNames = MappingPediaUtility.normalizeTerm(subclassesLocalName);
      normalizedLocalNames.map(normalizedLocalName => normalizedLocalName -> subclassesLocalName)
    }).toMap
  }


  def getSubclassesDetail(cls:OntClass) : ListResult = {
    logger.info("Retrieving subclasses of = " + cls.getURI)
    val clsSubClasses:List[OntClass] = cls.listSubClasses(false).toList.toList;

    val clsSuperclasses:List[OntClass] = cls.listSuperClasses(true).toList.toList;

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

  def getSuperclasses(aClass:String) : ListResult = {
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
    this.getSuperclasses(cls);

  }

  def getSuperclasses(cls:OntClass) : ListResult = {

    logger.info("Retrieving superclasses of = " + cls.getURI)
    val clsSuperclasses:List[OntClass] = cls.listSuperClasses(true).toList.toList;
    val result = clsSuperclasses.map(clsSuperClass => {
      clsSuperClass.getLocalName
    })

    val listResult = new ListResult(result.size, result);
    listResult;
  }

  def getSubclassesDetail(pClass:String) : ListResult  = {
    logger.info(s"pClass = $pClass");
    val isLocalName = if(pClass.contains("/")) { false } else { true }
    logger.info(s"isLocalName = $isLocalName");
    val classIRI = if(isLocalName) {
      "http://schema.org/" + pClass
    } else {
      pClass
    }
    logger.info(s"classIRI = $classIRI");

//    val normalizedClasses = MappingPediaUtility.normalizeTerm(classIRI);
//    logger.info(s"normalizedClasses = $normalizedClasses");

  //  val resultAux:List[String] = normalizedClasses.flatMap(normalizedClass => {
      //logger.info(s"normalizedClass = $normalizedClass");
      try {
        //val schemaClass = this.mapNormalizedTerms(normalizedClass);
        //logger.info(s"schemaClass = $schemaClass");
        val resource = ontModel.getResource(classIRI);
        val cls = resource.as(classOf[OntClass])
        val result = this.getSubclassesDetail(cls).results.asInstanceOf[List[String]];
        val listResult = new ListResult(result.size, result);
        listResult
      } catch {
        case e:Exception => {
          new ListResult(0, Nil);
        }
      }
    //});


  }

  def getSubclassesLocalNames(aClass:String) : ListResult = {
    val isLocalName = if(aClass.contains("/")) { false } else { true }

    val subclassesDetail= this.getSubclassesDetail(aClass)


    if(subclassesDetail != null) {
      val subclassesInList:Iterable[String] = subclassesDetail.results.map(result =>
        if(isLocalName) {
          result.asInstanceOf[OntologyClass].getLocalName
        } else {
          result.asInstanceOf[OntologyClass].getURI
        }).toList.distinct

      new ListResult(subclassesInList.size, subclassesInList);
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
