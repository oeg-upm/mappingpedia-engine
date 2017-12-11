package es.upm.fi.dia.oeg.mappingpedia.utility

import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import es.upm.fi.dia.oeg.mappingpedia.model.{OntologyClass, OntologyResource}
import es.upm.fi.dia.oeg.mappingpedia.model.result.ListResult
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import org.apache.jena.rdf.model.{ModelFactory, Resource}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.jena.ontology._
import org.apache.jena.vocabulary.{RDF, RDFS}

import scala.collection.JavaConversions._

class JenaClient(val ontModel:OntModel) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  val mapNormalizedTerms:Map[String, String] = {
    val subclassesLocalNames:List[String] = this.getSubclassesSummary("Thing").results.asInstanceOf[List[String]];
    val subclassesURIs:List[String] = this.getSubclassesSummary("http://schema.org/Thing").results.asInstanceOf[List[String]];

    (subclassesLocalNames:::subclassesURIs).distinct.flatMap(subclassesLocalName => {
      val normalizedLocalNames = MappingPediaUtility.normalizeTerm(subclassesLocalName);
      normalizedLocalNames.map(normalizedLocalName => normalizedLocalName -> subclassesLocalName)
    }).toMap
  }



  def getDetails(uri:String) : OntologyResource = {
    logger.info(s"uri = $uri");

    val resource = ontModel.getResource(uri);
    val ontResource = resource.as(classOf[OntResource]);
    val localName = ontResource.getLocalName
    val comment = ontResource.getComment("");
    val label = ontResource.getLabel("")

    new OntologyResource(uri, localName, label, comment);
  }

  def getProperties(ontClass: OntClass, direct:Boolean) : ListResult = {
    logger.info(s"ontClass = $ontClass");
    logger.info(s"direct = $direct");

    val properties = ontClass.listDeclaredProperties(direct).toList.toList

    val propertiesInString = properties.map(property => property.toString);

    new ListResult(propertiesInString.length, propertiesInString.asInstanceOf[List[String]])
  }

  /*  def isClass(resource:Resource) = {
      val rdfTypeValue = resource.getPropertyResourceValue(RDF.`type`).toString
      if("http://www.w3.org/2000/01/rdf-schema#Class".equalsIgnoreCase(rdfTypeValue)) {
        true
      } else {
        false
      }
    }

    def isProperty(resource:Resource) = {
      val rdfTypeValue = resource.getPropertyResourceValue(RDF.`type`).toString
      if("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property".equalsIgnoreCase(rdfTypeValue)) {
        true
      } else {
        false
      }
    }*/

  def getProperties(cls:String, direct:String) : ListResult = {
    val directBoolean = if("false".equalsIgnoreCase(direct) || "no".equalsIgnoreCase(direct)) {
      false
    } else {
      true
    }

    val classURI = MappingPediaUtility.getClassURI(cls);
    val resource = ontModel.getResource(classURI);

    val ontClass = try {
      resource.as(classOf[OntClass])
    } catch {
      case e:Exception => null
    }

    if(ontClass == null) {
      new ListResult(0, Nil)
    } else {
      this.getProperties(ontClass, directBoolean)
    }
  }

  def getSubclassesDetail(cls:OntClass) : ListResult = {
    //logger.info("Retrieving subclasses of = " + cls.getURI)
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
    val classURI = MappingPediaUtility.getClassURI(aClass, "http://schema.org/");
    val resource = ontModel.getResource(classURI);
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
    val classURI = MappingPediaUtility.getClassURI(pClass, "http://schema.org/");

    //    val normalizedClasses = MappingPediaUtility.normalizeTerm(classIRI);
    //    logger.info(s"normalizedClasses = $normalizedClasses");

    //  val resultAux:List[String] = normalizedClasses.flatMap(normalizedClass => {
    //logger.info(s"normalizedClass = $normalizedClass");
    try {
      //val schemaClass = this.mapNormalizedTerms(normalizedClass);
      //logger.info(s"schemaClass = $schemaClass");
      val resource = ontModel.getResource(classURI);
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

  def getSubclassesSummary(aClass:String) : ListResult = {
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
  def loadSchemaOrgOntology(virtuosoClient: VirtuosoClient, ontologyFileName:String, ontologyFormat:String) : OntModel = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass);

    //val ontologyFileName = "tree.jsonld";
    //val ontologyFormat = "JSON-LD";
    val ontModelSpec = OntModelSpec.RDFS_MEM_TRANS_INF;

    val defaultModel = virtuosoClient.readModelFromFile(ontologyFileName, ontologyFormat);
    val rdfsModel = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM_TRANS_INF, defaultModel)
    val ontProperties = rdfsModel.listOntProperties().toList
    logger.info(s"ontProperties  = " + ontProperties )
    logger.info(s"ontProperties.length  = ${ontProperties.length}")
    for(ontProperty <- ontProperties) {
      logger.info(s"ontProperty  = " + ontProperty )

      val domainOld = ontProperty.getDomain;
      logger.info(s"domainOld  = ${domainOld}")
      val rangeOld = ontProperty.getRange;
      logger.info(s"rangeOld  = ${rangeOld}")

      val schemaDomain = ontProperty.getPropertyResourceValue(MappingPediaConstant.SCHEMA_DOMAIN_INCLUDES_PROPERTY)
      if(schemaDomain != null) {
        ontProperty.addDomain(schemaDomain);

      }
      val schemaRange = ontProperty.getPropertyResourceValue(MappingPediaConstant.SCHEMA_RANGE_INCLUDES_PROPERTY)
      if(schemaRange != null) {
        ontProperty.addRange(schemaRange);
      }

      val domainNew = ontProperty.getDomain;
      logger.info(s"domainNew  = ${domainNew}")
      val rangeNew = ontProperty.getRange;
      logger.info(s"rangeNew  = ${rangeNew}")

    }






    rdfsModel;
  }

}
