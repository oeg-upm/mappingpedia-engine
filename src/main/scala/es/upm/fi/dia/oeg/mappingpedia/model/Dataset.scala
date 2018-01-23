package es.upm.fi.dia.oeg.mappingpedia.model

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

//based on dcat:Dataset
//see also ckan:Package
class Dataset(val dctPublisher:Agent, val dctIdentifier:String){
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val createdDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())

  def this(dctPublisher:Agent) {
    this(dctPublisher, UUID.randomUUID.toString)
  }

  def this(dctIdentifier:String) {
    this(new Agent(), dctIdentifier)
  }

  var dctTitle:String = null;
  var dctDescription:String = null;
  var dctIssued:String = createdDate;
  var dctModified:String = createdDate;
  var dcatKeyword:String = null;
  var dctLanguage:String = null;
  var dcatDistribution:List[Distribution] = Nil;

  var manifestAccessURL:String = null;
  var manifestDownloadURL:String = null;

  var ckanPackageId:String = null;
  var ckanPackageName:String = null;
  var ckanSource:String = null;
  var ckanVersion:String = null;
  var ckanAuthor:Agent = null;
  var ckanMaintener:Agent = null;
  var ckanTemporal:String = null;
  var ckanSpatial:String = null;
  var ckanAccrualPeriodicity:String = null;


  //var mappingDocuments:List[MappingDocument] = Nil;

  //for the moment assume that only one distribution for each dataset
  def getDistribution() = if(dcatDistribution == Nil) {
    null
  } else {
    this.dcatDistribution.iterator.next()
  }

  /*
  def getMapping_documents() = {
    logger.info(s"this.mappingDocuments.iterator = ${this.mappingDocuments.iterator}")

    this.mappingDocuments.iterator.asJava

  }
  */

  /*
  def getMapping_document() = {
    if(this.mappingDocuments == null || this.mappingDocuments.size == 0) {
      null
    } else {
      this.mappingDocuments.iterator.next()
    }
  }
  */

  def addDistribution(distribution: Distribution) = {
    if(this.dcatDistribution == null) {
      this.dcatDistribution = List(distribution);
    } else {
      if(!this.dcatDistribution.contains(distribution)) {
        this.dcatDistribution = distribution :: this.dcatDistribution;
      }
    }
  }

  def getId = this.dctIdentifier;

  def getTitle = this.dctTitle

  def getCKAN_package_id = this.ckanPackageId

  def getAuthor_name = if(this.ckanAuthor == null) { null } else { this.ckanAuthor.foafName}
  def getAuthor_email = if(this.ckanAuthor == null) { null } else { this.ckanAuthor.foafMbox}

  def getMaintainer_name = if(this.ckanMaintener == null) { null } else { this.ckanMaintener.foafName}
  def getMaintainer_email = if(this.ckanMaintener == null) { null } else { this.ckanMaintener.foafMbox}

}
