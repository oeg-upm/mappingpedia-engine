package es.upm.fi.dia.oeg.mappingpedia.model

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

//based on dcat:Dataset
// and ckan:Package

class Dataset(val dctPublisher:Agent, val dctIdentifier:String) extends Entity {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val createdDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())

  def this(dctPublisher: Agent) {
    this(dctPublisher, UUID.randomUUID.toString)
  }

  def this(dctIdentifier: String) {
    this(new Agent(), dctIdentifier)
  }

  var dctTitle: String = null;
  var dctDescription: String = null;
  var dctIssued: String = createdDate;
  var dctModified: String = createdDate;
  var dcatKeyword: String = null;
  var dctLanguage: String = null;
  var dcatDistributions: List[Distribution] = Nil;

  var manifestAccessURL: String = null;
  var manifestDownloadURL: String = null;

  var dctAccessRight:String = null;
  var dctProvenance:String = null;

  var ckanPackageId: String = null;
  var ckanPackageName: String = null;
  var dctSource: String = null;
  var ckanVersion: String = null;
  var ckanAuthor: Agent = null;
  var ckanMaintener: Agent = null;
  var ckanTemporal: String = null;
  var ckanSpatial: String = null;
  var ckanAccrualPeriodicity: String = null;

  var superset: Dataset = null;

  var mvpCategory: String = null;


  //var mappingDocuments:List[MappingDocument] = Nil;

  //for the moment assume that only one distribution for each dataset


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
    if (this.dcatDistributions == null) {
      this.dcatDistributions = List(distribution);
    } else {
      if (!this.dcatDistributions.contains(distribution)) {
        this.dcatDistributions = distribution :: this.dcatDistributions;
      }
    }
  }


  def getId = this.dctIdentifier;

  def getTitle = this.dctTitle

  def getCKAN_package_id = this.ckanPackageId

  def getAuthor_name = if (this.ckanAuthor == null) {
    null
  } else {
    this.ckanAuthor.foafName
  }

  def getAuthor_email = if (this.ckanAuthor == null) {
    null
  } else {
    this.ckanAuthor.foafMbox
  }

  def getMaintainer_name = if (this.ckanMaintener == null) {
    null
  } else {
    this.ckanMaintener.foafName
  }

  def getMaintainer_email = if (this.ckanMaintener == null) {
    null
  } else {
    this.ckanMaintener.foafMbox
  }

  def getDistribution() = if (dcatDistributions == Nil) {
    null
  } else {
    this.dcatDistributions.iterator.next()
  }

  def setTitle(pDatasetTitle1: String, pDatasetTitle2: String) = {
    if (pDatasetTitle1 != null && !("" == pDatasetTitle1)) { this.dctTitle = pDatasetTitle1 }
    else if (pDatasetTitle2 != null && !("" == pDatasetTitle2)) {this.dctTitle = pDatasetTitle2}
    else {this.dctTitle = this.dctIdentifier }
  }

  def setDescription(pDatasetDescription1:String, pDatasetDescription2:String) = {
    if(pDatasetDescription1 != null && !"".equals(pDatasetDescription1)) {
      this.dctDescription = pDatasetDescription1;
    } else if(pDatasetDescription2 != null  && !"".equals(pDatasetDescription2)) {
      this.dctDescription = pDatasetDescription2;
    } else {
      this.dctDescription = this.dctIdentifier;
    }
  }

  def setKeywords(pDatasetKeywords1:String, pDatasetKeywords2:String) = {
    if(pDatasetKeywords1 != null && !"".equals(pDatasetKeywords1)) {
      this.dcatKeyword = pDatasetKeywords1;
    } else if(pDatasetKeywords2 != null && !"".equals(pDatasetKeywords2)) {
      this.dcatKeyword = pDatasetKeywords2;
    }
  }

  def setLanguage(pDatasetLanguage1:String, pDatasetLanguage2:String) = {
    if(pDatasetLanguage1 != null && !"".equals(pDatasetLanguage1)) {
      this.dctLanguage = pDatasetLanguage1;
    } else if(pDatasetLanguage2 != null && !"".equals(pDatasetLanguage2)) {
      this.dctLanguage = pDatasetLanguage2;
    } else {
      this.dctLanguage = "";
    }
  }

  def setAuthor(authorName:String, authorEmail:String) = {
    val author = new Agent();
    author.foafName = authorName;
    author.foafMbox = authorEmail;
    this.ckanAuthor = author;
  }

  def setMaintainer(maintainerName:String, maintainerEmail:String) = {
    val maintainer = new Agent();
    maintainer.foafName = maintainerName;
    maintainer.foafMbox = maintainerEmail;
    this.ckanMaintener = maintainer;
  }



}

object Dataset {
  def apply(dctPublisher:Agent, dctIdentifier:String) : Dataset = {
    if(dctIdentifier == null) {
      new Dataset(dctPublisher)
    } else {
      new Dataset(dctPublisher, dctIdentifier);
    }
  }

  def apply(organizationId:String, dctIdentifier:String) : Dataset = {
    val dctPublisher = new Agent(organizationId);
    Dataset.apply(dctPublisher, dctIdentifier);
  }
}
