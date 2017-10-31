package es.upm.fi.dia.oeg.mappingpedia.model

import java.io.File
import java.util.Date

import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.sdf

//based on dcat:Disctribution https://www.w3.org/TR/vocab-dcat/#class-distribution
//see also ckan:Resource  http://docs.ckan.org/en/ckan-1.7.4/domain-model-resource.html
class Distribution (val dataset: Dataset){
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  dataset.addDistribution(this);

  //FIELDS FROM DCAT
  var dctTitle:String = null;
  var dctDescription:String = null;
  var dcatAccessURL:String = null;
  var dcatDownloadURL:String = null;
  var dcatMediaType:String = null;

  //CUSTOM FIELDS
  var cvsFieldSeparator:String=",";
  var distributionFile:File = null;

  def generateManifestFile() = {
    val dataset = this.dataset;
    val organization = dataset.dctPublisher;

    var distributionAccessURL = this.dcatAccessURL
    if(distributionAccessURL != null && !distributionAccessURL.startsWith("<")) {
      distributionAccessURL = "<" + distributionAccessURL;
    }
    if(distributionAccessURL != null && !distributionAccessURL.endsWith(">")) {
      distributionAccessURL = distributionAccessURL + ">";
    }
    var distributionDownloadURL = this.dcatDownloadURL
    if(distributionDownloadURL != null && !distributionDownloadURL.startsWith("<")) {
      distributionDownloadURL = "<" + distributionDownloadURL;
    }
    if(distributionDownloadURL != null && !distributionDownloadURL.endsWith(">")) {
      distributionDownloadURL = distributionDownloadURL + ">";
    }

    logger.info("Generating manifest file ...")
    try {
      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-dataset-template.ttl"
        , "templates/metadata-distributions-template.ttl"
      );

      val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

      val mapValues:Map[String,String] = Map(
        "$datasetID" -> dataset.dctIdentifier
        , "$datasetTitle" -> dataset.dctTitle
        , "$datasetKeywords" -> dataset.dcatKeyword
        , "$publisherId" -> organization.dctIdentifier
        , "$datasetLanguage" -> dataset.dctLanguage
        , "$distributionID" -> dataset.dctIdentifier
        , "$distributionAccessURL" -> distributionAccessURL
        , "$distributionDownloadURL" -> distributionDownloadURL
        , "$distributionMediaType" -> this.dcatMediaType
      );

      val filename = "metadata-dataset.ttl";
      val manifestFile = MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, dataset.dctIdentifier);
      logger.info("Manifest file generated.")
      manifestFile;
    } catch {
      case e:Exception => {
        e.printStackTrace()
        val errorMessage = "Error occured when generating manifest file: " + e.getMessage
        null;
      }
    }
  }
}
