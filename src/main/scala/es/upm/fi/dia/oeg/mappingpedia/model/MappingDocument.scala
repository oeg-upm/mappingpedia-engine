package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

import com.mashape.unirest.http.Unirest
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

/**
  * Created by fpriyatna on 07/04/2017.
  * based on Dublin core Bibliographic Reource
  */
class MappingDocument(val dctIdentifier:String) {
  def this() = {
    this(UUID.randomUUID.toString)
  }

  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  var title:String = null;
  var dataset:String = null; // it is a hack by Dr. Freddy
  var filePath:String = null;
  var creator:String = null;
  //var distribution:String = null;
  var subject:String = null;
  var distributionAccessURL:String = null;
  var mappingLanguage:String = null;
  var distributionFieldSeparator:Option[String] = None;
  private var accessURL:String = null;
  private var downloadURL:String = null;
  var multipartFile: MultipartFile = null;


  def setDownloadURL(pMappingDocumentURL:String) = {
    downloadURL = if(pMappingDocumentURL != null && pMappingDocumentURL.startsWith("<") && pMappingDocumentURL.endsWith(">")) {
      pMappingDocumentURL.substring(1, pMappingDocumentURL.length-1)
    } else {
      pMappingDocumentURL
    }
  }

  def getDownloadURL() = {
    if (downloadURL != null) {
      downloadURL
    } else {
      if(accessURL == null) {
        null
      } else {
        try {
          val response = Unirest.get(accessURL).asJson();
          downloadURL = response.getBody.getObject.getString("download_url");
          downloadURL;
        } catch {
          case e: Exception => accessURL
        }
      }
    }
  }



  //
  //logger.info("response = " + response);
  //logger.info("response.getBody= " + response.getBody);
  //logger.info("response.getBody.getObject= " + response.getBody.getObject);
  //logger.info("response.getBody.getObject.getString(\"downloadURL\")= " + response.getBody.getObject.getString("download_url"));

  //val mappingDocumentDownloadURL = response.getBody.getObject.getString("downloadURL");
  //val mappingDocumentDownloadURL = response.getBody.getObject.getString("download_url");

  def getId = this.dctIdentifier;
  def getTitle = this.title;
  def getDataset = this.dataset;
  def getCreator = this.creator;

  def getMappingDocumentFile = this.filePath
  //def getDistribution = this.distribution;
  //def getDistributionAccessURL = this.distributionAccessURL;
  //def getMappingDocumentDownloadURL = this.mappingDocumentDownloadURL;
}
