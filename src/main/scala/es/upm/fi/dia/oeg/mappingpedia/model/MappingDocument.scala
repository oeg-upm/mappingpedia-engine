package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

import com.mashape.unirest.http.Unirest
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

/**
  * Created by fpriyatna on 07/04/2017.
  */
class MappingDocument(val dctIdentifier:String) {
  def this() = {
    this(UUID.randomUUID.toString)
  }

  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  var title:String = null;
  var dataset:String = null;
  var filePath:String = null;
  var creator:String = null;
  var distribution:String = null;
  var subject:String = null;
  var distributionAccessURL:String = null;
  var mappingLanguage:String = null;
  var distributionFieldSeparator:Option[String] = None;
  private var mappingDocumentAccessURL:String = null;
  private var mappingDocumentDownloadURL:String = null;
  var multipartFile: MultipartFile = null;


  def setMappingdocumentURL(pMappingDocumentURL:String) = {
    if(pMappingDocumentURL != null && pMappingDocumentURL.startsWith("<") && pMappingDocumentURL.endsWith(">")) {
      pMappingDocumentURL.substring(1, pMappingDocumentURL.length-1)
    } else {
      pMappingDocumentURL
    }
  }

  def getMappingDocumentDownloadURL() = {
    if (mappingDocumentDownloadURL != null) {
      mappingDocumentDownloadURL
    } else {
      if(mappingDocumentAccessURL == null) {
        null
      } else {
        try {
          val response = Unirest.get(mappingDocumentAccessURL).asJson();
          mappingDocumentDownloadURL = response.getBody.getObject.getString("download_url");
          mappingDocumentDownloadURL;
        } catch {
          case e: Exception => mappingDocumentAccessURL
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
  //def getDistribution = this.distribution;
  def getDistributionAccessURL = this.distributionAccessURL;
  //def getMappingDocumentDownloadURL = this.mappingDocumentDownloadURL;
}
