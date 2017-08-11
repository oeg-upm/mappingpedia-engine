package es.upm.fi.dia.oeg.mappingpedia.model

import com.mashape.unirest.http.Unirest
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by fpriyatna on 07/04/2017.
  */
class MappingDocument(val id:String, val title:String, val dataset:String
  , val filePath:String, val creator:String
  , val distribution:String, val distributionAccessURL:String
  , val pMappingDocumentURL:String, val mappingLanguage:Option[String]) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  var distributionFieldSeparator:Option[String] = None;

  val mappingDocumentURL = if(pMappingDocumentURL.startsWith("<") && pMappingDocumentURL.endsWith(">")) {
    pMappingDocumentURL.substring(1, pMappingDocumentURL.length-1)
  } else {
    pMappingDocumentURL
  }

  val mappingDocumentDownloadURL = try {
    val response = Unirest.get(mappingDocumentURL).asJson();
    response.getBody.getObject.getString("download_url");
  } catch {
    case e:Exception => mappingDocumentURL
  }
  //
  //logger.info("response = " + response);
  //logger.info("response.getBody= " + response.getBody);
  //logger.info("response.getBody.getObject= " + response.getBody.getObject);
  //logger.info("response.getBody.getObject.getString(\"downloadURL\")= " + response.getBody.getObject.getString("download_url"));

  //val mappingDocumentDownloadURL = response.getBody.getObject.getString("downloadURL");
  //val mappingDocumentDownloadURL = response.getBody.getObject.getString("download_url");

  def getId = this.id;
  def getTitle = this.title;
  def getDataset = this.dataset;
  def getCreator = this.creator;
  //def getDistribution = this.distribution;
  def getDistributionAccessURL = this.distributionAccessURL;
  def getMappingDocumentURL = this.mappingDocumentURL;
  def getMappingDocumentDownloadURL = this.mappingDocumentDownloadURL;
}
