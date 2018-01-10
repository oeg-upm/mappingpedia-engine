package es.upm.fi.dia.oeg.mappingpedia.model

import java.io.File
import java.util.UUID

import com.mashape.unirest.http.Unirest
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by fpriyatna on 07/04/2017.
  * based on Dublin core Bibliographic Resource: http://dublincore.org/documents/dcmi-terms/#terms-BibliographicResource
  */
class MappingDocument(val dctIdentifier:String) {
  def this() = {
    this(UUID.randomUUID.toString)
  }

  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  //FIELDS FROM dct:BibliographicResource
  var dctTitle:String = null;
  var dctCreator:String = null;
  var dctSubject:String = null;
  var dctDateSubmitted:String = null;

  //FIELDS SPECIFIC FOR A MAPPING DOCUMENT
  var accessURL:String = null;
  private var downloadURL:String = null;
  var mappingLanguage:String = null;
  var distributionFieldSeparator:Option[String] = None;
  //var multipartFile: MultipartFile = null;
  var mappingDocumentFile:File = null;
  var sha:String = null;

  // TODO it is a hack by Freddy, refactor this
  /*
  var dataset:String = null;
  var distributionAccessURL:String = null;
  var distributionDownloadURL:String = null;
  var distributionSHA:String = null;
  */
  var dataset:Dataset = null;

  var manifestAccessURL:String = null;
  var manifestDownloadURL:String = null;

  def setDownloadURL(pDownloadURL:String) = {
    downloadURL = if(pDownloadURL != null && pDownloadURL.startsWith("<") && pDownloadURL.endsWith(">")) {
      pDownloadURL.substring(1, pDownloadURL.length-1)
    } else {
      pDownloadURL
    }
  }

  /*
  def getDownloadURL() = {
    this.downloadURL
  }
  */

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

  def getId = this.dctIdentifier;
  def getTitle = this.dctTitle;
  def getDataset = this.dataset;
  def getCreator = this.dctCreator;

  def getDateSubmitted = this.dctDateSubmitted;
  def getSHA = this.sha
  //def getDistribution_sha = this.distributionSHA

  def getMapping_language = this.mappingLanguage;
}
