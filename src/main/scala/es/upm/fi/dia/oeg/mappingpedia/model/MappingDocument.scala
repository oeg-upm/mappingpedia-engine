package es.upm.fi.dia.oeg.mappingpedia.model

import java.io.File
import java.util.UUID

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

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
  var hash:String = null;

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

  //FIELDS FOR CKAN
  var ckanPackageId:String = null;
  var ckanResourceId:String = null;

  def setDownloadURL(pDownloadURL:String) = {
    downloadURL = if(pDownloadURL != null && pDownloadURL.startsWith("<") && pDownloadURL.endsWith(">")) {
      pDownloadURL.substring(1, pDownloadURL.length-1)
    } else {
      pDownloadURL
    }
  }


  def getDownloadURL() = {
    if (downloadURL != null) {
      downloadURL
    } else {
      if(accessURL != null) {
        try {
          val response = Unirest.get(accessURL).asJson();
          downloadURL = response.getBody.getObject.getString("download_url");
          downloadURL;
        } catch {
          case e: Exception => accessURL
        }

      } else {
        null
      }
    }
  }

  def getHash= this.hash
  def getId = this.dctIdentifier;
  def getTitle = this.dctTitle;
  def getCreator = this.dctCreator;
  def getDateSubmitted = this.dctDateSubmitted;

  def getMapping_language = this.mappingLanguage;
  def getCKAN_package_id = this.ckanPackageId
  def getCKAN_resource_id = this.ckanResourceId
  def getDataset = this.dataset;

  def setTitle(pTitle1: String) : Unit = {
    this.setTitle(pTitle1, null)
  }

  def setTitle(pTitle1: String, pTitle2: String) : Unit = {
    if (pTitle1 != null && !("" == pTitle1)) { this.dctTitle = pTitle1 }
    else if (pTitle2 != null && !("" == pTitle2)) {this.dctTitle = pTitle2}
    else {this.dctTitle = this.dctIdentifier }
  }

  def setMappingLanguage(pLanguage1:String, pLanguage2:String) = {
    if(pLanguage1 != null && !"".equals(pLanguage1)) {
      this.mappingLanguage = pLanguage1;
    } else if(pLanguage2 != null && !"".equals(pLanguage2)) {
      this.mappingLanguage = pLanguage2;
    }
  }

  def setFile(pFile1:MultipartFile, pFile2:MultipartFile, datasetId:String) = {
    if(pFile1 != null) {
      this.mappingDocumentFile = MappingPediaUtility.multipartFileToFile(pFile1, datasetId);}
    else if(pFile2 != null) {
      this.mappingDocumentFile = MappingPediaUtility.multipartFileToFile(pFile2 , datasetId);
    }
  }

  def setDownloadURL(pDownloadURL1:String, pDownloadURL2:String) = {
    if(pDownloadURL1 != null) {
      this.downloadURL = pDownloadURL1;
    } else if(pDownloadURL2 != null) {
      this.downloadURL = pDownloadURL2;
    }
  }
}
