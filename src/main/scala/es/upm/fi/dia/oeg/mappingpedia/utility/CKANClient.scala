package es.upm.fi.dia.oeg.mappingpedia.utility


import java.io.File
import java.net.HttpURLConnection

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.model.result.ListResult
import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution, Organization}
import es.upm.fi.dia.oeg.mappingpedia.utility.CKANClient.logger
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaEngine, MappingPediaProperties}
import eu.trentorise.opendata.jackan.CkanClient
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

class CKANClient(val ckanUrl: String, val authorizationToken: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def createResource(distribution: Distribution) = {
    val dataset = distribution.dataset;
    logger.info(s"dataset = $dataset")

    val packageId = dataset.dctIdentifier;
    logger.info(s"packageId = $packageId")
    logger.info(s"url = ${distribution.dcatDownloadURL}")
    logger.info(s"description = ${distribution.dctDescription}")
    logger.info(s"mimetype = ${distribution.dcatMediaType}")
    logger.info(s"upload = ${distribution.distributionFile}")

    val httpClient = HttpClientBuilder.create.build
    try {

      val uploadFileUrl = ckanUrl + "/api/action/resource_create"
      logger.info(s"Hitting endpoint: $uploadFileUrl");

      val httpPostRequest = new HttpPost(uploadFileUrl)
      httpPostRequest.setHeader("Authorization", authorizationToken)
      val builder = MultipartEntityBuilder.create()
        .addTextBody("package_id", packageId)
        .addTextBody("url", distribution.dcatDownloadURL)
      ;

      if(distribution.dctDescription != null) {
        builder.addTextBody("description", distribution.dctDescription)
      }

      if(distribution.dcatMediaType != null) {
        builder.addTextBody("mimetype", distribution.dcatMediaType)
      }
      if(distribution.distributionFile != null) {
        builder.addBinaryBody("upload", distribution.distributionFile)
      }

      val mpEntity = builder.build();
      httpPostRequest.setEntity(mpEntity)
      val response = httpClient.execute(httpPostRequest)
      response

      /*
      val responseStatus = response.getStatusLine;

      if (response.getStatusLine.getStatusCode < 200 || response.getStatusLine.getStatusCode >= 300)
        throw new RuntimeException("failed to add the file to CKAN storage. response status line from " + uploadFileUrl + " was: " + response.getStatusLine)
      val httpEntity  = response.getEntity
      val entity = EntityUtils.toString(httpEntity)
      //logger.info(s"entity = " + entity)
      val responseEntity = new JSONObject(entity);
      //logger.info(s"responseEntity = " + responseEntity)
      (responseStatus, responseEntity);
      */

    } catch {
      case e: Exception => {
        e.printStackTrace()
        //HttpURLConnection.HTTP_INTERNAL_ERROR
        throw e;
      }

      // log error
    } finally if (httpClient != null) httpClient.close()

  }

  def updateResource(filePath: String, resourceId: String) : Integer = {
    val file = new File(filePath);
    this.updateResource(file, resourceId);
  }

  def updateResource(file:File, resourceId: String) : Integer = {
    val httpClient = HttpClientBuilder.create.build
    try {
      val uploadFileUrl = ckanUrl + "/api/action/resource_update"
      val httpPostRequest = new HttpPost(uploadFileUrl)
      httpPostRequest.setHeader("Authorization", authorizationToken)
      val mpEntity = MultipartEntityBuilder.create().addBinaryBody("upload", file)
        .addTextBody("id", resourceId).build();
      httpPostRequest.setEntity(mpEntity)
      val response = httpClient.execute(httpPostRequest)
      if (response.getStatusLine.getStatusCode < 200 || response.getStatusLine.getStatusCode >= 300) throw new RuntimeException("failed to add the file to CKAN storage. response status line from " + uploadFileUrl + " was: " + response.getStatusLine)
      val responseEntity = response.getEntity
      System.out.println(responseEntity.toString)
      HttpURLConnection.HTTP_OK
    } catch {
      case e: Exception => {
        e.printStackTrace()
        HttpURLConnection.HTTP_INTERNAL_ERROR
      }
    } finally if (httpClient != null) httpClient.close()

  }


  def addNewOrganization(organization:Organization) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", organization.foafName);

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionOrganizationCreate
    logger.info(s"Hitting endpoint: $uri");
    val response = Unirest.post(uri)
      .header("Authorization", this.authorizationToken)
      .body(jsonObj)
      .asJson();
    response;
  }

  def addNewPackage(dataset:Dataset) = {
    val organization = dataset.dctPublisher;

    val jsonObj = new JSONObject();
    jsonObj.put("name", dataset.dctIdentifier);
    jsonObj.put("owner_org", organization.dctIdentifier);
    if(dataset.dctTitle != null) {
      jsonObj.put("title", dataset.dctTitle);
    }
    if(dataset.dctDescription != null) {
      jsonObj.put("notes", dataset.dctDescription);
    }

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageCreate
    logger.info(s"Hitting endpoint: $uri");

    val response = Unirest.post(uri)
      .header("Authorization", this.authorizationToken)
      .body(jsonObj)
      .asJson();
    response;
  }

}

object CKANClient {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def getDatasetList(catalogUrl:String) : ListResult = {
    val cc: CkanClient = new CkanClient(catalogUrl)
    val datasetList = cc.getDatasetList.asScala

    logger.info(s"ckanDatasetList $catalogUrl = " + datasetList)
    new ListResult(datasetList.size, datasetList)
  }

}
