package es.upm.fi.dia.oeg.mappingpedia.utility


import java.io.File
import java.net.HttpURLConnection
import java.util.Properties

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.model.result.ListResult
import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution, Agent}
import es.upm.fi.dia.oeg.mappingpedia.utility.CKANUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaEngine, MappingPediaProperties}
import eu.trentorise.opendata.jackan.CkanClient
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

class CKANUtility(val ckanUrl: String, val authorizationToken: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def createResource(distribution: Distribution, textBodyMap:Option[Map[String, String]]) = {
    logger.info("CREATING A RESOURCE ON CKAN ... ")
    //val dataset = distribution.dataset;

    val packageId = distribution.dataset.dctIdentifier;
    logger.info(s"packageId = $packageId")
    logger.info(s"url = ${distribution.dcatDownloadURL}")

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

      logger.info(s"distribution.dctDescription = ${distribution.dctDescription}")
      if(distribution.dctDescription != null) {
        builder.addTextBody("description", distribution.dctDescription)
      }

      logger.info(s"dataset.dcatMediaType = ${distribution.dcatMediaType}")
      if(distribution.dcatMediaType != null) {
        builder.addTextBody("mimetype", distribution.dcatMediaType)
      }

      logger.info(s"dataset.distributionFile = ${distribution.distributionFile}")
      if(distribution.distributionFile != null) {
        builder.addBinaryBody("upload", distribution.distributionFile)
      }



      if(textBodyMap != null && textBodyMap.isDefined) {

        for((key, value) <- textBodyMap.get) {

          if(key != null && value != null) {
            builder.addTextBody(key, value)
          } else {
            logger.warn(s"textBodyMap key,value = ${key},${value}")
          }
        }
      }


      val mpEntity = builder.build();
      httpPostRequest.setEntity(mpEntity)
      val response = httpClient.execute(httpPostRequest)
      logger.info(s"response = ${response}")

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
    } finally {
      //if (httpClient != null) httpClient.close()
    }


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


  def addNewOrganization(organization:Agent) = {
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
    if(dataset.dcatKeyword != null) {
      jsonObj.put("category", dataset.dcatKeyword);
    }
    if(dataset.dctLanguage != null) {
      jsonObj.put("language", dataset.dctLanguage);
    }

    //logger.info(s"dataset.dctLanguage = ${dataset.dctLanguage}")
    if(dataset.dctLanguage != null && !"".equals(dataset.dctLanguage)) {
      jsonObj.put("language", dataset.dctLanguage)
    }

    //logger.info(s"dataset.ckanSource = ${dataset.ckanSource}")
    if(dataset.ckanSource != null && !"".equals(dataset.ckanSource)) {
      jsonObj.put("source", dataset.ckanSource)
    }

    //logger.info(s"dataset.ckanVersion = ${dataset.ckanVersion}")
    if(dataset.ckanVersion != null && !"".equals(dataset.ckanVersion)) {
      jsonObj.put("version", dataset.ckanVersion)
    }

    //logger.info(s"dataset.getAuthor_name = ${dataset.getAuthor_name}")
    if(dataset.getAuthor_name != null && !"".equals(dataset.getAuthor_name)) {
      jsonObj.put("author", dataset.getAuthor_name)
    }

    //logger.info(s"dataset.getAuthor_email = ${dataset.getAuthor_email}")
    if(dataset.getAuthor_email != null && !"".equals(dataset.getAuthor_email)) {
      jsonObj.put("author_email", dataset.getAuthor_email)
    }

    //logger.info(s"dataset.getMaintainer_name = ${dataset.getMaintainer_name}")
    if(dataset.getMaintainer_name != null && !"".equals(dataset.getMaintainer_name)) {
      jsonObj.put("maintainer", dataset.getMaintainer_name)
    }

    //logger.info(s"dataset.getMaintainer_email = ${dataset.getMaintainer_email}")
    if(dataset.getMaintainer_email != null && !"".equals(dataset.getMaintainer_email)) {
      jsonObj.put("maintainer_email", dataset.getMaintainer_email)
    }

    //logger.info(s"dataset.ckanTemporal = ${dataset.ckanTemporal}")
    if(dataset.ckanTemporal != null && !"".equals(dataset.ckanTemporal)) {
      jsonObj.put("temporal", dataset.ckanTemporal)
    }

    //logger.info(s"dataset.ckanSpatial = ${dataset.ckanSpatial}")
    if(dataset.ckanSpatial != null && !"".equals(dataset.ckanSpatial)) {
      jsonObj.put("spatial", dataset.ckanSpatial)
    }

    //logger.info(s"dataset.ckanAccrualPeriodicity = ${dataset.ckanAccrualPeriodicity}")
    if(dataset.ckanAccrualPeriodicity != null && !"".equals(dataset.ckanAccrualPeriodicity)) {
      jsonObj.put("accrual_periodicity", dataset.ckanAccrualPeriodicity)
    }

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageCreate
    logger.info(s"Hitting endpoint: $uri");

    val response = Unirest.post(uri)
      .header("Authorization", this.authorizationToken)
      .body(jsonObj)
      .asJson();
    logger.debug(s"response.getHeaders = ${response.getHeaders}")
    logger.debug(s"response.getBody = ${response.getBody}")
    response;
  }



  def updateDatasetLanguage(organizationId:String, datasetId:String, language:String) : Integer = {
    val jsonObj = new JSONObject();

    jsonObj.put("owner_org", organizationId);
    jsonObj.put("name", datasetId);
    jsonObj.put("language", language);
    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageUpdate
    logger.info(s"Hitting endpoint: $uri");
    logger.info(s"owner_org = $organizationId");
    logger.info(s"name = $datasetId");
    logger.info(s"language = $language");

    val response = Unirest.post(uri)
      .header("Authorization", this.authorizationToken)
      .body(jsonObj)
      .asJson();
    response.getStatus

  }

  def getDatasets(organizationId:String) = {
    val uri = s"${MappingPediaEngine.mappingpediaProperties.ckanActionOrganizationShow}?id=$organizationId&include_datasets=true"
    logger.info(s"Hitting endpoint: $uri");

    val response = Unirest.get(uri)
      .header("Authorization", this.authorizationToken)
      .asJson();
    response
  }

  def updateDatasetLanguage(organizationId:String, language:String) : Integer = {
    val getDatasetsResponse = this.getDatasets(organizationId)
    val getDatasetsResponseStatus = getDatasetsResponse.getStatus
    if(getDatasetsResponseStatus >= 200 && getDatasetsResponseStatus < 300) {
      val packages = getDatasetsResponse.getBody.getObject.getJSONObject("result").getJSONArray("packages")
      for(i <- 0 to packages.length() - 1) {
        val pkg = packages.get(i)
        val datasetId = pkg.asInstanceOf[JSONObject].getString("id")
        logger.info(s"datasetId = $datasetId");

        this.updateDatasetLanguage(organizationId, datasetId, language);
      }
      HttpURLConnection.HTTP_OK

    } else {
      HttpURLConnection.HTTP_INTERNAL_ERROR
    }



  }
}

object CKANUtility {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def getDatasetList(catalogUrl:String) : ListResult = {
    val cc: CkanClient = new CkanClient(catalogUrl)
    val datasetList = cc.getDatasetList.asScala

    logger.info(s"ckanDatasetList $catalogUrl = " + datasetList)
    new ListResult(datasetList.size, datasetList)
  }

  def getResultId(response:CloseableHttpResponse) = {
    if(response == null) {
      ""
    } else {
      val httpEntity  = response.getEntity
      val entity = EntityUtils.toString(httpEntity)
      val responseEntity = new JSONObject(entity);
      responseEntity.getJSONObject("result").getString("id");
    }
  }
}
