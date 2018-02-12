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

      if(distribution.dctLanguage != null) {
        builder.addTextBody("language", distribution.dctLanguage)
      }

      if(distribution.dctRights != null) {
        builder.addTextBody("rights", distribution.dctRights)
      }

      if(distribution.hash != null) {
        builder.addTextBody("hash", distribution.hash)
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


      if (response.getStatusLine.getStatusCode < 200 || response.getStatusLine.getStatusCode >= 300) {
        logger.info(s"response = ${response}")
        logger.info(s"response.getAllHeaders= ${response.getAllHeaders}");
        logger.info(s"response.getEntity= ${response.getEntity}");
        logger.info(s"response.getProtocolVersion= ${response.getProtocolVersion}");
        logger.info(s"response.getStatusLine= ${response.getStatusLine}");
        logger.info(s"response.getClass= ${response.getClass}");

        throw new Exception("Failed to add the file to CKAN storage. Response status line from " + uploadFileUrl + " was: " + response.getStatusLine)
      }

      response
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

    val optionalFields:Option[Map[String, String]] = Some(Map(
      "title" -> dataset.dctTitle
      , "notes" -> dataset.dctDescription
      , "category" -> dataset.mvpCategory
      , "tag_string" -> dataset.dcatKeyword
      , "language" -> dataset.dctLanguage
      , "license_id" -> dataset.ckanPackageLicense
      , "url" -> dataset.dctSource
      , "version" -> dataset.ckanVersion
      , "author" -> dataset.getAuthor_name
      , "author_email" -> dataset.getAuthor_email
      , "maintainer" -> dataset.getMaintainer_name
      , "maintainer_email" -> dataset.getMaintainer_email
      , "temporal" -> dataset.ckanTemporal
      , "spatial" -> dataset.ckanSpatial
      , "accrualPeriodicity" -> dataset.ckanAccrualPeriodicity
      , "was_attributed_to" -> dataset.provWasAttributedTo
      , "was_generated_by" -> dataset.provWasGeneratedBy
      , "was_derived_from" -> dataset.provWasDerivedFrom
      , "accrualPeriodicity" -> dataset.ckanAccrualPeriodicity
      , "had_primary_source" -> dataset.provHadPrimarySource
      , "was_revision_of" -> dataset.provWasRevisionOf
      , "was_influenced_by" -> dataset.provWasInfluencedBy
    ))

    if(optionalFields != null && optionalFields.isDefined) {
      for((key, value) <- optionalFields.get) {
        if(key != null && !"".equals(key) && value != null && !"".equals(value)) {
          jsonObj.put(key, value)
        } else {
          logger.warn(s"jsonObj key,value = ${key},${value}")
        }
      }
    }


    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageCreate
    logger.info(s"Hitting endpoint: $uri");

    val response = Unirest.post(uri)
      .header("Authorization", this.authorizationToken)
      .body(jsonObj)
      .asJson();

    val responseStatus = response.getStatus
    val responseStatusText = response.getStatusText
    if (responseStatus < 200 || responseStatus >= 300) {
      logger.info(s"response.getBody= ${response.getBody}");
      logger.info(s"response.getHeaders= ${response.getHeaders}");
      logger.info(s"response.getRawBody= ${response.getRawBody}");
      logger.info(s"response.getStatus= ${response.getStatus}");
      logger.info(s"response.getStatusText= ${response.getStatusText}");
      throw new Exception(responseStatusText)
    }

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
