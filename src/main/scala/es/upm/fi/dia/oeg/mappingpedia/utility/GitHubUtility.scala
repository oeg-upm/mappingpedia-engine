package es.upm.fi.dia.oeg.mappingpedia.utility

import java.io.{File, FileInputStream}
import java.net.HttpURLConnection

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.mashape.unirest.http.{HttpResponse, JsonNode, Unirest}
import es.upm.fi.dia.oeg.mappingpedia.model.Dataset
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine, MappingPediaProperties}
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by fpriyatna on 21/02/2017.
  */
class GitHubUtility(githubRepository:String, githubUsername:String, githubAccessToken:String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  //val githubRepository:String = MappingPediaEngine.mappingpediaProperties.githubRepository;
  //val githubUsername:String = MappingPediaEngine.mappingpediaProperties.githubUser;
  //val githubAccessToken:String = MappingPediaEngine.mappingpediaProperties.githubAccessToken;

  def getDownloadURL(accessURL:String) : String = {
    val downloadURL = if(accessURL != null) {
      try {
        Unirest.get(accessURL).basicAuth(githubUsername, githubAccessToken).asJson().getBody.getObject.getString("download_url");
      } catch {
        case e:Exception => accessURL
      }
    }  else {
      null
    }

    downloadURL
  }


  def getDownloadURL(githubResponse:HttpResponse[JsonNode]) : String = {
    if(githubResponse != null) {
      val accessURL = this.getAccessURL(githubResponse)
      val downloadURL = this.getDownloadURL(accessURL);
      downloadURL
    } else {
      null
    }
  }

  def getSHA(githubResponse:HttpResponse[JsonNode]) : String = {
    if(githubResponse != null) {
      val accessURL = this.getAccessURL(githubResponse)
      val sha = this.getSHA(accessURL);
      sha
    } else {
      null
    }
  }

  def getAccessURL(githubResponse:HttpResponse[JsonNode]) : String = {
    if(githubResponse != null) {
      val responseStatus = githubResponse.getStatus

      if (HttpURLConnection.HTTP_CREATED == responseStatus || HttpURLConnection.HTTP_OK == responseStatus) {
        val accessURL = githubResponse.getBody.getObject.getJSONObject("content").getString("url");
        accessURL
      } else {
        null
      }
    } else {
      null
    }
  }




  def getFile(organizationId:String, datasetId:String, filename:String) = {
    //val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/mappingpedia-testuser/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c.ttl";
    //val response = Unirest.get(uri).asJson();

    //val uri = MappingPediaEngine.mappingpediaProperties.githubRepoContents + "/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val uri = MappingPediaConstant.GITHUB_ACCESS_URL_PREFIX + "/{organizationId}/{datasetId}/{filename}";
    val response = Unirest.get(uri)
      .routeParam("organizationId", organizationId)
      .routeParam("datasetId", datasetId)
      .routeParam("filename", filename)
      .basicAuth(githubUsername, githubAccessToken)
      //.header("Content-Type", "application/json")
      //.body(jsonObj)
      .asJson();
    //logger.info("responseHeaders = " + response.getHeaders);
    //logger.info("responseBody = " + response.getBody);
    response;
  }

  def getSHA(organizationID:String, datasetID:String, filename:String) : String = {
    val githubRepository = MappingPediaEngine.mappingpediaProperties.githubRepository;
    val uri = MappingPediaConstant.GITHUB_ACCESS_URL_PREFIX + s"${githubRepository}/contents/${organizationID}/${datasetID}/${filename}";

    try {
      val response = Unirest.get(uri)
        //.routeParam("mappingpediaUsername", mappingpediaUsername)
        //.routeParam("mappingDirectory", mappingDirectory)
        //.routeParam("mappingFilename", mappingFilename)
        //.routeParam("mappingFileExtension", mappingFileExtension)
        .basicAuth(githubUsername, githubAccessToken)
        .asJson();

      response.getBody.getObject.getString("sha");
    } catch {
      case e:Exception => {
        val errorMessage = s"Error getting sha for $uri"
        logger.error(errorMessage);
        null
      }
    }

  }

  def putEncodedContent(organizationId:String, datasetId:String, filename:String
                        , message:String, base64EncodedContent:String
                       ) : HttpResponse[JsonNode] = {
    val filePath = s"${organizationId}/${datasetId}/${filename}";
    this.putEncodedContent(filePath, message, base64EncodedContent);
  }

  def putEncodedContent(filePath:String, message:String, base64EncodedContent:String) : HttpResponse[JsonNode] = {
    val githubRepository = MappingPediaEngine.mappingpediaProperties.githubRepository;
    val uri = MappingPediaConstant.GITHUB_ACCESS_URL_PREFIX + s"${githubRepository}/contents/${filePath}";

    val jsonObj = new JSONObject();
    jsonObj.put("message", message);
    jsonObj.put("content", base64EncodedContent);

    try {
      val sha = this.getSHA(uri);
      jsonObj.put("sha", sha);
    } catch {
      case e:Exception => {
      }
    }

    logger.info(s"hitting github url $uri");
    //logger.info(s"message = $message");
    //logger.info(s"content = $base64EncodedContent");

    val response = Unirest.put(uri)
      .basicAuth(githubUsername, githubAccessToken)
      .body(jsonObj)
      .asJson();
    response;
  }

  def getSHA(url:String) : String = {
    val response = Unirest.get(url)
      .basicAuth(githubUsername, githubAccessToken)
      .asJson();
    val responseStatus = response.getStatus
    if(responseStatus >= 200 && responseStatus < 300) {
      response.getBody.getObject.getString("sha");
    } else {
      null
    }
  }

  def encodeAndPutFile(organizationId:String, datasetId:String, filename:String, message:String, file:File) = {
    val base64EncodedContent = GitHubUtility.encodeToBase64(file);
    this.putEncodedContent(organizationId, datasetId, filename, message, base64EncodedContent)
  }

  def encodeAndPutFile(filePath:String, message:String, file:File) = {
    val base64EncodedContent = GitHubUtility.encodeToBase64(file);
    this.putEncodedContent(filePath, message, base64EncodedContent)
  }

}

object GitHubUtility {
  /*
  val stringToEncode = "foo"; 
  val encodedX = BaseEncoding.base32().encode(stringToEncode.getBytes(Charsets.US_ASCII))
  println("encodedX = " + encodedX);
  val decodedX = BaseEncoding.base32().decode(encodedX);
  println("decodedX = " + decodedX);
  val decodedXString = new String(decodedX, "UTF-8");
  println("decodedXString = " + decodedXString);
  */

  /*
  val encodedString = "QHByZWZpeCBycjogPGh0dHA6Ly93d3cudzMub3JnL25zL3Iycm1sIz4gLgpA\ncHJlZml4IGZvYWY6IDxodHRwOi8veG1sbnMuY29tL2ZvYWYvMC4xLz4gLgpA\ncHJlZml4IGV4OiA8aHR0cDovL2V4YW1wbGUuY29tLz4gLgpAcHJlZml4IHhz\nZDogPGh0dHA6Ly93d3cudzMub3JnLzIwMDEvWE1MU2NoZW1hIz4gLgpAYmFz\nZSA8aHR0cDovL21hcHBpbmdwZWRpYS5vcmcvcmRiMnJkZi9yMnJtbC90Yy8+\nIC4KCjxUcmlwbGVzTWFwMT4KICAgIGEgcnI6VHJpcGxlc01hcDsKICAgICAg\nICAKICAgIHJyOmxvZ2ljYWxUYWJsZSBbIHJyOnRhYmxlTmFtZSAiXCJQZXJz\nb25cIiI7IF0gOwoJCiAgICBycjpzdWJqZWN0TWFwIFsgcnI6dGVtcGxhdGUg\nImh0dHA6Ly9leGFtcGxlLmNvbS97XCJOb21icmVcIn0iIF07IAoJCiAgICBy\ncjpwcmVkaWNhdGVPYmplY3RNYXAKICAgIFsgCiAgICAgIHJyOnByZWRpY2F0\nZQkJZm9hZjpuYW1lIDsgCiAgICAgIHJyOm9iamVjdE1hcAkJWyBycjpjb2x1\nbW4gIlwiTm9tYnJlXCIiIF0KICAgIF0KICAgIC4=\n";
  val cleanedEncodedString = encodedString.replaceAllLiterally("\n", "");
  println("cleanedEncodedString = " + cleanedEncodedString);
  val decodedString = this.decodeFromBase64(cleanedEncodedString);
  println("decodedString = " + decodedString);
  */

  //val logger : Logger = LogManager.getLogger("GitHubUtility");
  val logger: Logger = LoggerFactory.getLogger(this.getClass);





  def encodeToBase64(content:String) : String = {
    val bytes = content.getBytes(Charsets.UTF_8);
    val base64EncodedContent = BaseEncoding.base64().encode(bytes);
    base64EncodedContent;
  }

  def encodeToBase64(file:File) : String = {
    val in = new FileInputStream(file)
    val bytes = new Array[Byte](file.length.toInt)
    in.read(bytes)
    in.close();
    val base64EncodedContent = BaseEncoding.base64().encode(bytes);
    base64EncodedContent;
  }

  def decodeFromBase64(encodedContent:String) : String = {
    val cleanedEncodedString = encodedContent.replaceAllLiterally("\n", "");
    val base64DecodedContent = BaseEncoding.base64().decode(cleanedEncodedString);
    val decodedContentInString = new String(base64DecodedContent, Charsets.UTF_8);
    decodedContentInString;
  }



  def generateDownloadURL(organizationId:String, datasetId:String, fileName:String) = {
    val githubRepository = MappingPediaEngine.mappingpediaProperties.githubRepository;

    val downloadURL:String = s"${MappingPediaConstant.GITHUB_RAW_URL_PREFIX}$githubRepository/master/$organizationId/$datasetId/$fileName";
    logger.info(s"downloadURL = " + downloadURL);
    downloadURL
  }


}
