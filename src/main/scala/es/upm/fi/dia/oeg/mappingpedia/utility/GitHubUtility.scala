package es.upm.fi.dia.oeg.mappingpedia.utility

import java.io.{File, FileInputStream}

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine, MappingPediaProperties}
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by fpriyatna on 21/02/2017.
  */
class GitHubUtility {

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

  def getFile(githubUsername:String, githubAccessToken:String
              , mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String) = {
    //val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/mappingpedia-testuser/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c.ttl";
    //val response = Unirest.get(uri).asJson();

    //val uri = MappingPediaEngine.mappingpediaProperties.githubRepoContents + "/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val uri = MappingPediaConstant.GITHUB_ACCESS_URL_PREFIX + "/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val response = Unirest.get(uri)
      .routeParam("mappingpediaUsername", mappingpediaUsername)
      .routeParam("mappingDirectory", mappingDirectory)
      .routeParam("mappingFilename", mappingFilename)
      //.basicAuth(githubUsername, githubAccessToken)
      //.header("Content-Type", "application/json")
      //.body(jsonObj)
      .asJson();
    //logger.info("responseHeaders = " + response.getHeaders);
    //logger.info("responseBody = " + response.getBody);
    response;
  }

  def encodeAndPutFile(githubUsername:String, githubAccessToken:String
                        , mappingpediaUsername:String, directory:String, filename:String
                        , message:String, file:File
                       ) = {
    val base64EncodedContent = this.encodeToBase64(file);
    this.putEncodedContent(githubUsername, githubAccessToken
      , mappingpediaUsername, directory, filename
      , message, base64EncodedContent)
  }

  def putEncodedContent(githubUsername:String, githubAccessToken:String
                      , mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
                      , message:String, base64EncodedContent:String
  ) = {
    val jsonObj = new JSONObject();
    jsonObj.put("message", message);
    jsonObj.put("content", base64EncodedContent);

    try {
      val sha = GitHubUtility.getSHA(mappingpediaUsername, mappingDirectory, mappingFilename
        , githubUsername, githubAccessToken);
      jsonObj.put("sha", sha);
    } catch {
      case e:Exception => {}
    }

    //val uri = MappingPediaEngine.mappingpediaProperties.githubRepoContents + "/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val githubRepository = MappingPediaEngine.mappingpediaProperties.githubRepository;
    val uri = MappingPediaConstant.GITHUB_ACCESS_URL_PREFIX + s"${githubRepository}/contents/${mappingpediaUsername}/${mappingDirectory}/${mappingFilename}";
    logger.info(s"hitting github url $uri");
    val response = Unirest.put(uri)
      //.routeParam("githubRepository", githubRepository)
      //.routeParam("mappingpediaUsername", mappingpediaUsername)
      //.routeParam("mappingDirectory", mappingDirectory)
      //.routeParam("mappingFilename", mappingFilename)
      .basicAuth(githubUsername, githubAccessToken)
      .body(jsonObj)
      .asJson();
    response;
  }

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
  
  def getSHA(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
             , githubUsername:String, githubAccessToken:String) : String = {
    //val uri = MappingPediaEngine.mappingpediaProperties.githubRepoContents + "/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val uri = MappingPediaConstant + "/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val response = Unirest.get(uri)
      .routeParam("mappingpediaUsername", mappingpediaUsername)
      .routeParam("mappingDirectory", mappingDirectory)
      .routeParam("mappingFilename", mappingFilename)
      //.routeParam("mappingFileExtension", mappingFileExtension)
      .basicAuth(githubUsername, githubAccessToken)
      .asJson();

    response.getBody.getObject.getString("sha");
  }

  def getSHA(url:String, githubUsername:String, githubAccessToken:String) : String = {
    val response = Unirest.get(url)
      .basicAuth(githubUsername, githubAccessToken)
      .asJson();
    response.getBody.getObject.getString("sha");
  }

  def generateDownloadURL(organizationId:String, datasetId:String, fileName:String) = {
    val githubRepository = MappingPediaEngine.mappingpediaProperties.githubRepository;

    val downloadURL:String = s"${MappingPediaConstant.GITHUB_RAW_URL_PREFIX}/$githubRepository/master/$organizationId/$datasetId/$fileName";
    logger.info(s"downloadURL = " + downloadURL);
    downloadURL
  }

}
