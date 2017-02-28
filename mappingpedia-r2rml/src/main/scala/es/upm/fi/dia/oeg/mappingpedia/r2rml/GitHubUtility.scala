package es.upm.fi.dia.oeg.mappingpedia.r2rml

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.mashape.unirest.http.Unirest
import org.apache.logging.log4j.{LogManager, Logger}
import org.json.JSONObject
import java.io.FileInputStream
import java.io.File

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
  
  val logger : Logger = LogManager.getLogger("GitHubUtility");

  def getFile(githubUsername:String, githubAccessToken:String
              , mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String) = {
    //val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/mappingpedia-testuser/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c.ttl";
    //val response = Unirest.get(uri).asJson();

    val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
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

  def putEncodedFile(mappingDirectory:String, mappingFilename:String, message:String, base64EncodedContent:String
    , githubUsername:String, githubAccessToken:String, mappingpediaUsername:String
    //, sha:Option[String]
  ) = {
    /*
    logger.info("content= " + base64EncodedContent);
    logger.info("message = " + message);
    logger.info("sha = " + sha);
    logger.info("mappingpediaUsername= " + mappingpediaUsername);
    logger.info("uuid = " + uuid);
    logger.info("filename = " + filename);
    */

    val jsonObj = new JSONObject();
    jsonObj.put("message", message);
    jsonObj.put("content", base64EncodedContent);

    try {
      val sha = GitHubUtility.getSHA(mappingpediaUsername, mappingDirectory, mappingFilename
        , Application.prop.githubUser, Application.prop.githubAccessToken);
      jsonObj.put("sha", sha);
    } catch {
      case e:Exception => {}
    }

    //if(sha!= null && sha.isDefined) { jsonObj.put("sha", sha.get); }
    //jsonObj.put("path", filename);
    //jsonObj.put("access_token", "7dabca6745186161c07d04353b6967249095b679");

    //val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/mappingpedia-testuser/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c.ttl";
    val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val response = Unirest.put(uri)
      .routeParam("mappingpediaUsername", mappingpediaUsername)
      .routeParam("mappingDirectory", mappingDirectory)
      .routeParam("mappingFilename", mappingFilename)
      .basicAuth(githubUsername, githubAccessToken)
      //.header("Content-Type", "application/json")
      .body(jsonObj)
      .asJson();
    //logger.info("responseHeaders = " + response.getHeaders);
    //logger.info("responseBody = " + response.getBody);
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
    val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
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
}
