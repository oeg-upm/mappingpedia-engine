package es.upm.fi.dia.oeg.mappingpedia.utility

import java.io._
import java.nio.channels.FileChannel
import java.security.MessageDigest
import java.util.UUID

import es.upm.fi.dia.oeg.mappingpedia.model.result.ListResult
import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution, OntologyClass}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine, MappingPediaProperties}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.query.QuerySolution
import org.apache.jena.rdf.model._
import org.apache.jena.util.FileManager
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

import scala.collection.JavaConversions._
import org.apache.commons.io.FileUtils
import org.apache.jena.ontology.OntClass

import scala.io.Source

/**
  * Created by freddy on 10/08/2017.
  */
object MappingPediaUtility {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);






  /*
  def getVirtuosoGraph(virtuosoJDBC : String, virtuosoUser : String, virtuosoPwd : String
                       , virtuosoGraphName : String) : VirtGraph = {
    logger.info("Connecting to Virtuoso Graph...");
    logger.info(s"virtuosoGraphName = $virtuosoGraphName");

    val virtGraph : VirtGraph = new VirtGraph (
      virtuosoGraphName, virtuosoJDBC, virtuosoUser, virtuosoPwd);
    logger.info("Connected to Virtuoso Graph...");
    return virtGraph;
  }
  */

  def getFirstPropertyObjectValueLiteral(resource:Resource, property:Property): Literal = {
    val it = resource.listProperties(property);
    var result: Literal = null;
    if(it != null && it.hasNext()) {
      val statement = it.next();
      val objectNode = statement.getObject();
      result = objectNode.asLiteral()
    }
    return result;
  }

  def collectBlankNodes(triples:List[Triple]) : Set[Node] = {
    val blankNodes:Set[Node] = if(triples.isEmpty) {
      Set.empty;
    } else {
      val blankNodesHead:Set[Node] = this.collectBlankNode(triples.head);
      val blankNodesTail:Set[Node] = this.collectBlankNodes(triples.tail);

      blankNodesHead ++ blankNodesTail;
    }

    blankNodes;
  }



  def collectBlankNode(tp:Triple) : Set[Node] = {
    val tpSubject = tp.getSubject;
    val tpObject = tp.getObject;

    var blankNodes:Set[Node] = Set.empty;

    if(tpSubject.isBlank()) {
      blankNodes = blankNodes + tpSubject;
    }

    if(tpObject.isBlank()) {
      blankNodes = blankNodes + tpObject;
    }

    blankNodes;
  }

  def skolemizeTriples(triples:List[Triple], baseURI:String) : List[Triple] = {
    val blankNodes = this.collectBlankNodes(triples);
    val mapNewNodes = this.skolemizeBlankNodes(blankNodes, baseURI);
    val newTriples = this.replaceBlankNodesInTriples(triples, mapNewNodes);
    newTriples;
  }

  def replaceBlankNodesInTriples(triples:List[Triple], mapNewNodes:Map[Node, Node]) : List[Triple] = {
    val newTriples = triples.map(x => {this.replaceBlankNodesInTriple(x, mapNewNodes)});
    newTriples;
  }

  def replaceBlankNodesInTriple(tp:Triple, mapNewNodes:Map[Node, Node]) : Triple = {
    val tpSubject = tp.getSubject;
    val tpObject = tp.getObject;

    val tpSubjectNew:Node = if(tpSubject.isBlank()) { mapNewNodes.get(tpSubject).get } else { tpSubject; }
    val tpObjectNew:Node = if(tpObject.isBlank()) { mapNewNodes.get(tpObject).get } else { tpObject; }

    val newTriple = new Triple(tpSubjectNew, tp.getPredicate, tpObjectNew);
    newTriple;
  }

  def skolemizeBlankNodes(blankNodes:Set[Node], baseURI:String) : Map[Node, Node] = {
    val mapNewNodes = blankNodes.map(x => {(x -> this.skolemizeBlankNode(x, baseURI))}).toMap;
    mapNewNodes;
  }

  def skolemizeBlankNode(blankNode:Node, baseURI:String) : Node = {
    //val absoluteBaseURI = if(baseURI.endsWith("/")) { baseURI } else { baseURI + "/" }

    val newNodeURI = baseURI + ".well-known/genid/" + blankNode.getBlankNodeLabel;
    val newNode = NodeFactory.createURI(newNodeURI);
    newNode;
  }

  def toTriples(model:Model) : List[Triple] = {
    val statements = model.listStatements();
    //val statementList = statements.toList();
    var triples:List[Triple] = List.empty;
    if(statements != null) {
      while(statements.hasNext()) {
        val statement = statements.nextStatement();
        val triple = statement.asTriple();
        triples = triples ::: List(triple);
      }
    }
    triples;
  }

  def replaceBaseURI(lines:Iterator[String], pNewBaseURI:String) : Iterator[String] = {
    var newBaseURI = pNewBaseURI;
    if(!pNewBaseURI.startsWith("<")) {
      newBaseURI = "<" + newBaseURI;
    }
    if(!pNewBaseURI.endsWith(">")) {
      newBaseURI = newBaseURI + ">";
    }

    val newLines = lines.map(oneLine => {
      val line = oneLine.trim;

      if(line.startsWith("@base")) {
        "@base " + newBaseURI + " . ";
      } else { line }
    }

    )
    newLines;
  }

  @throws(classOf[IOException])
  def materializeFileInputStream(source: FileInputStream, dest: File) {
    var sourceChannel: FileChannel = null
    var destChannel: FileChannel = null
    val fos: FileOutputStream = new FileOutputStream(dest)
    try {
      sourceChannel = source.getChannel
      destChannel = fos.getChannel
      destChannel.transferFrom(sourceChannel, 0, sourceChannel.size)
    } finally {
      sourceChannel.close
      destChannel.close
      fos.close
    }
  }

  @throws(classOf[IOException])
  def materializeFileInputStream(source: FileInputStream, uuidDirectoryName: String, fileName: String): File = {
    val uploadDirectoryPath: String = MappingPediaConstant.DEFAULT_UPLOAD_DIRECTORY;
    val outputDirectory: File = new File(uploadDirectoryPath)
    if (!outputDirectory.exists) {
      outputDirectory.mkdirs
    }
    val uuidDirectoryPath: String = uploadDirectoryPath + "/" + uuidDirectoryName
    //logger.info("upload directory path = " + uuidDirectoryPath)
    val uuidDirectory: File = new File(uuidDirectoryPath)
    if (!uuidDirectory.exists) {
      uuidDirectory.mkdirs
    }

    val uploadedFilePath: String = uuidDirectory + "/" + fileName
    val dest: File = new File(uploadedFilePath)
    dest.createNewFile
    this.materializeFileInputStream(source, dest)
    return dest
  }

  def pushContentToGitHub(file:File) = {

  }

  def multipartFileToFile(fileRef:MultipartFile) : File = {
    // Path where the uploaded files will be stored.
    val uuid = UUID.randomUUID().toString();

    val file = this.multipartFileToFile(fileRef, uuid);
    file;
  }

  def multipartFileToFile(fileRef:MultipartFile, uuid:String) : File = {

    // Create the input stream to uploaded files to read data from it.
    val fis:FileInputStream = try {
      if(fileRef != null) {
        val inputStreamAux = fileRef.getInputStream().asInstanceOf[FileInputStream];
        inputStreamAux;
      } else {
        val errorMessage = "can't process the uploaded file, fileRef is null";
        throw new Exception(errorMessage);
      }
    } catch {
      case e:Exception => {
        e.printStackTrace();
        throw e;
      }
    }

    // Get the name of uploaded files.
    val fileName = fileRef.getOriginalFilename();

    val file = MappingPediaUtility.materializeFileInputStream(fis, uuid, fileName);
    file;
  }



  def readFromResourcesDirectory(filePath:String) : String = {
    //var lines: String = Source.fromResource(templateFilePath).getLines.mkString("\n");
    val is: InputStream = getClass.getResourceAsStream("/" + filePath)
    val lines= scala.io.Source.fromInputStream(is).getLines.mkString("\n");
    lines;

  }

  def getStringOrElse(qs:QuerySolution, varName:String, obj:String) : String = {
    val result = qs.get(varName);
    if(result == null) {
      obj
    } else {
      result.toString;
    }

  }

  def getOptionString(qs:QuerySolution, varName:String) : Option[String] = {
    val result = qs.get(varName);
    if(result == null) {
      None
    } else {
      Some(result.toString);
    }

  }


  def getClassesLocalNames(listOfClasses:List[OntClass]) : List[String] = {
    val result = listOfClasses.map(ontClass => ontClass.getLocalName);
    result;
  }

  def getClassesURIs(listOfClasses:List[OntClass]) : List[String] = {
    val result = listOfClasses.map(ontClass => ontClass.getURI);
    result;
  }

  def calculateHash(dataset:Dataset) : String = {
    val distributions = dataset.getDistributions;
    logger.info(s"{distributions.length = ${distributions.length}")

    this.calculateHash(distributions);
  }

  def calculateHash(distributions:List[Distribution]) : String = {
  val datasetHashValue = distributions.foldLeft(0)((acc, distribution) => {
      val distributionHashValue = MappingPediaUtility.calculateHash(
        distribution.dcatDownloadURL, distribution.encoding);
      logger.info(s"distributionHashValue = ${distributionHashValue}")
      acc + distributionHashValue.toInt
    })

    logger.debug(s"datasetHashValue = ${datasetHashValue}")
    datasetHashValue.toString
  }


  def calculateHash(downloadURL:String) : String = {
    this.calculateHash(downloadURL, null);
  }

  def calculateHash(downloadURL:String, pEncoding:String) : String = {
    logger.debug(s"calculating hash value of ${downloadURL}");
    val encoding = if(pEncoding == null) { "UTF-8" } else { pEncoding }

    val hashValue = try {
      if (downloadURL != null) {
        val downloadURLContent = scala.io.Source.fromURL(downloadURL, encoding).mkString
        //logger.info(s"downloadURLContent ${downloadURLContent}");

        //val downloadURLContentBase64 = GitHubUtility.encodeToBase64(downloadURLContent);
        //logger.info(s"downloadURLContentBase64 ${downloadURLContentBase64}");

        //MessageDigest.getInstance("SHA").digest(downloadURLContentBase64.getBytes).toString

        downloadURLContent.hashCode.toString;

      } else {
        null
      }
    } catch {
      case e:Exception => {
        e.printStackTrace()
        null
      }
    }

    logger.debug(s"hash value of of ${downloadURL} = ${hashValue}");
    hashValue
  }


  def getFileNameAndContent(file: File, downloadURL:String, encoding:String) = {

    val (fileName:String, fileContent:String) = {
      if(file != null) {
        val fileAbsolutePath = file.getAbsolutePath;
        logger.info(s"fileAbsolutePath = $fileAbsolutePath");

        val fileContent = Source.fromFile(file.getAbsolutePath, encoding)
        logger.info(s"encoding = $encoding");

        val fileContentLines = fileContent.getLines;

        val fileContentString = fileContentLines.mkString("\n")
        (file.getName, fileContentString)
      } else if(downloadURL!= null) {
        val downloadURLFilename = downloadURL.substring(
          downloadURL.lastIndexOf('/') + 1, downloadURL.length)
        val downloadURLContent = scala.io.Source.fromURL(downloadURL, encoding)
        val downloadURLContentString = downloadURLContent.mkString
        (downloadURLFilename, downloadURLContentString);
      } else {
        val errorMessage = "No file or download url has been provided"
        logger.info(errorMessage);
        throw new Exception(errorMessage);
      }
    }
    (fileName:String, fileContent:String);
  }

  def normalizeTerm(originalTerm:String) : List[String] = {
    if(originalTerm != null) {
      val normalizedTermSingular = originalTerm.toLowerCase.replaceAll(" ", "");
      val normalizedTermPlural1 = normalizedTermSingular + "s"
      val normalizedTermPlural2 = normalizedTermSingular + "es"
      val normalizedTermPlural3 = if(normalizedTermSingular.endsWith("y")) {
        val plural = normalizedTermSingular.substring(0, normalizedTermSingular.length-1) + "ies"
        //logger.info(s"$normalizedTermSingular  -- $plural")
        plural
      } else { normalizedTermSingular }

      List(normalizedTermSingular, normalizedTermPlural1, normalizedTermPlural2, normalizedTermPlural3)
    } else {
      Nil
    }
  }

  def getClassURI(pClass:String) : String  = {
    this.getClassURI(pClass, "http://schema.org/")
  }

  def getClassURI(pClass:String, defaultNamespace:String) : String = {
    val isLocalName = if(pClass.contains("/")) { false } else { true }
    val classIRI = if(isLocalName) {
      if(defaultNamespace.endsWith("/")) {
        defaultNamespace + pClass
      } else {
        defaultNamespace + "/" + pClass
      }
    } else {
      pClass
    }
    classIRI
  }

  /*  def stringToBoolean(aString:String) : Boolean = {
    if(aString != null) {
      if(aString.equalsIgnoreCase("true") || aString.equalsIgnoreCase("yes")) {
        true;
      } else {
        false
      }
    } else {
      false
    }
  }*/

  def stringToBoolean(inputString:String) : Boolean = {
    try {
      val listOfTrueString = List("true", "yes");

      val result = if(inputString == null) {
        false
      } else {
        if(listOfTrueString.contains(inputString)) {
          true
        } else {
          false
        }
      }

      result
    } catch {
      case e:Exception => {
        e.printStackTrace()
        false
      }
    }

  }


}
