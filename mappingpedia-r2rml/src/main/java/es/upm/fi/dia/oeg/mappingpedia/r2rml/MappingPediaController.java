package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import es.upm.fi.dia.oeg.mappingpedia.storage.StorageFileNotFoundException;
import es.upm.fi.dia.oeg.mappingpedia.storage.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import es.upm.fi.dia.oeg.mappingpedia.storage.StorageService;

import java.io.IOException;
import java.util.stream.Collectors;

@RestController
public class MappingPediaController {
	static Logger logger = LogManager.getLogger("MappingPediaController");
	
    private static final String template = "Hello, %s!";
    private static final String template2 = "%s!";
    private final AtomicLong counter = new AtomicLong();

    
    
    
    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
    	logger.info("/greeting ...");
        return new Greeting(counter.incrementAndGet(),
                            String.format(template, name));
    }
    
    @RequestMapping("/uploadMapping")
    public MappingPediaExecutionResult uploadMapping(@RequestParam(value="manifestText", defaultValue="ManifestTest") String manifestText, @RequestParam(value="mappingText", defaultValue="MappingTest") String mappingText) {
    	logger.info("/uploadMapping ...");
    	
    	logger.info("Application.mappingpediaR2RML = " + Application.mappingpediaR2RML);
    	//String message = null;
    	String status=null;
    	try {
    		Application.mappingpediaR2RML.insertMappingInString(manifestText, mappingText);
    		status="success";
    		//message = "manifestText = " + manifestText + ", mappingText= " + mappingText + ", status = " + status;
    		logger.info("mapping inserted.");
    	} catch (Exception e){
    		e.printStackTrace();
    		status="failed";
    		//message = "manifestText = " + manifestText + ", mappingText= " + mappingText + ", status = " + status;
    	}
    	
    	MappingPediaExecutionResult executionResult = new MappingPediaExecutionResult(manifestText, mappingText, status);
        return executionResult;
    }

}
