package es.upm.fi.dia.oeg.mappingpedia.model;

import de.escalon.hypermedia.hydra.mapping.Expose;
import de.escalon.hypermedia.hydra.mapping.Term;

enum BusinessFunction {
    @Expose("gr:LeaseOut")
    RENT,
    @Expose("gr:Sell")
    FOR_SALE,
    @Expose("gr:Buy")
    BUY
}


@Term(define = "schema", as = "http://schema.org/")
@Expose("schema:Inbox")
public class Inbox {
    //public BusinessFunction businessFunction = BusinessFunction.RENT;

    @Expose("schema:location")
    public String getInbox() {
        return "http://mappingpedia.linkeddata.es/engine/inbox/";
    }
}
