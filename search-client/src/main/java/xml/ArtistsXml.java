package xml;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JacksonXmlRootElement(localName = "artists")
public class ArtistsXml {

    @JacksonXmlProperty(localName = "artist")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<ArtistXml> artists;

    public static class ArtistXml {
        public String id;
        public String name;
        public String country;

        @JacksonXmlElementWrapper(localName = "tags")
        @JacksonXmlProperty(localName = "tag")
        public List<String> tags;

        public Integer popularity;

        @JacksonXmlProperty(localName = "createdAt")
        public String createdAt;
    }
}
