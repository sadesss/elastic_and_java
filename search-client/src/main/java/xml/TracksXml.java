package xml;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JacksonXmlRootElement(localName = "tracks")
public class TracksXml {

    @JacksonXmlProperty(localName = "track")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<TrackXml> tracks;

    public static class TrackXml {
        public String id;

        public String title;

        @JacksonXmlProperty(localName = "artistId")
        public String artistId;

        @JacksonXmlProperty(localName = "artistName")
        public String artistName;

        @JacksonXmlProperty(localName = "albumTitle")
        public String albumTitle;

        @JacksonXmlElementWrapper(localName = "tags")
        @JacksonXmlProperty(localName = "tag")
        public List<String> tags;

        public String genre;
        public Integer year;

        @JacksonXmlProperty(localName = "durationSec")
        public Integer durationSec;

        public Integer popularity;

        @JacksonXmlProperty(localName = "createdAt")
        public String createdAt;
    }
}
