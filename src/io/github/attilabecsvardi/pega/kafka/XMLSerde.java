package io.github.attilabecsvardi.pega.kafka;

import com.pega.pegarules.pub.PRRuntimeException;
import com.pega.pegarules.pub.clipboard.ClipboardPage;
import com.pega.pegarules.pub.runtime.PublicAPI;
import com.pega.pegarules.pub.util.HashStringMap;
import com.pega.platform.kafka.serde.PegaSerde;

import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;


public class XMLSerde implements PegaSerde {

    private String className = null;
    private String XMLParseRuleName = null;
    private String XMLParseRootElementName = null;
    private String XMLStreamRuleName = null;
    private String XMLStreamXMLType = null;

    public XMLSerde() {
    }

    /**
     * Configure this class, which will configure the underlying serializer and deserializer.
     *
     * @param tools   PublicAPI
     * @param configs configs in key/value pairs
     */
    @Override
    public void configure(PublicAPI tools, Map<String, ?> configs) {
        PegaSerde.super.configure(tools, configs);
        Preconditions.checkArgument(configs.containsKey("classname"), "ClassName is not configured");
        Preconditions.checkArgument(StringUtils.isNotBlank(configs.get("classname").toString()), "ClassName is not configured");
        this.className = configs.get("classname").toString();

        Preconditions.checkArgument(configs.containsKey("XMLParseRuleName"), "XMLParseRuleName is not configured");
        Preconditions.checkArgument(StringUtils.isNotBlank(configs.get("XMLParseRuleName").toString()), "XMLParseRuleName is not configured");
        this.XMLParseRuleName = configs.get("XMLParseRuleName").toString();

        Preconditions.checkArgument(configs.containsKey("XMLParseRootElementName"), "XMLParseRootElementName is not configured");
        Preconditions.checkArgument(StringUtils.isNotBlank(configs.get("XMLParseRootElementName").toString()), "XMLParseRootElementName is not configured");
        this.XMLParseRootElementName = configs.get("XMLParseRootElementName").toString();

        Preconditions.checkArgument(configs.containsKey("XMLStreamRuleName"), "XMLStreamRuleName is not configured");
        Preconditions.checkArgument(StringUtils.isNotBlank(configs.get("XMLStreamRuleName").toString()), "XMLStreamRuleName is not configured");
        this.XMLStreamRuleName = configs.get("XMLStreamRuleName").toString();

        Preconditions.checkArgument(configs.containsKey("XMLStreamXMLType"), "XMLStreamXMLType is not configured");
        Preconditions.checkArgument(StringUtils.isNotBlank(configs.get("XMLStreamXMLType").toString()), "XMLStreamXMLType is not configured");
        this.XMLStreamXMLType = configs.get("XMLStreamXMLType").toString();
    }

    /**
     * Convert {@link com.pega.pegarules.pub.clipboard.ClipboardPage} into a byte array.
     *
     * @param tools         PublicAPI associated with the request
     * @param clipboardPage page to be serialized in to bytes
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(PublicAPI tools, ClipboardPage clipboardPage) {
        String str = "";

        if (tools != null && clipboardPage != null) {
            String streamClassName = clipboardPage.getStringIfPresent("pxObjClass");

            HashStringMap map = new HashStringMap();
            map.putString("pxObjClass", "Rule-Obj-XML");
            map.putString("pyClassName", streamClassName);
            map.putString("pyStreamName", this.XMLStreamRuleName);
            map.putString("pyXMLType", this.XMLStreamXMLType);
            // stream clipboard page with the passed Stream-XML rule
            str = tools.getStream(map, clipboardPage);

            // check the parse status
            if (tools.getStepStatus().getLatestSeverityText().startsWith("FAIL")) {
                throw new PRRuntimeException("Failed to stream clipboard page, error: " + tools.getStepStatus().getLatestMessage());
            }
        }

        return str.getBytes();
    }

    /**
     * Deserialize a byte array into a {@link com.pega.pegarules.pub.clipboard.ClipboardPage} object.
     *
     * @param tools PublicAPI associated with the request
     * @param data  serialized bytes
     * @return deserialized typed data
     */
    @Override
    public ClipboardPage deserialize(PublicAPI tools, byte[] data) {
        ClipboardPage page = tools.createPage(this.className, "");

        if (tools != null && data != null) {
            // parse the xml data with the passed Parse-XML rule
            tools.getParseUtils().parseXML(new String(data), this.XMLParseRuleName, this.XMLParseRootElementName, page);

            // check the parse status
            if (tools.getStepStatus().getLatestSeverityText().startsWith("FAIL")) {
                throw new PRRuntimeException("Failed to parse XML data, error: " + tools.getStepStatus().getLatestMessage());
            }
        }
        return page;
    }
}
