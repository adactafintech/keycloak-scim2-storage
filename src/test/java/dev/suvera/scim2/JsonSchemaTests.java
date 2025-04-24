package dev.suvera.scim2;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

@RunWith(Arquillian.class)
public class JsonSchemaTests {
    public static String SCHEMA = "{ }";

    /*
     * com.github.java-json-tools library is the only place that is using com.sun.mail and javax.mail namespace.
     * com.sun.mail was creatin chaos in keycloak mail functionality, so we removed the dependency to com.sun.mail
     * and added our own javax.mail classes that are derived from jakarta.mail.
     * This test checks if the JsonSchema functionality still works.
     */
    @Test
    public void getJsonSchema_ShouldNotThrow_WhenDeseserializingJsonSchema() throws IOException, ProcessingException {
        JsonNode sNode = JsonLoader.fromString(SCHEMA);
        JsonSchema schema = JsonSchemaFactory.byDefault().getJsonSchema(sNode);
        assertNotNull(schema);
    }
}
