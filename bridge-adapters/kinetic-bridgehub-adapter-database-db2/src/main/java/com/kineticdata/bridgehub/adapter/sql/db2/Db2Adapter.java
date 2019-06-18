package com.kineticdata.bridgehub.adapter.sql.db2;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.sql.SqlAdapter;
import com.kineticdata.bridgehub.adapter.sql.SqlQualification;
import com.kineticdata.bridgehub.adapter.sql.SqlQualificationParameter;
import com.kineticdata.bridgehub.adapter.sql.SqlQualificationParser;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

public class Db2Adapter extends SqlAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name. */
    public static final String NAME = "DB2 Bridge";
    
    /** Specify the adapter class and ensure it is loaded **/
    public static final String ADAPTER_CLASS = "com.ibm.db2.jcc.DB2Driver";

    /** Defines the logger */
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Db2Adapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(Db2Adapter.class.getResourceAsStream("/"+Db2Adapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+Db2Adapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    /** Defines the collection of property names for the adapter. */
    public static class Properties {
        public static final String USERNAME = "Username";
        public static final String PASSWORD = "Password";
        public static final String SERVER = "Server";
        public static final String PORT = "Port";
        public static final String DATABASE_NAME = "Database Name";
    }

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
            new ConfigurableProperty(Properties.USERNAME).setIsRequired(true),
            new ConfigurableProperty(Properties.PASSWORD).setIsRequired(true).setIsSensitive(true),
            new ConfigurableProperty(Properties.SERVER).setIsRequired(true).setValue("127.0.0.1"),
            new ConfigurableProperty(Properties.PORT).setIsRequired(true).setValue("50000"),
            new ConfigurableProperty(Properties.DATABASE_NAME).setIsRequired(true)
    );

    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public String getVersion() {
       return VERSION;
    }
    
    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }
    
    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public void initialize() throws BridgeError {
        super.initalize (
            ADAPTER_CLASS,
            "jdbc:db2://"+
                properties.getValue(Properties.SERVER)+":"+
                properties.getValue(Properties.PORT)+"/"+
                properties.getValue(Properties.DATABASE_NAME),
            properties.getValue(Properties.USERNAME),
            properties.getValue(Properties.PASSWORD)
        );
    }

    @Override
    protected PreparedStatement buildPaginatedStatement(
        Connection connection,
        BridgeRequest request,
        Long offset,
        Long pageSize
    ) throws BridgeError, SQLException {
        // Build the list of columns to retrieve from the field string
        String columns = request.getFieldString();
        // If the field string was not provided, default it to all columns
        if (StringUtils.isBlank(columns)) {columns = "*";}

        // Build up the SQL WHERE clause
        SqlQualification qualification = SqlQualificationParser.parse(request.getQuery());
        // Build the SQL ORDER BY clause (validating that only the requested
        // fields are used in the column list and that there is no attempt at
        // injection).
        String order = null;
        if (StringUtils.isNotBlank(request.getMetadata("order"))) {
            order = SqlQualificationParser.buildOrderByClause(request.getFields(), request.getMetadata("order"));
        } else if(!"*".equals(columns)) {
            order = SqlQualificationParser.buildOrderByClause(request.getFields(), columns);
        }

        // Build the main statement string
        StringBuilder statementString = new StringBuilder();

        // DB2 Does not support the OFFSET and LIMIT keyworsds, but a similar
        // result can be accomplished by wrapping a query in another
        // "pagination" query.
        //   * offset means how many row s to skip
        //   * 0 implies show the first row.
        //   * If pageSize is zero, don't use pagination.
        if (offset >= 0 && pageSize > 0 && !"*".equals(columns)) {
            offset = (offset>0?++offset : offset);
            pageSize = (offset>0? offset-1+pageSize: pageSize);

            statementString.append("SELECT ").append(columns);
            statementString.append(" FROM ");
            statementString.append("(SELECT row_number()");
            // Use the metadata order if it is available
            if (order != null) {
                statementString.append(" OVER (ORDER BY ").append(order).append(")");
            }
            statementString.append(" AS rid,").append(columns);
            statementString.append(" FROM ").append(request.getStructure());
            statementString.append(" WHERE ").append(qualification.getParameterizedString());
            statementString.append(" ) AS t");
            statementString.append(" WHERE t.rid BETWEEN ").append(offset).append(" AND ").append(pageSize);
        } else {
            if (offset >= 0 && pageSize > 0){
                logger.warn("Unable to apply offset and pageSize when "+
                    "retrieve * columns, defaulting to returning all rows");
            }
            // Build a request without pagination
            statementString.append("SELECT ").append(columns);
            statementString.append(" FROM ").append(request.getStructure());
            statementString.append(" WHERE ").append(qualification.getParameterizedString());
            if (order != null) {
                statementString.append(" ORDER BY ").append(order);
            }
        }

        // Prepare the statement
        logger.debug("Preparing Query");
        logger.debug("  "+statementString);
        PreparedStatement statement = connection.prepareStatement(statementString.toString());
        for (SqlQualificationParameter parameter : qualification.getParameters()) {
            // Retrieve the parameter value
            String parameterValue = request.getParameter(parameter.getName());
            // If there is a reference to a parameter that was not passed
            if (parameterValue == null) {
                throw new BridgeError("Unable to parse qualification, "+
                    "the '"+parameter.getName()+"' parameter was "+
                    "referenced but not provided.");
            }
            
            // Log each parameter value in the query
            logger.trace("  "+ Integer.toString(parameter.getIndex()+1) + " (" +parameter.getName()+") : "+parameterValue);
            
            // Set the value for the parameter in the SQL statement.
            statement.setObject(parameter.getIndex(), parameterValue);
        }

        // Return the statement
        return statement;
    }
}