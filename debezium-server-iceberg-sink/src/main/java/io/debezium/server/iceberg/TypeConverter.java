package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Handles type promotion and conversion for Iceberg compatibility.
 * Follows Iceberg data type evolution rules:
 * 1. int can promote to long
 * 2. float can promote to double
 * 3. decimal(P,S) has fixed scale and precision must be 38 or less
 * 
 * Also handles reverse conversions (like float to int) if no data loss occurs.
 */
public class TypeConverter {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TypeConverter.class);
    private static final int MAX_DECIMAL_PRECISION = 38;

    /**
     * Converts JSON value to appropriate Iceberg value based on expected type.
     * Handles type promotion and conversion according to Iceberg compatibility rules.
     *
     * @param field The Iceberg field with expected type information
     * @param node The JSON node containing the actual value
     * @return Object value converted to appropriate type, or null if conversion not possible
     * @throws RuntimeException if value cannot be converted to expected type or promoted
     */
    public static Object convertValueWithTypeHandling(Types.NestedField field, JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        Type.TypeID expectedTypeId = field.type().typeId();
        
        try {
            // First try direct conversion
            return directConvert(expectedTypeId, node, field.type());
        } catch (Exception e) {
            LOGGER.debug("Direct conversion failed for field '{}' with value '{}', trying type promotion: {}", 
                    field.name(), node, e.getMessage());
            
            // Try type promotion if direct conversion fails
            try {
                return tryTypePromotion(expectedTypeId, node, field.name(), field.type());
            } catch (Exception promoException) {
                // If promotion fails, try safe type demotion
                try {
                    Object demotedValue = tryTypeDemotion(expectedTypeId, node, field.name());
                    if (demotedValue != null) {
                        LOGGER.info("Successfully demoted {} value '{}' to {} for field '{}'", 
                                getNodeType(node), node, expectedTypeId, field.name());
                        return demotedValue;
                    }
                } catch (Exception demotionException) {
                    // Log but re-throw the original promotion exception since that's the primary path
                    LOGGER.debug("Type demotion also failed: {}", demotionException.getMessage());
                }
                
                // If we couldn't demote either, throw the promotion exception
                throw promoException;
            }
        }
    }
    
    /**
     * Gets a string representation of the JsonNode's type
     */
    private static String getNodeType(JsonNode node) {
        if (node.isInt()) return "INT";
        if (node.isLong()) return "LONG";
        if (node.isFloat()) return "FLOAT";
        if (node.isDouble()) return "DOUBLE";
        if (node.isBoolean()) return "BOOLEAN";
        if (node.isTextual()) return "STRING";
        return node.getNodeType().toString();
    }
    
    /**
     * Attempts direct conversion to the expected type
     */
    private static Object directConvert(Type.TypeID expectedTypeId, JsonNode node, Type fieldType) {
        switch (expectedTypeId) {
            case INTEGER:
                if (node.isInt()) {
                    return node.asInt();
                } else if (node.isTextual()) {
                    // Try parsing string as int
                    return Integer.parseInt(node.asText());
                }
                break;
                
            case LONG:
                if (node.isLong() || node.isInt()) {
                    return node.asLong();
                } else if (node.isTextual()) {
                    // Try parsing string as long
                    return Long.parseLong(node.asText());
                }
                break;
                
            case FLOAT:
                if (node.isFloat() || node.isInt() || node.isLong()) {
                    return node.floatValue();
                } else if (node.isTextual()) {
                    // Try parsing string as float
                    return Float.parseFloat(node.asText());
                }
                break;
                
            case DOUBLE:
                if (node.isDouble() || node.isFloat() || node.isInt() || node.isLong()) {
                    return node.asDouble();
                } else if (node.isTextual()) {
                    // Try parsing string as double
                    return Double.parseDouble(node.asText());
                }
                break;
                
            case BOOLEAN:
                if (node.isBoolean()) {
                    return node.asBoolean();
                } else if (node.isTextual()) {
                    String text = node.asText().toLowerCase();
                    if (text.equals("true") || text.equals("false")) {
                        return Boolean.parseBoolean(text);
                    } else if (text.equals("1") || text.equals("0")) {
                        return text.equals("1");
                    }
                } else if (node.isNumber()) {
                    // Treat 1 as true, 0 as false
                    int value = node.asInt();
                    if (value == 1 || value == 0) {
                        return value == 1;
                    }
                }
                break;
                
            case DECIMAL:
                if (node.isNumber() || node.isTextual()) {
                    BigDecimal decimal;
                    if (node.isTextual()) {
                        decimal = new BigDecimal(node.asText());
                    } else {
                        decimal = node.decimalValue();
                    }
                    
                    Types.DecimalType decimalType = (Types.DecimalType) fieldType;
                    int scale = decimalType.scale();
                    
                    // Ensure the decimal value has the correct scale
                    return decimal.setScale(scale, RoundingMode.HALF_UP);
                }
                break;
                
            case STRING:
                // Any type can be converted to string
                return node.isValueNode() ? node.asText() : node.toString();
                
            default:
                // For other types, return null to let the original converter handle it
                return null;
        }
        
        // If we get here, direct conversion failed
        throw new RuntimeException("Cannot directly convert " + node + " to " + expectedTypeId);
    }
    
    /**
     * Tries to promote the type following Iceberg evolution rules
     */
    private static Object tryTypePromotion(Type.TypeID expectedTypeId, JsonNode node, String fieldName, Type fieldType) {
        switch (expectedTypeId) {
            case LONG:
                // Try to promote from int to long
                if (node.isInt()) {
                    LOGGER.info("Promoting int to long for field '{}'", fieldName);
                    return (long) node.asInt();
                } else if (node.isTextual() && canParseAs(node.asText(), Type.TypeID.INTEGER)) {
                    LOGGER.info("Promoting string->int->long for field '{}'", fieldName);
                    return (long) Integer.parseInt(node.asText());
                }
                break;
                
            case DOUBLE:
                // Try to promote from float to double
                if (node.isFloat()) {
                    LOGGER.info("Promoting float to double for field '{}'", fieldName);
                    return (double) node.floatValue();
                } else if (node.isInt()) {
                    LOGGER.info("Promoting int to double for field '{}'", fieldName);
                    return (double) node.asInt();
                } else if (node.isLong()) {
                    LOGGER.info("Promoting long to double for field '{}'", fieldName);
                    return (double) node.asLong();
                } else if (node.isTextual()) {
                    // Try parsing as float first, then promote to double
                    if (canParseAs(node.asText(), Type.TypeID.FLOAT)) {
                        LOGGER.info("Promoting string->float->double for field '{}'", fieldName);
                        return (double) Float.parseFloat(node.asText());
                    }
                }
                break;
                
            case DECIMAL:
                if (node.isNumber() || node.isTextual()) {
                    try {
                        Types.DecimalType decimalType = (Types.DecimalType) fieldType;
                        int scale = decimalType.scale();
                        int precision = decimalType.precision();
                        
                        // Validate precision is within Iceberg limits
                        if (precision > MAX_DECIMAL_PRECISION) {
                            throw new RuntimeException("Decimal precision exceeds maximum allowed (" + 
                                    MAX_DECIMAL_PRECISION + "): " + precision);
                        }
                        
                        BigDecimal decimal;
                        if (node.isTextual()) {
                            decimal = new BigDecimal(node.asText());
                        } else if (node.isInt()) {
                            decimal = BigDecimal.valueOf(node.asInt());
                        } else if (node.isLong()) {
                            decimal = BigDecimal.valueOf(node.asLong());
                        } else if (node.isFloat() || node.isDouble()) {
                            decimal = BigDecimal.valueOf(node.asDouble());
                        } else {
                            decimal = node.decimalValue();
                        }
                        
                        // Set the scale to match the expected decimal type
                        decimal = decimal.setScale(scale, RoundingMode.HALF_UP);
                        
                        // Check if the precision fits within the expected decimal type
                        if (decimal.precision() > precision) {
                            LOGGER.warn("Decimal value '{}' exceeds precision {} for field '{}'", 
                                    decimal, precision, fieldName);
                            // We could decide to truncate here, but it's safer to throw an exception
                            throw new RuntimeException("Decimal value exceeds precision limit");
                        }
                        
                        return decimal;
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Cannot convert value to decimal: " + e.getMessage(), e);
                    }
                }
                break;
                
            case STRING:
                // Any type can be promoted to string
                return node.isValueNode() ? node.asText() : node.toString();
        }
        
        // Try to see if we can convert through String for any numeric type
        if ((expectedTypeId == Type.TypeID.INTEGER || 
             expectedTypeId == Type.TypeID.LONG || 
             expectedTypeId == Type.TypeID.FLOAT || 
             expectedTypeId == Type.TypeID.DOUBLE) && 
            node.isTextual()) {
            
            String textValue = node.asText();
            try {
                switch (expectedTypeId) {
                    case INTEGER:
                        return Integer.parseInt(textValue);
                    case LONG:
                        return Long.parseLong(textValue);
                    case FLOAT:
                        return Float.parseFloat(textValue);
                    case DOUBLE:
                        return Double.parseDouble(textValue);
                    default:
                        break;
                }
            } catch (NumberFormatException e) {
                LOGGER.debug("Failed to parse '{}' as {}: {}", textValue, expectedTypeId, e.getMessage());
            }
        }
        
        // If we get here, type promotion failed
        throw new RuntimeException("Cannot promote value " + node + " to compatible type " + expectedTypeId + 
                " for field '" + fieldName + "'. Value type is incompatible with schema.");
    }
    
    /**
     * Tries to safely demote values (opposite of promotion), checking if it can be done without data loss.
     * Example: A float like 42.0 can be safely demoted to an int 42.
     * 
     * @param expectedTypeId The target type ID to convert to
     * @param node The value to convert
     * @param fieldName Field name for logging
     * @return Converted value if possible, or null if not
     */
    private static Object tryTypeDemotion(Type.TypeID expectedTypeId, JsonNode node, String fieldName) {
        switch (expectedTypeId) {
            case INTEGER:
                // Try to demote float/double to int if no fractional part
                if (node.isFloat()) {
                    float floatVal = node.floatValue();
                    if (canSafelyConvertToInt(floatVal)) {
                        LOGGER.info("Safely demoting float {} to int for field '{}'", floatVal, fieldName);
                        return (int) floatVal;
                    }
                } else if (node.isDouble()) {
                    double doubleVal = node.doubleValue();
                    if (canSafelyConvertToInt(doubleVal)) {
                        LOGGER.info("Safely demoting double {} to int for field '{}'", doubleVal, fieldName);
                        return (int) doubleVal;
                    }
                } else if (node.isLong()) {
                    long longVal = node.longValue();
                    if (canSafelyConvertToInt(longVal)) {
                        LOGGER.info("Safely demoting long {} to int for field '{}'", longVal, fieldName);
                        return (int) longVal;
                    }
                } else if (node.isTextual()) {
                    try {
                        String text = node.asText();
                        // Try to parse as float/double first and see if it can be an int
                        if (text.contains(".")) {
                            double parsed = Double.parseDouble(text);
                            if (canSafelyConvertToInt(parsed)) {
                                LOGGER.info("Safely demoting string '{}' to int for field '{}'", text, fieldName);
                                return (int) parsed;
                            }
                        }
                    } catch (NumberFormatException e) {
                        // Ignore, this just means we can't demote
                    }
                }
                break;
                
            case FLOAT:
                // Try to demote double to float if precision allows
                if (node.isDouble()) {
                    double doubleVal = node.doubleValue();
                    if (canSafelyConvertToFloat(doubleVal)) {
                        LOGGER.info("Safely demoting double {} to float for field '{}'", doubleVal, fieldName);
                        return (float) doubleVal;
                    }
                }
                break;
                
            case LONG:
                // Long has larger range than float/double so only convert if no fractional part
                if (node.isFloat()) {
                    float floatVal = node.floatValue();
                    if (canSafelyConvertToLong(floatVal)) {
                        LOGGER.info("Safely demoting float {} to long for field '{}'", floatVal, fieldName);
                        return (long) floatVal;
                    }
                } else if (node.isDouble()) {
                    double doubleVal = node.doubleValue();
                    if (canSafelyConvertToLong(doubleVal)) {
                        LOGGER.info("Safely demoting double {} to long for field '{}'", doubleVal, fieldName);
                        return (long) doubleVal;
                    }
                }
                break;
        }
        
        return null; // Indicates we can't safely demote
    }
    
    /**
     * Checks if a float can be converted to an int without data loss
     */
    public static boolean canSafelyConvertToInt(float value) {
        // Check if value has no fractional part and is within int range
        return Math.floor(value) == value 
                && value >= Integer.MIN_VALUE 
                && value <= Integer.MAX_VALUE;
    }
    
    /**
     * Checks if a double can be converted to an int without data loss
     */
    public static boolean canSafelyConvertToInt(double value) {
        // Check if value has no fractional part and is within int range
        return Math.floor(value) == value 
                && value >= Integer.MIN_VALUE 
                && value <= Integer.MAX_VALUE;
    }
    
    /**
     * Checks if a long can be converted to an int without data loss
     */
    public static boolean canSafelyConvertToInt(long value) {
        // Check if value is within int range
        return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
    }
    
    /**
     * Checks if a float can be converted to a long without data loss
     */
    public static boolean canSafelyConvertToLong(float value) {
        // Check if value has no fractional part
        // Note: float doesn't have enough precision to represent all longs,
        // so this is a best-effort check
        return Math.floor(value) == value;
    }
    
    /**
     * Checks if a double can be converted to a long without data loss
     */
    public static boolean canSafelyConvertToLong(double value) {
        // Check if value has no fractional part and is within long range
        // Note: double can't represent all possible long values precisely
        return Math.floor(value) == value 
                && value >= Long.MIN_VALUE 
                && value <= Long.MAX_VALUE;
    }
    
    /**
     * Checks if a double can be represented as a float without significant precision loss
     */
    public static boolean canSafelyConvertToFloat(double value) {
        // Convert to float and back to double to see if precision was lost
        float asFloat = (float) value;
        double backToDouble = (double) asFloat;
        
        // Check if the conversion causes significant change
        return Math.abs(value - backToDouble) < 0.00001;
    }
    
    /**
     * Determines if a string value can be parsed as a specific data type
     */
    public static boolean canParseAs(String value, Type.TypeID typeId) {
        try {
            switch (typeId) {
                case INTEGER:
                    Integer.parseInt(value);
                    return true;
                case LONG:
                    Long.parseLong(value);
                    return true;
                case FLOAT:
                    Float.parseFloat(value);
                    return true;
                case DOUBLE:
                    Double.parseDouble(value);
                    return true;
                case BOOLEAN:
                    return value.equalsIgnoreCase("true") || 
                           value.equalsIgnoreCase("false") ||
                           value.equals("1") || 
                           value.equals("0");
                case DECIMAL:
                    new BigDecimal(value);
                    return true;
                default:
                    return false;
            }
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Checks if a type change is compatible according to Iceberg evolution rules
     * 
     * @param fromType Original type
     * @param toType Target type
     * @return true if the types are compatible for evolution
     */
    public static boolean isTypeChangeCompatible(Type.TypeID fromType, Type.TypeID toType) {
        if (fromType == toType) {
            return true;
        }
        
        switch (fromType) {
            case INTEGER:
                // int can promote to long, float, double or decimal
                return toType == Type.TypeID.LONG || 
                       toType == Type.TypeID.FLOAT || 
                       toType == Type.TypeID.DOUBLE || 
                       toType == Type.TypeID.DECIMAL;
                
            case LONG:
                // long can promote to float, double or decimal
                return toType == Type.TypeID.FLOAT || 
                       toType == Type.TypeID.DOUBLE || 
                       toType == Type.TypeID.DECIMAL;
                
            case FLOAT:
                // float can promote to double or decimal
                return toType == Type.TypeID.DOUBLE || 
                       toType == Type.TypeID.DECIMAL;
                
            case DOUBLE:
                // double can only promote to decimal
                return toType == Type.TypeID.DECIMAL;
                
            default:
                // Any type can be represented as string
                return toType == Type.TypeID.STRING;
        }
    }
} 