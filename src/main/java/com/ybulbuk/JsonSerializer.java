package com.ybulbuk;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.beans.FeatureDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonSerializer {

    public static JSONArray serialize(ResultSet resultSet, Class clazz) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException, IntrospectionException, JSONException {

        final Object emptyObject = clazz.newInstance();
        final JSONObject emptyJsonObject = new JSONObject();

        Object resultObject = emptyObject;

        Map<Map.Entry<String, String>, Integer> fields = parseFields(resultSet, clazz);

        Map<String, PropertyDescriptor> propertyDescriptorMap =
                Stream.of(Introspector.getBeanInfo(clazz).getPropertyDescriptors())
                        .collect(Collectors.toMap(FeatureDescriptor::getName, featureDescriptor -> featureDescriptor));

        JSONArray jsonArray = new JSONArray();

        JSONObject results = emptyJsonObject;
        while (resultSet.next()) {

            for (Map.Entry<Map.Entry<String, String>, Integer> entry : fields.entrySet()) {

                propertyDescriptorMap.get(entry.getKey().getKey())
                        .getWriteMethod().invoke(resultObject, resultSet.getString(entry.getValue()));

                results.put(entry.getKey().getValue(), propertyDescriptorMap.get(entry.getKey().getKey()).getReadMethod().invoke(resultObject).toString());
            }

            jsonArray.put(results);

            results = emptyJsonObject;
            resultObject = emptyObject;
        }

        return jsonArray;

    }

    private static Map<Map.Entry<String, String>, Integer> parseFields(ResultSet resultSet, Class clazz) throws SQLException {

        class TableColumnDescriptor {

            private String columnName;
            private Integer columnIndex;

            private TableColumnDescriptor(String columnName, Integer columnIndex) {
                this.columnName = columnName;
                this.columnIndex = columnIndex;
            }

            private String getColumnName() {
                return columnName;
            }

            private Integer getColumnIndex() {
                return columnIndex;
            }

        }

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        List<TableColumnDescriptor> databaseColumnNames = new ArrayList<>();

        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {

            databaseColumnNames.add(new TableColumnDescriptor(resultSetMetaData.getColumnName(i), i));
        }

        List<Class> classes = new ArrayList<>();

        Class currentClass = clazz;

        do {

            classes.add(currentClass);
            currentClass = currentClass.getSuperclass();

        } while (currentClass != null);

        return classes.stream()
                .map(Class::getDeclaredFields)
                .flatMap(Stream::of)
                .filter(field -> !field.isAnnotationPresent(JsonIgnore.class))
                .collect(
                        Collectors.toMap(
                                Field::getName,
                                field -> {
                                    if (field.isAnnotationPresent(JsonProperty.class)) {

                                        JsonProperty jsonProperty = field.getAnnotation(JsonProperty.class);
                                        return jsonProperty.value().isEmpty() ? field.getName() : jsonProperty.value();
                                    }

                                    return field.getName();
                                })
                )
                .entrySet()
                .parallelStream()
                .collect(
                        Collectors.toMap(entry -> entry, entry ->
                                databaseColumnNames.stream()
                                        .filter(tableColumnDescriptor -> tableColumnDescriptor.getColumnName().equalsIgnoreCase(entry.getValue()))
                                        .findFirst()
                                        .orElseThrow(() -> new SerializationException("Column " + entry.getValue() + " is missing"))
                                        .getColumnIndex())
                );
    }
}
