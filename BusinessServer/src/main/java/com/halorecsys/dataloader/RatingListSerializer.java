package com.halorecsys.dataloader;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.List;

/**
 * @program: HaloRecSys
 * @description:
 * @author: HaloZhang
 * @create: 2021-05-10 19:32
 **/
public class RatingListSerializer extends JsonSerializer<List<Rating>> {
    @Override
    public void serialize(List<Rating> ratingList, JsonGenerator jsonGenerator,
                          SerializerProvider provider) throws IOException {
        jsonGenerator.writeStartArray();
        for (Rating rating : ratingList) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("rating", rating);
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }
}
