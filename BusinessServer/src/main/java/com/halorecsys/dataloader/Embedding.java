package com.halorecsys.dataloader;

import java.util.ArrayList;

/**
 * @program: HaloRecSys
 * @description: The definition of user and movie embedding vector
 * @author: HaloZhang
 * @create: 2021-05-06 20:34
 **/
public class Embedding {
    ArrayList<Float> embVector;

    public Embedding() {
        this.embVector = new ArrayList<Float>();
    }

    public Embedding(ArrayList<Float> embVector) {
        this.embVector = embVector;
    }

    public ArrayList<Float> getEmbVector() {
        return this.embVector;
    }

    public void setEmbVector(ArrayList<Float> embVector) {
        this.embVector = embVector;
    }

    //calculate cosine similarity between two embeddings
    public double calculateSimilarity(Embedding otherEmb){
        if (null == embVector || null == otherEmb || null == otherEmb.getEmbVector()
                || embVector.size() != otherEmb.getEmbVector().size()) {
            return -1;
        }
        double dotProduct = 0;
        double denominator1 = 0;
        double denominator2 = 0;
        for (int i = 0; i < embVector.size(); i++) {
            dotProduct += embVector.get(i) * otherEmb.getEmbVector().get(i);
            denominator1 += embVector.get(i) * embVector.get(i);
            denominator2 += otherEmb.getEmbVector().get(i) * otherEmb.getEmbVector().get(i);
        }
        return dotProduct / (Math.sqrt(denominator1) * Math.sqrt(denominator2));
    }
}
