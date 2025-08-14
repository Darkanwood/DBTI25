package fhwedel.Mongo;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class CRUDclientTest {

    @Test
    public void testInsertOne() {
        try (MongoClient client = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase db = client.getDatabase("bibliothek");
            MongoCollection<Document> buecher = db.getCollection("buecher");
            MongoCollection<Document> leser   = db.getCollection("leser");

            buecher.deleteMany(new Document());
            leser.deleteMany(new Document());

            buecher.insertOne(new Document("invnr","INV001")
                .append("titel","Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers")
                .append("autor","Marc-Uwe Kling")
                .append("verlag","Ullstein"));

            leser.insertOne(new Document("lnr","L001")
                .append("name","Friedrich Funke")
                .append("adresse", new Document("strasse","Bahnhofstraße 17")
                    .append("plz",23758).append("ort","Oldenburg")));

            assertEquals(1, buecher.countDocuments());
            assertEquals(1, leser.countDocuments());
        }
    }

    @Test
    public void testInsertMany() {
        try (MongoClient client = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase db = client.getDatabase("bibliothek");
            MongoCollection<Document> buecher = db.getCollection("buecher");
            MongoCollection<Document> leser   = db.getCollection("leser");

            buecher.deleteMany(new Document());
            leser.deleteMany(new Document());

            buecher.insertMany(Arrays.asList(
                new Document("invnr","INV002").append("titel","QualityLand").append("autor","Marc-Uwe Kling").append("verlag","Ullstein"),
                new Document("invnr","INV003").append("titel","Der König von Berlin").append("autor","Horst Evers").append("verlag","Rowohlt")
            ));

            leser.insertMany(Arrays.asList(
                new Document("lnr","L002").append("name","Anna Meier")
                    .append("adresse", new Document("strasse","Mühlenweg 3").append("plz",24105).append("ort","Kiel"))
            ));

            assertEquals(2, buecher.countDocuments());
            assertEquals(1, leser.countDocuments());
        }
    }

    @Test
    public void testEntliehenInsert() {
        try (MongoClient client = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase db = client.getDatabase("bibliothek");
            MongoCollection<Document> buecher   = db.getCollection("buecher");
            MongoCollection<Document> leser     = db.getCollection("leser");
            MongoCollection<Document> entliehen = db.getCollection("entliehen");

            buecher.deleteMany(new Document());
            leser.deleteMany(new Document());
            entliehen.deleteMany(new Document());

            buecher.insertOne(new Document("invnr","INV001"));
            leser.insertOne(new Document("lnr","L001"));

            entliehen.insertOne(new Document("lnr","L001")
                .append("invnr","INV001")
                .append("rueckgabedatum", null));

            assertEquals(1, entliehen.countDocuments());
        }
    }

    @Test
    public void testCountDocuments() {
        try (MongoClient client = MongoClients.create("mongodb://localhost:27017")) {
            MongoDatabase db = client.getDatabase("bibliothek");
            MongoCollection<Document> buecher = db.getCollection("buecher");

            buecher.deleteMany(new Document());

            buecher.insertMany(Arrays.asList(
                new Document("invnr","INV001"),
                new Document("invnr","INV002")
            ));

            long anzahl = buecher.countDocuments();
            System.out.println("Anzahl Bücher: " + anzahl);
            assertEquals(2L, anzahl); // long vergleichen
        }
    }
}
