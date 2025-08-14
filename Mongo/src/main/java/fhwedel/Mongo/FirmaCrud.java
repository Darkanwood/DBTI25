package fhwedel.Mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

public class FirmaCrud {

    private final MongoCollection<Document> personal;
    private final MongoCollection<Document> gehalt;


    /**
     * Erstellt ein FirmaCurd Objekt und initialisiert die internen Referenzen
     * auf die Collections personal und gehalt der angegebenen MongoDB
     * @param db Offene Verbindung zu einer MongoDB-Datenbank
     */
    public FirmaCrud(MongoDatabase db) {
        this.personal = db.getCollection("personal");
        this.gehalt   = db.getCollection("gehalt");
    }

    

    /**
     * Legt einen neuen Mitarbeiter in der Collection personal an. Falls bereits ein Datensatz mit der
     * angegebenen Personalnummer existiert, wird dieser vorher gelöscht (idempotentes Verhalten)
     * @param pnr Eindeutige Personalnummer
     * @param name Nachname des Mitarbeiters
     * @param vorname Vorname des Mitarbeiters
     * @param abtNr Abteilungsnummer
     * @param gehStufe Gehaltsstuffe
     * @param krankenkasse kürzel der Krankenkasse
     * @return true, wenn der Mitarbeiter neu angelegt wurde, ansonsten false
     */
    public boolean createPersonal(int pnr, String name, String vorname,
                                  String abtNr, String gehStufe, String krankenkasse) {
        assert pnr > 0;
        
        // einfache Validierung
        if (pnr <= 0){
            throw new IllegalArgumentException("pnr muss > 0 sein");
        }

        name        = nTrim(name);
        vorname     = nTrim(vorname);
        abtNr       = nTrim(abtNr);
        gehStufe    = nTrim(gehStufe);
        krankenkasse= nTrim(krankenkasse);

        // idempotent: vorhandenen Datensatz entfernen
        personal.deleteOne(eq("pnr", pnr));

        var doc = new Document("pnr", pnr)
                .append("name", name)
                .append("vorname", vorname)
                .append("abt_nr", abtNr)
                .append("geh_stufe", gehStufe)
                .append("krankenkasse", krankenkasse);

        personal.insertOne(doc);
        return true;
    }

    

    /**
     * Liest alle Mitarbeiter-Dokumente aus der Collection personal. Es kann optional ein Filterdokument
     * angegeben werden, um die Ergebnismenge einzuschränken.
     * @param optionalFilter MongoDB-Filterdokument
     * @return LIste der gefundenen Mitarbeiter-Dokumente
     */
    public List<Document> readPersonal(Document optionalFilter) {
        var filter = optionalFilter == null ? new Document() : optionalFilter;
        List<Document> out = new ArrayList<>();
       
        for (var d : personal.find(filter)) out.add(d);
        return out;
    }

    /** Liest eine Person per pnr. */
    public Document readPersonalByPnr(int pnr) {
        return personal.find(eq("pnr", pnr)).first();
    }


    
    /**
     * Erhöt den Betrag einer bestimmten Gehaltsstufe prozentual. Der neue Betrag wird auf eine Ganzzahl
     * gerundet
     * @param stufe Gehaltsstufe, die angepasst werden soll
     * @param percentPlus Prozentualler Zuschlag
     * @return Array mit zwei Werten, den alten und den neuen Betrag
     * @throws IllegalStateException Wenn Gehaltstufe nicht gefunden wird oder Update fehlschlägt
     */
    public int[] updateGehaltPercent(String stufe, double percentPlus) {
        Objects.requireNonNull(stufe, "stufe");
        var doc = gehalt.find(eq("geh_stufe", stufe)).first();
        
        if (doc == null){
            throw new IllegalStateException("Gehaltsstufe nicht gefunden: " + stufe);
        }

        int alt = ((Number) doc.get("betrag")).intValue();
        int neu = Math.toIntExact(Math.round(alt * (1.0 + percentPlus / 100.0)));
        
        UpdateResult res = gehalt.updateOne(eq("geh_stufe", stufe), set("betrag", neu));
       
        if (res.getMatchedCount() == 0){
            throw new IllegalStateException("Update fehlgeschlagen (match=0).");
        }
        return new int[]{alt, neu};
    }

   

    /**
     * Akutalisiert die Angaben eines Mitarbeiters in der Collection
     * @param pnr Personalnummer des Mitarbeiters
     * @param abtNr neue Abteilungsnummer
     * @param gehStufe neue Gehaltsstufe
     * @param krankenkasse neue Krankenkasse
     * @return UpdateResult mit Information zum Update
     * @throws IllegalArgumentException falls keine der übergebenen Felder gesetzt ist
     */
    public UpdateResult updatePersonal(int pnr, String abtNr, String gehStufe, String krankenkasse) {
        var updates = new ArrayList<org.bson.conversions.Bson>();

        if (abtNr != null){        
            updates.add(set("abt_nr", nTrim(abtNr)));
        }

        if (gehStufe != null){     
            updates.add(set("geh_stufe", nTrim(gehStufe)));
        }

        if (krankenkasse != null){
            updates.add(set("krankenkasse", nTrim(krankenkasse)));
        }

        if (updates.isEmpty()){ 
            throw new IllegalArgumentException("Keine zu ändernden Felder gesetzt.");
        }

        return personal.updateOne(eq("pnr", pnr), combine(updates));
    }


   
    /**
     * Löscht einen Mitarbeiter aus der Collection personal anhand der Personalnummer
     * @param pnr Personalnummer des zu löschenden Mitarbeiters
     * @return DeleteResult mit Informationen zum Löschsvorgang
     */
    public DeleteResult deletePersonalByPnr(int pnr) {
        return personal.deleteOne(eq("pnr", pnr));
    }

    

    /**
     * Löscht alle Mitarbeiter aus der Collection personal, die den angegebenen Vor- und Nachnamen haben
     * @param vorname Vorname des Mitarbeiters
     * @param name Nachname des Mitarbeiters
     * @return DeleteResult mit Informationen zum Löschvorgang
     */
    public DeleteResult deletePersonalByName(String vorname, String name) {
        return personal.deleteMany(and(eq("name", nTrim(name)), eq("vorname", nTrim(vorname))));
    }

    

    /**
     * Gibt eine Liste aller Mitarbeiter in der angegebenen Abteilung zurück
     * @param abtNr Abteilungsnummer
     * @return Liste der passenden Document Objekte aus der Collection
     */
    public List<Document> listPersonalInAbteilung(String abtNr) {
        List<Document> out = new ArrayList<>();
        for (var d : personal.find(eq("abt_nr", nTrim(abtNr)))) out.add(d);
        return out;
    }


   
    /**
     * Zählt, wie viele Mitarbeiter pro Abteilung vorhanden sind und sortiert das Ergebnis absteigend
     * nacht der Mitarbeiteranzahl
     * @return Liste von Document-Objekten, die pro Abteilung die Abeilungsnummer und die Anzahl der
     * Mitarbeiter anzahl enthalten.
     */
    public List<Document> countPersonalByAbteilung() {
        return personal.aggregate(List.of(
                new Document("$group", new Document("_id", "$abt_nr").append("anzahl", new Document("$sum", 1))),
                new Document("$sort", new Document("anzahl", -1))
        )).into(new ArrayList<>());
    }



    /**
     * Hilfsfunktion
     * Entfernt führende und nachfolgende Leerzeichen aus einem String
     * @param s der zu bearbeitende String
     * @return der getrimmte String
     */
    private static String nTrim(String s) { return s == null ? null : s.trim(); }


    //Testausgabe
    public static void main(String[] args) {
    String mongoUri  = getEnvOrDefault("MONGO_URI", "mongodb://localhost:27017");
    String mongoDb   = getEnvOrDefault("MONGO_DB", "firma");

    System.out.println("[INFO] Starte CRUD auf MongoDB");
    System.out.println("[INFO] MongoDB: " + mongoUri + " / DB=" + mongoDb);

    try (MongoClient mClient = MongoClients.create(mongoUri)) {
        MongoDatabase db = mClient.getDatabase(mongoDb);

        var crud = new FirmaCrud(db);

        // Create
        System.out.println("\n=== (a) CREATE: Henrik Krause ===");
        crud.createPersonal(417, "Krause", "Henrik", "d13", "it1", "tkk");
        System.out.println("[CHECK] " + crud.readPersonalByPnr(417));

        // Read
        System.out.println("\n=== (b) READ: Alle Personal ===");
        var alle = crud.readPersonal(null);
        System.out.println("[READ] Anzahl: " + alle.size());

        // Update
        System.out.println("\n=== (c) UPDATE: Gehalt it1 +10% ===");
        int[] altNeu = crud.updateGehaltPercent("it1", 10.0);
        System.out.printf("[RESULT] it1: %d -> %d%n", altNeu[0], altNeu[1]);

        // Delete
        System.out.println("\n=== (d) DELETE: Lutz Tietze ===");
        var del = crud.deletePersonalByPnr(135);
        System.out.println("[RESULT] gelöscht: " + del.getDeletedCount());

        // Query
        System.out.println("\n=== (e) QUERY: Abteilung Verkauf (d15) ===");
        var verkauf = crud.listPersonalInAbteilung("d15");
        System.out.println("[RESULT] Anzahl in Verkauf: " + verkauf.size());
        for (var d : verkauf) {
            System.out.printf("  pnr=%s  %s %s%n", d.get("pnr"), d.getString("vorname"), d.getString("name"));
        }

        System.out.println("\n[DONE] CRUD-Teil abgeschlossen.");
    }
}

/**
 * Hilfsfunktion
 * Liefert den Wert einer Umgebungsvariable oder einen Standartwert, falls die Variable nicht gesetzt oder
 * leer ist.
 * @param key der Name der Umgebungsvariablen
 * @param def der Standartwert, der zurückgegeben wird, wenn die Variable nicht existiert oder leer ist 
 * @return der Wert der Umgebungsvariablen oder def, falls nicht vorhanden
 */
private static String getEnvOrDefault(String key, String def) {
    String v = System.getenv(key);
    return (v == null || v.isEmpty()) ? def : v;
}


}
