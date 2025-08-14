package fhwedel.Mongo;

import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class CRUDclient {

    public static void main(String[] args) {
        String mariaUrl  = getenvOr("MARIADB_URL", "jdbc:mariadb://localhost:3306/firma");
        String mariaUser = getenvOr("MARIADB_USER", "root");
        String mariaPass = getenvOr("MARIADB_PASS", "password");
        String mongoUri  = getenvOr("MONGO_URI", "mongodb://localhost:27017");
        String mongoDb   = getenvOr("MONGO_DB", "firma");

        System.out.println("[INFO] Starte Import: MariaDB -> MongoDB");
        System.out.println("[INFO] MariaDB: " + mariaUrl + " (user=" + mariaUser + ")");
        System.out.println("[INFO] MongoDB: " + mongoUri + " / DB=" + mongoDb);

        try (Connection maria = DriverManager.getConnection(mariaUrl, mariaUser, mariaPass);
             MongoClient mClient = MongoClients.create(mongoUri)) {

            MongoDatabase db = mClient.getDatabase(mongoDb);

            // Ziel-Collections
            MongoCollection<Document> abteilungen  = db.getCollection("abteilungen");
            MongoCollection<Document> gehalt       = db.getCollection("gehalt");
            MongoCollection<Document> personal     = db.getCollection("personal");

            // Für wiederholte Läufe Collections leeren
            abteilungen.deleteMany(new Document());
            gehalt.deleteMany(new Document());
            personal.deleteMany(new Document());

            // Import in logischer Reihenfolge
            importAbteilungen(maria, abteilungen);
            importGehalt(maria, gehalt);
            importPersonalMitEinbettungen(maria, personal);

            // Indexe erstellen
            personal.createIndex(Indexes.ascending("pnr"), new IndexOptions().unique(true));
            abteilungen.createIndex(Indexes.ascending("abt_nr"), new IndexOptions().unique(true));
            gehalt.createIndex(Indexes.ascending("geh_stufe"), new IndexOptions().unique(true));

            // Abschluss-Info
            System.out.println("[OK] Import abgeschlossen.");
            System.out.printf("     personal:    %d%n", personal.countDocuments());
            System.out.printf("     abteilungen: %d%n", abteilungen.countDocuments());
            System.out.printf("     gehalt:      %d%n", gehalt.countDocuments());

        } catch (SQLException e) {
            System.err.println("[ERROR] SQL: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        } catch (Exception e) {
            System.err.println("[ERROR] General: " + e.getMessage());
            e.printStackTrace();
            System.exit(3);
        }
    }

    /**
     * Liest alle Abteilungen aus MariaDB und speichert sie als einzelne
     * Dokumente in der Collection abteilungen.
     *
     * @param maria Offene MariaDB-Verbindung
     * @param abteilungen Ziel-Collection in MongoDB
     * @throws SQLException Falls ein SQL-Fehler auftritt
     */
    private static void importAbteilungen(Connection maria, MongoCollection<Document> abteilungen) throws SQLException {
        String sql = "SELECT abt_nr, name FROM abteilung";

        try (Statement st = maria.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            int n = 0;

            while (rs.next()) {
                abteilungen.insertOne(new Document("abt_nr", nTrim(rs.getString("abt_nr")))
                        .append("name", nTrim(rs.getString("name"))));
                n++;
            }

            System.out.println("Abteilungen importiert: " + n);
        }
    }

    /**
     * Liest alle Gehaltsstufen aus MariaDB und speichert sie in der Collection "gehalt".
     *
     * @param maria Offene MariaDB-Verbindung
     * @param gehalt Ziel-Collection in MongoDB
     * @throws SQLException Falls ein SQL-Fehler auftritt
     */
    private static void importGehalt(Connection maria, MongoCollection<Document> gehalt) throws SQLException {
        String sql = "SELECT geh_stufe, betrag FROM gehalt";

        try (Statement st = maria.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            int n = 0;

            while (rs.next()) {
                gehalt.insertOne(new Document("geh_stufe", nTrim(rs.getString("geh_stufe")))
                        .append("betrag", rs.getInt("betrag")));
                n++;
            }
            
            System.out.println("Gehalt importiert: " + n);
        }
    }


/**
 * Liest alle Mitarbeiter aus MariaDB und ergänzt pro Mitarbeiter
 * eingebettete Arrays für Kinder, Prämien und Maschinen.
 * Nutzt die migrierte Struktur mit personal.kkid + Tabelle krankenversicherung.
 * @param maria Offene MariaDB-Verbindung
 * @param personal Ziel-Collection in MongoDB, in die die importierten Dokumente eingefügt werden
 * @throws SQLException Bei SQL Fehlern
 */
private static void importPersonalMitEinbettungen(Connection maria, MongoCollection<Document> personal) throws SQLException {
    String sqlP = 
    "SELECT p.pnr, p.name, p.vorname, p.geh_stufe, p.abt_nr, p.kkid, " +
    "kv.kuerzel AS kk_kuerzel " +
    "FROM personal p " +
    "LEFT JOIN krankenversicherung kv ON kv.kkid = p.kkid";


    try (Statement st =  maria.createStatement();
         ResultSet rs = st.executeQuery(sqlP)) {
        int n = 0;
        while (rs.next()) {
            int pnr = rs.getInt("pnr");

            Document doc = new Document("pnr", pnr)
                .append("name", nTrim(rs.getString("name")))
                .append("vorname", nTrim(rs.getString("vorname")))
                .append("geh_stufe", nTrim(rs.getString("geh_stufe")))
                .append("abt_nr", nTrim(rs.getString("abt_nr")))
                // bevorzugt: Kürzel wie im Konzept
                .append("krankenkasse", nTrim(rs.getString("kk_kuerzel")))
                // optional zusätzlich (hilfreich fürs Debuggen/Analysen):
                .append("kkid", rs.getObject("kkid") == null ? null : rs.getInt("kkid"))
                .append("kinder", ladeKinder(maria, pnr))
                .append("praemien", ladePraemien(maria, pnr))
                .append("maschinen", ladeMaschinen(maria, pnr));

            personal.insertOne(doc);
            n++;
        }
        System.out.println("Personal importiert: " + n);
    }
}


    /**
     * Lädt alle Kinder zu einem Mitarbeiter und gibt sie als Liste von Dokumenten zurück.
     *
     * @param maria Offene MariaDB-Verbindung
     * @param pnr Personalnummer
     * @return Liste von Kinder-Dokumenten
     * @throws SQLException Falls ein SQL-Fehler auftritt
     */
    private static List<Document> ladeKinder(Connection maria, int pnr) throws SQLException {

        String sql = "SELECT k_name, k_vorname, k_geb FROM kind WHERE pnr = ?";

        try (PreparedStatement ps = maria.prepareStatement(sql)) {
            ps.setInt(1, pnr);
            try (ResultSet rs = ps.executeQuery()) {
                List<Document> list = new ArrayList<>();

                while (rs.next()) {
                    list.add(new Document("k_name", nTrim(rs.getString("k_name")))
                            .append("k_vorname", nTrim(rs.getString("k_vorname")))
                            .append("k_geb", rs.getInt("k_geb")));
                }

                return list;
            }
        }
    }

    /**
     * Lädt alle Prämien zu einem Mitarbeiter.
     *
     * @param maria Offene MariaDB-Verbindung
     * @param pnr Personalnummer
     * @return Liste von Prämienbeträgen
     * @throws SQLException Falls ein SQL-Fehler auftritt
     */
    private static List<Integer> ladePraemien(Connection maria, int pnr) throws SQLException {
        String sql = "SELECT p_betrag FROM praemie WHERE pnr = ?";

        try (PreparedStatement ps = maria.prepareStatement(sql)) {
            ps.setInt(1, pnr);
            try (ResultSet rs = ps.executeQuery()) {
                List<Integer> list = new ArrayList<>();
                
                while (rs.next()) {
                    list.add(rs.getInt("p_betrag"));
                }

                return list;
            }
        }
    }

    /**
     * Lädt alle Maschinen zu einem Mitarbeiter.
     *
     * @param maria Offene MariaDB-Verbindung
     * @param pnr Personalnummer
     * @return Liste von Maschinen-Dokumenten
     * @throws SQLException Falls ein SQL-Fehler auftritt
     */
    private static List<Document> ladeMaschinen(Connection maria, int pnr) throws SQLException {
        String sql = "SELECT mnr, name, ansch_datum, neuwert, zeitwert FROM maschine WHERE pnr = ?";

        try (PreparedStatement ps = maria.prepareStatement(sql)) {
            ps.setInt(1, pnr);
            try (ResultSet rs = ps.executeQuery()) {
                List<Document> list = new ArrayList<>();
                
                while (rs.next()) {
                    list.add(new Document("mnr", rs.getInt("mnr"))
                            .append("name", nTrim(rs.getString("name")))
                            .append("ansch_datum", rs.getDate("ansch_datum"))
                            .append("neuwert", rs.getInt("neuwert"))
                            .append("zeitwert", rs.getInt("zeitwert")));
                }

                return list;
            }
        }
    }

    /**
     * Trimt einen String sicher (behandelt null-Werte).
     *
     * @param s Eingabestring
     * @return Getrimmter String oder null
     */
    private static String nTrim(String s) {
        return s == null ? null : s.trim();
    }

    /**
     * Liest eine Umgebungsvariable aus oder gibt den Standardwert zurück.
     *
     * @param key Name der Variable
     * @param def Standardwert, falls nicht gesetzt
     * @return Wert der Variable oder Standardwert
     */
    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
