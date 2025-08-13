package fhwedel.JDBC;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Hello {


    /**
     * Stellt eine Verbindung zu einer Datenbank her
     * @param url die URL der Datenbank
     * @param userName Der Benutzername für die Anmeldung an der Dantenbanke
     * @param password das Passwort für die Anmeldung an der Datenbank
     * @return Ein Connection-Objekt, das die aktivie Verbindung zur Datenbank darstellt
     * @throws RuntimeException wenn eine SQLException beim Verbindungsaufbau auftritt
     */
    public static Connection login(String url, String userName, String password) {
        try {
            Connection con = DriverManager.getConnection(url, userName, password);
            System.out.println("Connection established.");
            return con;
        } catch (SQLException e) {
            System.out.println("Connection failed");
            throw new RuntimeException(e);
        }
    }


    /**
     * Fügt einen neuen Datensatz in die Tabelle ein oder aktualisiert diese falls
     * bereits ein Eintrag mit derselben pnr existiert
     * @param con Eine offene COnnection zur Datenbank
     * @param pnr Personalnummer (Primärschlüssel)
     * @param name Name der Person
     * @param vorname Vorname der Person
     * @param geh_stufe Gehaltsstufe
     * @param abt_nr Abteilungsnummer
     * @param krankenkasse Krankenkassenkürzel
     * @return Anzahl der betroffenen Zeilen
     * @throws RuntimeException wenn ein SQLException wärend der Ausführung auftritt
     * @throws IllegalArgumentException wenn ein Parameter die Längenbeschränkung verletzt oder name null ist
     */
    public static int addDataPersonal(Connection con, int pnr, String name, String vorname,
                                      String geh_stufe, String abt_nr, String krankenkasse) {
        if (name == null || name.length() > 20){
            throw new IllegalArgumentException("name zu lang");
        }

        if (vorname != null && vorname.length() > 20){
            throw new IllegalArgumentException("vorname zu lang");
        }

        if (geh_stufe != null && geh_stufe.length() > 4){
            throw new IllegalArgumentException("geh_stufe zu lang");
        }

        if (abt_nr != null && abt_nr.length() > 3){
            throw new IllegalArgumentException("abt_nr zu lang");
        }

        if (krankenkasse != null && krankenkasse.length() > 3){
            throw new IllegalArgumentException("krankenkasse zu lang");
        }

        String sql =
                "INSERT INTO personal (pnr, name, vorname, geh_stufe, abt_nr, krankenkasse) " +
                        "VALUES (?,?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE name=VALUES(name), vorname=VALUES(vorname), " +
                        "geh_stufe=VALUES(geh_stufe), abt_nr=VALUES(abt_nr), krankenkasse=VALUES(krankenkasse)";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, pnr);
            ps.setString(2, name);
            ps.setString(3, vorname);
            ps.setString(4, geh_stufe);
            ps.setString(5, abt_nr);
            ps.setString(6, krankenkasse);

            return ps.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("INSERT/UPDATE personal fehlgeschlagen", e);
        }
    }


    /**
     * Select Abfrage auf der angegebenen Tabelle aus und gibt alle Datensätze
     * Zeilenweise in der Konsole aus
     * @param con Offene Connection zur Datenbank
     * @param tableName Name der Tabelle, deren Inhalt angezeigt werden soll
     * @throws RuntimeException wenn ein SQLException wärend der Ausführung auftritt
     */
    public static void showAll(Connection con, String tableName) {
        String sql = "SELECT * FROM " + tableName;

        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData m = rs.getMetaData();
            int c = m.getColumnCount();

            while (rs.next()) {

                StringBuilder row = new StringBuilder();

                for (int i = 1; i <= c; i++) {
                    row.append(m.getColumnLabel(i)).append(": ").append(rs.getString(i));
                    if (i < c) row.append(" | ");
                }
                //Teststelle
                System.out.println(row);
            }

        } catch (SQLException e) {
            throw new RuntimeException("SELECT * FROM " + tableName + " fehlgeschlagen", e);
        }
    }


    /**
     * Erhöht den Betrag aller Gehälter einer bestimmten Gehaltsstufe prozentual
     * @param con Offene Connection zur Datenbank
     * @param percent Prozentuale Erhöhrung
     * @param gehStufe Gehaltsstufe, die angepasst werden soll
     * @return Die Anzahl der betroffenen Datensätze
     * @throws RuntimeException wenn ein SQLException wärend der Ausführung auftritt
     */
    public static int raiseSalaryPercent(Connection con, int percent, String gehStufe) {
        String sql = "UPDATE gehalt SET betrag = ROUND(betrag * (1 + ?/100.0), 0) WHERE geh_stufe = ?";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, percent);
            ps.setString(2, gehStufe);

            return ps.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("UPDATE gehalt fehlgeschlagen", e);
        }
    }


    /**
     * Löscht alle Datensätze aus der Tabelle personal, deren
     * Name exakt dem angegebenen namen entpsricht
     * @param con Offene Connection zur Datenbank
     * @param name Nachmane der zu löschenden Mitarbeiter
     * @return die Anzahl der gelöschten Datensätze
     * @throws RuntimeException wenn ein SQLException wärend der Ausführung auftritt
     */
    public static int deletePersonalByName(Connection con, String name) {
        String sql = "DELETE FROM personal WHERE name = ?";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, name);

            return ps.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("DELETE personal fehlgeschlagen", e);
        }
    }


    /**
     * Ruft alle Mitarbeiter aus der Abteilung Verkauf ab
     * @param con Offene Connection zur Datenbank
     * @return Liste der Mitarbeiter in der Abteilung Verkauf; leere liste, weknn keine gefunden werden
     * @throws RuntimeExcewption wenn ein SQLException während der Abfrage auftritt
     */
    public static List<String> employeesInVerkauf(Connection con) {
        String sql =
                "SELECT p.pnr, p.name, p.vorname " +
                        "FROM personal p " +
                        "JOIN abteilung a ON a.abt_nr = p.abt_nr " +
                        "WHERE TRIM(a.name) = 'Verkauf'";

        List<String> result = new ArrayList<>();

        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                result.add(rs.getInt(1) + " - " + rs.getString(2) + ", " + rs.getString(3));
            }

            return result;

        } catch (SQLException e) {
            throw new RuntimeException("Abfrage Verkauf fehlgeschlagen", e);
        }
    }

    // 7) Migration: Krankenkasse-Kürzel -> separate Tabelle + FK
    /**
     * Migriert die Krankenkassen-Inforamtionen in eineeigene Tabelle krankenversicherung
     * und ersetzt die bisherigen Kürzel personal durch Fremdschlüsselreferenzen.
     * @param con Offene Connection zur Datenbank
     */
    public static void migrateKrankenkasse(Connection con) {
        try {
            con.setAutoCommit(false);

            // neue Tabellen anlegen
            try (Statement st = con.createStatement()) {
                st.execute(
                        "CREATE TABLE IF NOT EXISTS krankenversicherung(" +
                                "  kkid INT PRIMARY KEY," +
                                "  kuerzel CHAR(3) UNIQUE NOT NULL," +
                                "  name VARCHAR(100) NOT NULL" +
                                ")");

                st.execute(
                        "CREATE TABLE IF NOT EXISTS personal_neu(" +
                                "  pnr INT PRIMARY KEY," +
                                "  name CHAR(20) NOT NULL," +
                                "  vorname CHAR(20)," +
                                "  geh_stufe VARCHAR(4)," +
                                "  abt_nr CHAR(3)," +
                                "  kkid INT," +
                                "  CONSTRAINT fk_geh FOREIGN KEY (geh_stufe) REFERENCES gehalt(geh_stufe)," +
                                "  CONSTRAINT fk_abt FOREIGN KEY (abt_nr) REFERENCES abteilung(abt_nr)," +
                                "  CONSTRAINT fk_kk  FOREIGN KEY (kkid) REFERENCES krankenversicherung(kkid)" +
                                ")"
                );
            }

            // Stammdaten in krankenversicherung einfügen
            try (PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO krankenversicherung(kkid, kuerzel, name) VALUES (?,?,?) " +
                            "ON DUPLICATE KEY UPDATE name=VALUES(name)")) {
                Object[][] data = {
                        {1,"aok","Allgemeine Ortskrankenkasse"},
                        {2,"bak","Betriebskrankenkasse B. Braun Aesculap"},
                        {3,"bek","Barmer Ersatzkasse"},
                        {4,"dak","Deutsche Angestelltenkrankenkasse"},
                        {5,"tkk","Techniker Krankenkasse"},
                        {6,"kkh","Kaufmännische Krankenkasse"}
                };
                for (Object[] r : data) {
                    ps.setInt(1,(int) r[0]);
                    ps.setString(2,(String) r[1]);
                    ps.setString(3,(String) r[2]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // Daten migrieren (Kürzel -> kkid)
            try (Statement st = con.createStatement()) {
                st.execute(
                        "INSERT INTO personal_neu(pnr,name,vorname,geh_stufe,abt_nr,kkid) " +
                                "SELECT pnr, name, vorname, geh_stufe, abt_nr, kv.kkid " +
                                "FROM personal p LEFT JOIN krankenversicherung kv ON kv.kuerzel = p.krankenkasse");
            }

            // Swap
            try (Statement st = con.createStatement()) {
                st.execute("RENAME TABLE personal TO personal_alt, personal_neu TO personal");
            }

            // Aufräumen
            try (Statement st = con.createStatement()) {
                st.execute("DROP TABLE personal_alt");
            }

            con.commit();
            con.setAutoCommit(true);

            //Testausgabe
            System.out.println("Migration erfolgreich abgeschlossen.");

        } catch (SQLException e) {
            try { con.rollback(); } catch (SQLException ignored) {}
            throw new RuntimeException("Migration fehlgeschlagen", e);
        }
    }

    public static void main(String[] args) {
        String url = "jdbc:mariadb://localhost:3306/firma";
        String user = "root";
        String pass = "password";

        //Finale Testausgaben
        try (Connection con = login(url, user, pass)) {


            addDataPersonal(con, 417, "Krause", "Henrik", "it1", "d15", "tkk");


            System.out.println("== personal (vor Änderungen) ==");
            showAll(con, "personal");


            int upd = raiseSalaryPercent(con, 10, "it1");
            System.out.println("Gehaltsstufe it1 angepasst: " + upd + " Zeile(n).");


            int del = deletePersonalByName(con, "Tietze");
            System.out.println("Gelöscht: " + del + " Zeile(n) für 'Tietze'.");

            // Abfrage Verkauf
            System.out.println("MitarbeiterInnen in Abteilung 'Verkauf':");
            for (String s : employeesInVerkauf(con)) System.out.println("  " + s);


            migrateKrankenkasse(con);


            System.out.println("== personal (nach Migration) ==");
            showAll(con, "personal");
            System.out.println("== krankenversicherung ==");
            showAll(con, "krankenversicherung");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
