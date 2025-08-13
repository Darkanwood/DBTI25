package fhwedel.JDBC;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Hello {

  // 1) Verbindung
  /**
   * Stellt eine Verbindung zu einer Datenbank her.
   * @param url       die JDBC-URL (z. B. jdbc:mariadb://localhost:3306/firma)
   * @param userName  DB-Benutzername
   * @param password  DB-Passwort
   * @return aktive {@link Connection}
   * @throws RuntimeException bei SQL-Fehlern
   */
  public static Connection login(String url, String userName, String password) {
    try {
      DriverManager.setLoginTimeout(5);
      Connection con = DriverManager.getConnection(url, userName, password);
      System.out.println("Connection established: " + url);
      return con;
    } catch (SQLException e) {
      System.err.println("Connection failed → " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  // 2) CREATE/UPSERT: Datensatz in personal
  /**
   * Fügt einen Datensatz in {@code personal} ein oder aktualisiert ihn bei gleicher PNR.
   * @param con          offene Verbindung
   * @param pnr          Personalnummer (PK)
   * @param name         Nachname (<= 20)
   * @param vorname      Vorname (<= 20, optional)
   * @param geh_stufe    Gehaltsstufe (<= 4)
   * @param abt_nr       Abteilungsnummer (<= 3)
   * @param krankenkasse Krankenkassenkürzel (<= 3)
   * @return betroffene Zeilen (1 Insert, 2 Update)
   */
  public static int addDataPersonal(Connection con, int pnr, String name, String vorname,
                                    String geh_stufe, String abt_nr, String krankenkasse) {
    if (name == null || name.length() > 20) throw new IllegalArgumentException("name zu lang (max 20)");
    if (vorname != null && vorname.length() > 20) throw new IllegalArgumentException("vorname zu lang (max 20)");
    if (geh_stufe != null && geh_stufe.length() > 4) throw new IllegalArgumentException("geh_stufe zu lang (max 4)");
    if (abt_nr != null && abt_nr.length() > 3) throw new IllegalArgumentException("abt_nr zu lang (max 3)");
    if (krankenkasse != null && krankenkasse.length() > 3) throw new IllegalArgumentException("krankenkasse zu lang (max 3)");

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

  // 3) READ: alle Datensätze einer Tabelle ausgeben
  /**
   * Führt ein {@code SELECT *} auf der angegebenen Tabelle aus und gibt die Zeilen auf der Konsole aus.
   * @param con       offene Verbindung
   * @param tableName Tabellenname (nur vertrauenswürdig verwenden)
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
        System.out.println(row);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SELECT * FROM " + tableName + " fehlgeschlagen", e);
    }
  }

  // 4) UPDATE: Gehalt in Stufe prozentual erhöhen (Spalte 'betrag')
  /**
   * Erhöht den Betrag aller Gehälter einer Gehaltsstufe um {@code percent} %.
   * @param con      offene Verbindung
   * @param percent  z. B. 10 für +10 %
   * @param gehStufe Gehaltsstufe (z. B. it1)
   * @return betroffene Zeilen
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

  // 5) DELETE: Mitarbeiter per Name löschen
  /**
   * Löscht alle Datensätze aus {@code personal}, deren {@code name} exakt passt.
   * @param con  offene Verbindung
   * @param name Nachname
   * @return gelöschte Zeilen
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

  // 6) Abfrage „Verkauf“
  /**
   * Liefert alle Mitarbeiter aus der Abteilung „Verkauf“.
   * @param con offene Verbindung
   * @return Liste im Format {@code "pnr - name, vorname"}
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

  // ==== Helper für Migration (Schema-Checks) =================================

  private static boolean columnExists(Connection con, String schema, String table, String column) throws SQLException {
    String q = "SELECT COUNT(*) FROM information_schema.COLUMNS " +
               "WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?";
    try (PreparedStatement ps = con.prepareStatement(q)) {
      ps.setString(1, schema);
      ps.setString(2, table);
      ps.setString(3, column);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next(); return rs.getInt(1) > 0;
      }
    }
  }

  private static boolean fkExists(Connection con, String schema, String table, String fkName) throws SQLException {
    String q = "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS " +
               "WHERE CONSTRAINT_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME=? AND CONSTRAINT_TYPE='FOREIGN KEY'";
    try (PreparedStatement ps = con.prepareStatement(q)) {
      ps.setString(1, schema);
      ps.setString(2, table);
      ps.setString(3, fkName);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next(); return rs.getInt(1) > 0;
      }
    }
  }

  // 7) Migration: Krankenkasse-Kürzel -> separate Tabelle + FK (in-place & idempotent)
  /**
   * Migriert die Krankenkassen-Kürzel in eine Stammtabelle {@code krankenversicherung}
   * und ersetzt {@code personal.krankenkasse} durch {@code personal.kkid} inkl. FK.
   * – Idempotent und ohne Tabellen-Umbenennung.
   */
  public static void migrateKrankenkasse(Connection con) {
    final String schema = "firma"; // ggf. anpassen, falls DB anders heißt
    try {
      con.setAutoCommit(false);

      // (0) evtl. alte Swap-Tabelle entfernen
      try (Statement st = con.createStatement()) {
        st.execute("DROP TABLE IF EXISTS personal_alt");
      }

      // (1) Stammtabelle anlegen/füllen
      try (Statement st = con.createStatement()) {
        st.execute(
          "CREATE TABLE IF NOT EXISTS krankenversicherung(" +
          "  kkid INT PRIMARY KEY," +
          "  kuerzel CHAR(3) UNIQUE NOT NULL," +
          "  name VARCHAR(100) NOT NULL)"
        );
      }
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

      // (2) kkid-Spalte in personal anlegen (falls fehlt) + Index
      if (!columnExists(con, schema, "personal", "kkid")) {
        try (Statement st = con.createStatement()) {
          st.execute("ALTER TABLE personal ADD COLUMN kkid INT NULL");
          st.execute("CREATE INDEX idx_personal_kkid ON personal(kkid)");
        }
      }

      // (3) Kürzel -> kkid mappen (nur leere kkid)
      try (Statement st = con.createStatement()) {
        st.execute(
          "UPDATE personal p " +
          "LEFT JOIN krankenversicherung kv ON kv.kuerzel = p.krankenkasse " +
          "SET p.kkid = kv.kkid " +
          "WHERE p.kkid IS NULL"
        );
      }

      // (4) alte Spalte entfernen, wenn vorhanden
      if (columnExists(con, schema, "personal", "krankenkasse")) {
        try (Statement st = con.createStatement()) {
          st.execute("ALTER TABLE personal DROP COLUMN krankenkasse");
        }
      }

      // (5) FK setzen (einmalig)
      if (!fkExists(con, schema, "personal", "fk_personal_kk")) {
        try (Statement st = con.createStatement()) {
          st.execute("ALTER TABLE personal " +
                     "ADD CONSTRAINT fk_personal_kk FOREIGN KEY (kkid) " +
                     "REFERENCES krankenversicherung(kkid)");
        }
      }

      // (6) Optional: kkid NOT NULL, wenn vollständig gemappt
      try (Statement st = con.createStatement();
           ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM personal WHERE kkid IS NULL")) {
        rs.next();
        if (rs.getInt(1) == 0) {
          try (Statement st2 = con.createStatement()) {
            st2.execute("ALTER TABLE personal MODIFY kkid INT NOT NULL");
          }
        } else {
          System.out.println("Hinweis: Es gibt noch Datensätze ohne kkid – Spalte bleibt NULL-able.");
        }
      }

      con.commit();
      con.setAutoCommit(true);
      System.out.println("Migration (in-place) erfolgreich abgeschlossen.");
    } catch (SQLException e) {
      try { con.rollback(); } catch (SQLException ignored) {}
      throw new RuntimeException("Migration fehlgeschlagen", e);
    }
  }

  // === Demo-Main ===
  public static void main(String[] args) {
    String url  = "jdbc:mariadb://localhost:3306/firma";
    String user = "root";
    String pass = "password";

    try (Connection con = login(url, user, pass)) { // login

      // CREATE / UPSERT
      addDataPersonal(con, 417, "Krause", "Henrik", "it1", "d15", "tkk"); // addDataPersonal

      System.out.println("== personal (vor Änderungen) ==");
      showAll(con, "personal"); // showAll

      int upd = raiseSalaryPercent(con, 10, "it1"); // raiseSalaryPercent
      System.out.println("Gehaltsstufe it1 angepasst: " + upd + " Zeile(n).");

      int del = deletePersonalByName(con, "Tietze"); // deletePersonalByName
      System.out.println("Gelöscht: " + del + " Zeile(n) für 'Tietze'.");

      System.out.println("MitarbeiterInnen in Abteilung 'Verkauf':");
      for (String s : employeesInVerkauf(con)) System.out.println("  " + s); // employeesInVerkauf

      migrateKrankenkasse(con); // migrateKrankenkasse

      System.out.println("== personal (nach Migration) ==");
      showAll(con, "personal"); // showAll
      System.out.println("== krankenversicherung ==");
      showAll(con, "krankenversicherung"); // showAll

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
