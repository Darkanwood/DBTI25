package fhwedel.JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Hello {

    /**
     * Stellt Verbindung zum laufenden MariaDB-datenserver her
     * @return Laufende Connection mit gegebener url
     */
    public static Connection login(String url, String userName, String password){
        assert url != null;
        assert userName != null;
        assert password != null;
        
        Connection con = null;

        try{
            con = DriverManager.getConnection(url, userName, password);
            System.out.println("Connectin Established successfully");
        } catch (SQLException e){
            System.out.println("Connection Failed");
            e.printStackTrace();
        }

        return con;
    }
    
    /**
     * Fügt einen neuen Datensatz in die Tabelle personal ein
     * @param con Bestehende Verbindung zur Datenbank
     * @param pnr Personalnummer
     * @param name Nachnahme
     * @param vorname Vorname
     * @param geh_stufe Gehaltstuffe
     * @param abt_nr Abteilungsnummer
     * @param krankenkasse Krankenkasse
     */
    public static void addDataPersonal(Connection con,
                                Integer pnr,
                                String name,
                                String vorname,
                                String geh_stufe,
                                String abt_nr,
                                String krankenkasse){
    assert pnr < 0;
    assert pnr > 9999;
    assert name != null;
    assert name.length() > 20;
    assert vorname.length() > 20;
    assert geh_stufe.length() > 4;
    assert abt_nr.length() > 3;
    assert krankenkasse.length() > 3;
    
        try{
            con.setAutoCommit(false);

            String sql = "Insert Into personal pnr, name, vorname, geh_stufe, abt_nr, krankenkasse (?,?,?,?,?,?)";
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.setInt(1, pnr);
            stmt.setString(2, name);
            stmt.setString(3, vorname);
            stmt.setString(4, geh_stufe);
            stmt.setString(5, abt_nr);
            stmt.setString(6, krankenkasse);

            int row = stmt.executeUpdate();
            con.commit();
            System.out.println(row + " Zeile(n) eingefügt");

        } catch (SQLException e){
            try{
                System.out.println("Failure detected, rollback ist running");
                con.rollback();
            } catch (SQLException rollbackException){
                System.out.println("Rollback failured");
                rollbackException.printStackTrace();
            }
        }
    }


    public static ResultSet showAllDataFromTable(Connection con, String tableName){
        assert con != null;
        assert tableName != null;

        ResultSet rs = null;

        try{
        String sql = "SELECT * FROM ?";
        PreparedStatement stmt = con.prepareStatement(sql);
        stmt.setString(1, tableName);
        rs = stmt.executeQuery();    
        System.out.println("Data successful loaded");    

        } catch (SQLException e){
            System.out.println("Loading failed");
            e.getSQLState();
        }


        return rs;
    }



    public static void main(String[] args) throws SQLException {
    String url = "jdbc:mariadb://localhost:3306/firma";
    String password = "password";
    String userName = "root";
    Connection con = null;

    con = login(url, userName, password);
    
    /*
    Integer pnr = 417;
    String name = "Krause";
    String vorname = "Henrik";
    String geh_stufe = "it1";
    String abt_nr = "";
    String krankenkasse = "tkk";
    addDataPersonal(con, null, userName, userName, url, password, userName);
    }
    */
    String tablename = "personal";    
    ResultSet rs = showAllDataFromTable(con, tablename);
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();

    while(rs.next()){
        for(int i=0; i <= columnCount; i++){
            String columnName = meta.getColumnName(i);
            String columnValue = rs.getString(i);
            System.out.println(columnName + ": " + columnValue + "\t");
        }
        System.out.println();
    }
}
}
