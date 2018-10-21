package at.htl.vehicle;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MovieTest {

    public static final String  DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    static final String USER = "app";
    static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJDBC(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n" + e.getMessage() + "\n");
            System.exit(1);
        }

        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE movie (" +
                    "id INT CONSTRAINT movie_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL," +
                    "genre VARCHAR(255) NOT NULL" +
                    ")";

            stmt.execute(sql);
            sql = "INSERT INTO movie (id, name, genre) VALUES (1, 'Die Verurteilten', 'Drama')";
            stmt.execute(sql);
            sql = "INSERT INTO movie (id, name, genre) VALUES (2, 'Der Pate', 'Crime, Drama')";
            stmt.execute(sql);
            sql = "INSERT INTO movie (id, name, genre) VALUES (3, 'Schindlers Liste', 'Biography, Drama, History')";
            stmt.execute(sql);

            System.out.println("Tabelle Movie erstellt und befüllt!");

            sql = "CREATE TABLE rating (" +
                    "id INT CONSTRAINT rating_pk PRIMARY KEY," +
                    "score DOUBLE NOT NULL," +
                    "movie_id INT NOT NULL CONSTRAINT movie_id_fk REFERENCES movie(id)" +
                    ")";

            stmt.execute(sql);
            sql = "INSERT INTO rating (id, score, movie_id) VALUES (1, 9.2, 1)";
            stmt.execute(sql);
            sql = "INSERT INTO rating (id, score, movie_id) VALUES (2, 8.9, 3)";
            stmt.execute(sql);
            sql = "INSERT INTO rating (id, score, movie_id) VALUES (3, 9.2, 2)";
            stmt.execute(sql);

            System.out.println("Tabelle Rating erstellt und befüllt!");

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @AfterClass
    public static void teardownJDBC(){

        //Rating Tabelle muss wegen dem FK zuerst gelöscht werden
        try{
            conn.createStatement().execute("DROP TABLE rating");
            System.out.println("Tabelle RATING geöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle RATING konnte nicht gelöscht werden:\n"
                    + e.getMessage());
        }

        try{
            conn.createStatement().execute("DROP TABLE movie");
            System.out.println("Tabelle MOVIE geöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle MOVIE konnte nicht gelöscht werden:\n"
                    + e.getMessage());
        }

        try {
            if(conn != null && !conn.isClosed()){
                conn.close();
                System.out.println("Good bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMovie() {

        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, name, genre FROM movie");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("NAME"),is("Die Verurteilten"));
            assertThat(rs.getString("GENRE"),is("Drama"));
            rs.next();
            assertThat(rs.getString("NAME"),is("Der Pate"));
            assertThat(rs.getString("GENRE"),is("Crime, Drama"));
            rs.next();
            assertThat(rs.getString("NAME"),is("Schindlers Liste"));
            assertThat(rs.getString("GENRE"),is("Biography, Drama, History"));
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testRating() {

        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, score, movie_id FROM rating");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getDouble("SCORE"),is(9.2));
            assertThat(rs.getInt("MOVIE_ID"),is(1));
            rs.next();
            assertThat(rs.getDouble("SCORE"),is(8.9));
            assertThat(rs.getInt("MOVIE_ID"),is(3));
            rs.next();
            assertThat(rs.getDouble("SCORE"),is(9.2));
            assertThat(rs.getInt("MOVIE_ID"),is(2));
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testMetaDataRating() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "RATING";
            String columnNamePattern = null;

            ResultSet rs = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            rs.next();
            String columnName = rs.getString(4);
            int columnType = rs.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("SCORE"));
            assertThat(columnType, is(Types.DOUBLE));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("MOVIE_ID"));
            assertThat(columnType, is(Types.INTEGER));


            String schema = null;
            String tableName = "RATING";

            rs = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);

            rs.next();
            columnName = rs.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataMovie() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "MOVIE";
            String columnNamePattern = null;

            ResultSet rs = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            rs.next();
            String columnName = rs.getString(4);
            int columnType = rs.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("NAME"));
            assertThat(columnType, is(Types.VARCHAR));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("GENRE"));
            assertThat(columnType, is(Types.VARCHAR));


            String schema = null;
            String tableName = "MOVIE";

            rs = databaseMetaData.getPrimaryKeys(
                    catalog, schema, tableName);

            rs.next();
            columnName = rs.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
