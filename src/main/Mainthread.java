package main;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import javax.crypto.spec.RC2ParameterSpec;
import javax.swing.JOptionPane;

import org.bson.Document;


import java.util.logging.*;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.session.ClientSession;

import log.ThreadLog;
import mov.ThreadMov;
import temp.ThreadTemp;

public class Mainthread {

    public static final double DADOS_SEGUNDO = 1;

    private String URL;
    private String USER;
    private String PASSWORD;
    private List<Thread> workers;

    public Connection sqlConn;
    public VarSet globalVars;

    private MongoClient mongoClient;
    private MongoDatabase db;
    private String mongo_user;
    private String mongo_password;
    private String mongo_address;
    private String mongo_replica;
    private String mongo_database;
    private String mongo_authentication;

    private static String mongo_mazemove_collection;
    private static String mongo_mazemanage_collection;
    private static String mongo_mazetemp_collection;
    private static String mongo_mazelog_collection;

    private MongoCollection<Document> getMoveCol;
    private MongoCollection<Document> getManageCol;
    private MongoCollection<Document> getTempCol;
    private MongoCollection<Document> getLogCol;

    public Mainthread() {
        globalVars = new VarSet();

        try {
            loadSQLServerProperties();
            loadMongoProperties();
        } catch (FileNotFoundException e) {
            JOptionPane.showMessageDialog(null, "The sqlserver.properties file wasn't found:\n" + e, "Data Migration",
                    JOptionPane.ERROR_MESSAGE);
            // e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            JOptionPane.showMessageDialog(null, "Error loading file sqlserver.properties:\n" + e, "Data Migration",
                    JOptionPane.ERROR_MESSAGE);
            System.exit(1);
            // e.printStackTrace();
        }
    }

    // TODO: Definir transaction para SQL para usar nos outros sitios
    // lambdaOperations é uma função lambda
    public void sqlTransaction(Runnable lambdaOperations) {
        lambdaOperations.run();
        return;
    }


    // TODO: Definir transaction para Mongo para usar nos outros sitios
    // lambdaOperations é uma função lambda
    public void mongoTransaction(Runnable lambdaOperations) {
        lambdaOperations.run();
        return;
    }

    private void loadMongoProperties() {
        //Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
        try {
            Properties p = new Properties();
            //System.out.println(System.getProperty("user.dir"));
            p.load(new FileInputStream("lib/MongoProperties.properties"));
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_mazemove_collection = p.getProperty("mongo_move_collection");
            mongo_mazemanage_collection = p.getProperty("mongo_maze_collection");
            mongo_mazetemp_collection = p.getProperty("mongo_temp_collection");
            mongo_mazelog_collection = p.getProperty("mongo_error_collection");
        } catch (Exception e) {
            System.out.println("Error reading CloudToMongo.ini file " + e);
        }
    }

    private void connectMongo() {
        Main.documentLabel.append("Connecting to Mongo...\n");
        String mongoURI = new String();
        mongoURI = "mongodb://";
        if (mongo_authentication.equals("true"))
            mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true"))
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica + "&authSource=mqttData";
            else
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else if (mongo_authentication.equals("true"))
            mongoURI = mongoURI + "/?authSource=mqttData";
        MongoClient mongoClient = MongoClients.create(mongoURI);
        Main.documentLabel.append("Mongo Connected.\n");
        db = mongoClient.getDatabase(mongo_database);
        getMoveCol = db.getCollection(mongo_mazemove_collection);
        getManageCol = db.getCollection(mongo_mazemanage_collection);
        getTempCol = db.getCollection(mongo_mazetemp_collection);
        getLogCol = db.getCollection(mongo_mazelog_collection);
        getLogCol.insertOne(new Document("teste",2));
    }

    // sql_connector_type = "jdbc:mariadb"
    // sql_host = "localhost"
    // sql_db = micelab
    // sql_port = "3306"
    // sql_user = "java"
    // sql_user_password = "B@A+xg8NY("
    private void loadSQLServerProperties() throws FileNotFoundException, IOException {
        Properties p = new Properties();

        // System.out.println(path);
        p.load(new FileInputStream("lib/sqlserver.properties"));

        String c_type = p.getProperty("sql_connector_type");
        String host = p.getProperty("sql_host");
        String port = p.getProperty("sql_port");
        String db = p.getProperty("sql_db");
        URL = c_type + "://" + host + ":" + port + "/" + db;
        // System.out.println(URL);

        USER = p.getProperty("sql_user");
        PASSWORD = p.getProperty("sql_user_password");
        // System.out.println(USER + ":" + PASSWORD);
    }

    private void connectSQL() {
        try {
            Main.documentLabel.append("Connecting to MariaDB...\n");
            sqlConn = DriverManager.getConnection(URL, USER, PASSWORD);
            Main.documentLabel.append("MariaDB Connected.\n");
        } catch (SQLException e) {
            Main.documentLabel.append(e.toString() + "\n");
        }

    }

    public void connectDbs() {
        connectMongo();
        connectSQL();
        // TODO: Lançar as 4 threads
        //ThreadMov mov = new ThreadMov(getMoveCol, getManageCol, sqlConn, this);
        //mov.start();
        //ThreadTemp temp = new ThreadTemp(getTempCol, getManageCol, sqlConn, this);
        //temp.start();
        
    }

    public void closeConnections() {
        // TODO: Terminar as 4 threads em [workers]
        // System.out.println("Threads terminadas");
        String output = "Connections closed.";
        try {
            if (sqlConn != null) {
                if (!sqlConn.isClosed()) {
                    sqlConn.close();
                }
            }
            if (mongoClient != null)
                mongoClient.close();
            Main.documentLabel.append(output + "\n");
        } catch (SQLException e) {
            Main.documentLabel.append(e + "\n");
        }

    }

}
