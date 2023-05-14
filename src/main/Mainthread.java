package main;

import com.mongodb.MongoException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.*;
import log.ThreadLog;
import main.*;
import org.bson.Document;
import org.mariadb.jdbc.MariaDbDataSource;
import javaop.ThreadJavaOp;
import mov.ThreadMov;
import temp.ThreadTemp;
import javax.swing.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Mainthread extends Thread {

    public static final double DADOS_SEGUNDO = 1;

    private String URL;
    private String USER;
    private String PASSWORD;
    private List<Thread> workers;

    public Connection sqlConn;
    public VarSet globalVars;

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
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

    public MongoCollection<Document> getMoveCol;
    public MongoCollection<Document> getManageCol;
    public MongoCollection<Document> getTempCol;
    public MongoCollection<Document> getLogCol;
    public MariaDbDataSource dataSource;


    public Mainthread() {
        globalVars = new VarSet();
        workers = new ArrayList<>();

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
        }
    }


    private void loadMongoProperties() {
        //Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
        try {
            Properties p = new Properties();
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

    private void loadSQLServerProperties() throws IOException {
        Properties p = new Properties();
        p.load(new FileInputStream("lib/sqlserver.properties"));
        String c_type = p.getProperty("sql_connector_type");
        String host = p.getProperty("sql_host");
        String port = p.getProperty("sql_port");
        String db = p.getProperty("sql_db");
        URL = c_type + "://" + host + ":" + port + "/" + db;
        USER = p.getProperty("sql_user");
        PASSWORD = p.getProperty("sql_user_password");

    }

    private void connectMongo() throws MongoTimeoutException, MongoSocketReadException, MongoSocketOpenException {
        Main.documentLabel.append("Connecting to Mongo...\n");
        String mongoURI;
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
        mongoClient = MongoClients.create(mongoURI);
        Main.documentLabel.append("Mongo Connected.\n");
        mongoDatabase = mongoClient.getDatabase(mongo_database);
        getMoveCol = mongoDatabase.getCollection(mongo_mazemove_collection);
        getManageCol = mongoDatabase.getCollection(mongo_mazemanage_collection);
        getTempCol = mongoDatabase.getCollection(mongo_mazetemp_collection);
        getLogCol = mongoDatabase.getCollection(mongo_mazelog_collection);
    }


    // sql_connector_type = "jdbc:mariadb"
    // sql_host = "localhost"
    // sql_db = micelab
    // sql_port = "3306"
    // sql_user = "java"
    // sql_user_password = "B@A+xg8NY("
    private void connectSQL() throws SQLException {
            dataSource = new MariaDbDataSource(URL);
            dataSource.setUser(USER);
            dataSource.setPassword(PASSWORD);
            Main.documentLabel.append("Connecting to MariaDB...\n");
            sqlConn = dataSource.getConnection();
            Main.documentLabel.append("MariaDB Connected.\n");
    }

    //TODO: alguem tem de sinalizar o começo em caso de down
    public void tryConnect() throws InterruptedException{
        boolean dontGo = true;
        try{
            if(!(mongoClient==null || sqlConn==null)) {
                mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
                sqlConn.createStatement().executeQuery("SELECT 1");
                dontGo = false;
            }
        } catch (MongoException | SQLException e) {
            Main.documentLabel.append("Mainthread: Ligação em baixo, a tentar reconectar.\n");
        }

        while (dontGo) {
            try {
                connectSQL();
                connectMongo();
                mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
            } catch (MongoException | SQLException me) {
                Main.documentLabel.append("Mainthread: Failed to establish connections. Trying again soon...\n");
                sleep(1000);
            }
            dontGo = false;
        }


    }

    public ClientSession getMongoSession() {
        return mongoClient.startSession();
    }


    public Connection getConnectionSql() throws SQLException, InterruptedException {
        while (sqlConn!= null && !sqlConn.isValid(10)) dataSource.wait();
        return dataSource.getConnection();
    }


    @Override
    public void run() {
        try {
            tryConnect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        ThreadMov tm = new ThreadMov();
        ThreadTemp tt = new ThreadTemp();
        ThreadLog tl = new ThreadLog();
        ThreadJavaOp tj = new ThreadJavaOp();

        workers.addAll(new ArrayList<>(Arrays.asList(tm, tj, tt, tl)));
        workers.forEach((t) -> t.start());
        Main.documentLabel.append("MainThread: Threads lançadas.\n");

        while (true) {
            try {
                sleep(1000);
                tryConnect();
            } catch (InterruptedException e) {
                Main.documentLabel.append("Mainthread: Acordada! Tentar ligar.\n");
            }
        }

    }

    public void closeConnections() {
        for (Thread t : workers) t.interrupt();
        System.out.println("Threads terminadas\n");
        String output = "Connections closed.\n";
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

    private void failedToConnectTo(String db, Exception e) {
        //Main.documentLabel.append("Failed to connect to" + db + ":\n" + e + "\nTrying again in:");
        for (int i = 3; i > 0; i--) {
            Main.documentLabel.append(i + "... ");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Main.documentLabel.append(e.toString());
                System.exit(1);
            }
        }
        //Main.documentLabel.append("\n");
    }

}
