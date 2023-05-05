package javaop;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Aggregates.*;
import static java.util.Arrays.asList;
import org.bson.*;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import com.mongodb.*;
import com.mongodb.client.*;

public class TestMongo {

    private static MongoDatabase db;
    private static String mongo_user;
    private static String mongo_password;
    private static String mongo_address;
    private static String mongo_replica;
    private static String mongo_database;
    private static String mongo_authentication;

    private static String mongo_mazemove_collection;
    private static String mongo_mazemanage_collection;
    private static String mongo_mazetemp_collection;
    private static String mongo_mazelog_collection;

    private static MongoCollection<Document> getMoveCol;
    private static MongoCollection<Document> getManageCol;
    private static MongoCollection<Document> getTempCol;
    private static MongoCollection<Document> getLogCol;

    private static void loadMongoProperties() {
        Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
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

    public static void connectMongo() {
        loadMongoProperties();
        String mongoURI = new String();
        mongoURI = "mongodb://";
        if (mongo_authentication.equals("true"))
            mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true"))
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica + "&authSource=admin";
            else
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else if (mongo_authentication.equals("true"))
            mongoURI = mongoURI + "/?authSource=admin";
        MongoClient mongoClient = MongoClients.create(mongoURI);
        db = mongoClient.getDatabase(mongo_database);

        test();

    }

    private static void test() {
        // getMoveCol = db.getCollection(mongo_mazemove_collection);
        // getManageCol = db.getCollection(mongo_mazemanage_collection);
        // getTempCol = db.getCollection(mongo_mazetemp_collection);
        // getLogCol = db.getCollection(mongo_mazelog_collection);
        // BigDecimal bdec = new BigDecimal(1.2653232);
        // Document doc = new Document("idExp", 2)
        // .append("numExp", 3)
        // .append("outlierSampleSize", bdec.intValue());
        // getManageCol.insertOne(doc);

    }

    public static void main(String[] args) {
        connectMongo();
    }
}

/* ---------------------------TESTS----------------------------- */

// String time = "2023-04-30 13:06:06.000591";
// Document matchStage = new Document("$match", new Document("Hora", new
// Document("$gt", time))); // elementos com hora > time
// Document groupStage = new Document("$group", new Document("_id","$numExp"));
// // criar grupos com numExp
// Document sortStage = new Document("$sort", new Document("_id", 1)); //ordenar
// os grupos por numExp de forma crescente (1)
// Document projectStage = new Document("$project", new
// Document("_id",0).append("numExp", "$_id")); // não mostrar "_id" mas mostrar
// "numExp":valor
// Document limitStage = new Document("$limit",2); // limitar aos dois primeiros
// outputs
// List<Document> pipeline =
// Arrays.asList(matchStage,groupStage,sortStage,projectStage,limitStage);
// //construcao da query
// AggregateIterable<Document> result = getTempCol.aggregate(pipeline); //exec
// query em iteravel

// for(Document doc : result)
// System.out.println(doc);

// Document query = new Document("SalaEntrada", new Document("$eq",
// 1)).append("SalaSaida", new Document("$eq", 12));
// try {
// MongoCursor<Document> cursor = getMoveCol.find(query).iterator();
// System.out.println(cursor.hasNext());
// while (cursor.hasNext()) {
// Document doc = cursor.next();
// System.out.println(doc);
// }
// } catch (Exception e) {
// System.out.println(e);
// }

/*
 * Document query = new Document("idExp", new Document("$eq",
 * 0));
 * try {
 * MongoCursor<Document> cursor = getManageCol.find(query).iterator();
 * System.out.println(cursor.hasNext());
 * while (cursor.hasNext()) {
 * Document doc = cursor.next();
 * System.out.println(doc.get("idExp"));
 * System.out.println(doc.containsKey("numExp"));
 * }
 * } catch (Exception e) {
 * System.out.println(e);
 * }
 */