package log;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import main.Main;
import main.Mainthread;
import main.VarSet;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;

public class ThreadLog extends Thread {

    private MongoCollection logCol;
    private Connection sqlConn;
    private MongoCollection mazeManageCol;
    private Connection conn;
    private Mainthread mainThread;
    private long periodicidade = 0;

    public ThreadLog() {
        this.logCol = Main.mt.getLogCol;
        this.mazeManageCol = Main.mt.getManageCol;
    }


    private void initConn() throws InterruptedException {
        boolean flag = true;
        while (flag) {
            try {
                sqlConn = Main.mt.getConnectionSql();
                this.logCol = Main.mt.getManageCol;
                this.mazeManageCol = Main.mt.getManageCol;
                mazeManageCol.find(new Document("idExp", -1));
                Main.documentLabel.append("ThreadLog: Ligação estabelecida.\n");
                flag = false;
            } catch (SQLException | MongoException e) {
                Main.documentLabel.append("ThreadLog: Sem ligação.");
                sleep(1000);
            }
        }
    }

    private void initConn(Exception e) throws InterruptedException {
        boolean flag = true;
        while (flag) {
            System.out.println("---------------------------------ENTROU INITCONN--------------------------------");
            try {
                if(e instanceof MongoException){
                    this.logCol = Main.mt.getManageCol;
                    this.mazeManageCol = Main.mt.getManageCol;
                    mazeManageCol.find(new Document("idExp", -1));
                }
                else if(e instanceof SQLException) {
                    System.out.println("---------------------------------ENTROU SQL--------------------------------");
                    sqlConn = Main.mt.getConnectionSql();
                }
                flag = false;
                Main.documentLabel.append("ThreadLog: Ligação Estabelecida.\n");
            } catch (SQLException | MongoException e2) {
                System.out.println("---------------------------------NÃO CONSEGUIU--------------------------------");
                sleep(1000);
            }
        }
    }



    public void run() {
        try {
            initConn();
        } catch (InterruptedException e) {
            Main.documentLabel.append("ThreadLog: Interrompida, a terminar.\n");
            return;
        }
        while (true) {
            try {
                doRegularWork();
            } catch (InterruptedException ie) {
                Main.documentLabel.append("ThreadLog: Interrompida, a terminar.\n");
                return;
            } catch (MongoException | SQLException e){
                try{
                    sleep(1000);
                    Main.documentLabel.append("ThreadLog: Ligação perdida. A tentar reconectar.\n");
                    initConn(e);
                } catch (InterruptedException ie){
                    return;
                }
            }
            }
        }


    private void doRegularWork() throws InterruptedException, MongoException, SQLException {
        if(Main.mt.globalVars.isPopulated()) {
            VarSet.Vars vars = Main.mt.globalVars.getVars();
            FindIterable<Document> mazeManageIterDoc = mazeManageCol.find(Filters.eq("numExp", -1));
            FindIterable<Document> logIterDoc;
            if (mazeManageIterDoc.first() == null) {
                logIterDoc = logCol.find().sort(ascending("Hora", "_id"));
            } else {
                DBObject lastLogObject = BasicDBObject.parse(mazeManageIterDoc.first().getString("lastLog"));
                Document aux = new Document();
                aux.append("_id", new ObjectId(lastLogObject.get("_id").toString()));

                logIterDoc = logCol.find(
                                Filters.and(Filters.gte("Hora", lastLogObject.get("Hora")), Filters.gt("_id", aux.get("_id"))))
                        .sort(ascending("Hora", "_id"));
            }
            List<String> toSql = createSqlCommandsFromLogList(logIterDoc);

            closeDeal(logIterDoc, toSql);
            sleep(vars.getPeriodicidade());
        } else {
            sleep(1000);
        }
    }

    private void closeDeal(FindIterable<Document> logIterDoc, List<String> toSql) throws SQLException {

        sqlConn.setAutoCommit(false);
        ClientSession session = Main.mt.getMongoSession();
        session.startTransaction();

        try {
            List<PreparedStatement> lps = new ArrayList<>();
            for (String s : toSql) lps.add(sqlConn.prepareStatement(s));
            for (PreparedStatement pst : lps) pst.execute();


            for (String s : toSql) lps.add(conn.prepareStatement(s));
            if (toSql.size() != 0) {
                Document lastLogDocument = logIterDoc.skip(toSql.size() - 1).first();
                String lastLogString = "{_id:\"" + lastLogDocument.get("_id") + "\", Hora: \""
                        + lastLogDocument.get("Hora") + "\"}";
                mazeManageCol.updateOne(Filters.eq("numExp", -1),
                        Updates.set("lastLog", lastLogString), new UpdateOptions().upsert(true));
            }

            for (PreparedStatement ps : lps) ps.execute();
            sqlConn.commit();
            session.commitTransaction();

        } catch (Exception e) {
            if (sqlConn != null) sqlConn.rollback();
            if (session != null) session.abortTransaction();
        } finally {
            if (sqlConn != null) sqlConn.setAutoCommit(true);
            if (session != null) session.close();
        }

    }

    public List<String> createSqlCommandsFromLogList(FindIterable<org.bson.Document> logIterDoc) {
        List<String> toSql = new ArrayList<String>();
        for (org.bson.Document doc : logIterDoc) {
            toSql.add("INSERT INTO log(DataHora, Tipo, Valor) VALUES(\"" + doc.get("Hora")
                    + "\", \"Dado corrompido\",\"" + doc.get("Message") + "\")");
        }
        return toSql;
    }

}
