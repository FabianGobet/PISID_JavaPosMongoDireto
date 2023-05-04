package log;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;


import main.Mainthread;
import main.VarSet;

import java.sql.*;

import static com.mongodb.client.model.Sorts.ascending;

public class ThreadLog extends Thread {

    private MongoCollection logCol;
    private MongoCollection mazeManageCol;
    private Connection conn;
    private Mainthread mainThread;
    private long periodicidade = 0;

    public ThreadLog(MongoCollection logCol, MongoCollection mazeManageCol, Connection conn, Mainthread mainThread) {
        this.logCol = logCol;
        this.mazeManageCol = mazeManageCol;
        this.conn = conn;
        this.mainThread = mainThread;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(periodicidade);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // getLocalVariables();
            FindIterable<org.bson.Document> mazeManageIterDoc = mazeManageCol.find(Filters.eq("numExp", -1));

            FindIterable<org.bson.Document> logIterDoc;
            if (mazeManageIterDoc.first() == null) {
                logIterDoc = logCol.find().sort(ascending("Hora", "_id"));
            } else {
                DBObject lastLogObject = BasicDBObject.parse(mazeManageIterDoc.first().getString("lastLog"));
                org.bson.Document aux = new org.bson.Document();
                aux.append("_id", new ObjectId(lastLogObject.get("_id").toString()));

                logIterDoc = logCol.find(
                        Filters.and(Filters.gte("Hora", lastLogObject.get("Hora")), Filters.gt("_id", aux.get("_id"))))
                        .sort(ascending("Hora", "_id"));
            }
            /*
            for (org.bson.Document doc : logIterDoc) {
                System.out.println(doc);
            }*/

            List<String> toSql = createSqlCommandsFromLogList(logIterDoc);
            /*
            for (String a : toSql) {
                System.out.println(a);
            }
            */
            mainThread.sqlTransaction(() -> {
                mainThread.mongoTransaction(() -> {
                    if (toSql.size() != 0) {
                        org.bson.Document lastLogDocument = logIterDoc.skip(toSql.size() - 1).first();
                        String lastLogString = "{_id:\"" + lastLogDocument.get("_id") + "\", Hora: \""
                                + lastLogDocument.get("Hora") + "\"}";
                        //System.out.println(lastLogString);
                        mazeManageCol.updateOne(Filters.eq("numExp", -1),
                                Updates.set("lastLog", lastLogString), new UpdateOptions().upsert(true));
                    }
                });
                executeSqlCommands(toSql);
            });

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

    public void getLocalVariables() {
        try {
            VarSet varSet = mainThread.globalVars;
            while (!varSet.isPopulated())
                Thread.sleep(1000);
            VarSet.Vars vars = varSet.getVars();
            periodicidade = calcPeriodicidade(vars.getTempo_max_periodicidade(),
                    vars.getSegundos_abertura_portas_exterior(), vars.getTempo_entre_experiencia(),
                    vars.getFator_periodicidade().longValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public long calcPeriodicidade(int segMaxPer, int segAbrirPortas, int segMaxTempExp, long factorPer) {
        return segMaxPer - (segMaxPer / ((segAbrirPortas / segMaxTempExp) * factorPer + 1));
    }

    public void executeSqlCommands(List<String> commands) {
        for (String cmd : commands) {
            try {
                PreparedStatement ps = conn.prepareStatement(cmd);
                ps.execute();
               // System.out.println("executed");
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }

}
