package mov;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import main.Main;
import main.VarSet;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;

public class ThreadMov extends Thread {

    private MongoCollection moveCol;
    private MongoCollection mazeManageCol;
    private Connection sqlConn;


    private long periodicidade = 0;
    private int idExperience;
    private Date DataHoraFim;

    public ThreadMov(MongoCollection moveCol, MongoCollection mazeManageCol) {
        this.moveCol = moveCol;
        this.mazeManageCol = mazeManageCol;

    }

    private void initConn() throws InterruptedException {
        boolean flag = true;
        while (flag) {
            try {
                sqlConn = Main.mt.getConnectionSql();
                mazeManageCol.find(new Document("idExp", -1));
                flag = false;
            } catch (SQLException | MongoException e) {
                Main.documentLabel.append("ThreadLog waiting for connections...");
                sleep(1000);
            }
        }
    }


    public void run() {
        try {
            initConn();
        } catch (InterruptedException e) {
            Main.documentLabel.append("ThreadLog interrupted (code 0), a terminar....\n");
            return;
        }
        while (true) {
            try {
                doRegularWork();
            } catch (InterruptedException ie) {
                Main.documentLabel.append("ThreadMov interrupted (code 0), a terminar....\n");
                Main.documentLabel.append(ie + "\n");
                break;
            } catch (MongoTimeoutException | MongoSocketReadException | MongoSocketOpenException | SQLException e) {
                if(Main.mt.getState().equals(State.TIMED_WAITING)) Main.mt.interrupt();
                try {
                    sleep(1000);
                } catch (InterruptedException ex) {
                    Main.documentLabel.append("ThreadLog interrupted (code 0), a terminar....\n");
                    return;
                }
            }
        }
    }

    private void doRegularWork() throws InterruptedException, MongoTimeoutException, MongoSocketReadException, MongoSocketOpenException, SQLException {
        if(Main.mt.globalVars.isPopulated()) {
            System.out.println(Main.mt.globalVars.isPopulated());
            getLocalVariables();
            FindIterable<Document> mazeManageIterDoc = mazeManageCol.find(Filters.eq("idExp", idExperience));
            int numExp = mazeManageIterDoc.first().getInteger("numExp");
            FindIterable<Document> movIterDoc;
            int docSize;
            if (mazeManageIterDoc.first().getString("lastMov") == null) {
                movIterDoc = moveCol.find(Filters.eq("numExp", numExp)).sort(ascending("Hora", "_id"));
                docSize = (int) moveCol.count(Filters.eq("numExp", numExp));
            } else {
                DBObject lastMovObject = BasicDBObject.parse(mazeManageIterDoc.first().getString("lastMov"));
                Document aux = new Document();
                aux.append("_id", new ObjectId(lastMovObject.get("_id").toString()));
                docSize = (int) moveCol.count(Filters.and(Filters.eq("numExp", numExp),
                        Filters.gte("Hora", lastMovObject.get("Hora")), Filters.gt("_id", aux.get("_id"))));

                movIterDoc = moveCol.find(Filters.and(Filters.eq("numExp", numExp),
                                Filters.gte("Hora", lastMovObject.get("Hora")), Filters.gt("_id", aux.get("_id"))))
                        .sort(ascending("Hora", "_id"));
            }
            List<String> toSql = createSqlCommandsFromMovementList(movIterDoc);
            closeDeal(movIterDoc, docSize, toSql);
            sleep(periodicidade);
        } else {
            sleep(1000);
        }
    }

    private void closeDeal(FindIterable<Document> movIterDoc, int docSize, List<String> toSql) throws MongoTimeoutException, MongoSocketReadException, MongoSocketOpenException, SQLException {
        sqlConn.setAutoCommit(false);
        ClientSession session = Main.mt.getMongoSession();
        session.startTransaction();

        try {
            List<CallableStatement> lcs = new ArrayList<>();
            for (String s : toSql) lcs.add(sqlConn.prepareCall(s));
            for (CallableStatement cs : lcs) cs.execute();

            if (DataHoraFim != null) {
                if (!DataHoraFim.toString().isEmpty()) {
                    mazeManageCol.findOneAndUpdate(Filters.eq("idExp", idExperience),
                            Updates.set("populada", 1));
                }
            }
            if (docSize != 0) {
                Document lastMovDocument = movIterDoc.skip(docSize - 1).first();
                String lastMoveString = "{_id:\"" + lastMovDocument.get("_id") + "\", Hora: \""
                        + lastMovDocument.get("Hora") + "\"}";
                mazeManageCol.findOneAndUpdate(Filters.eq("idExp", idExperience),
                        Updates.set("lastMov", lastMoveString));
            }
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

    public List<String> createSqlCommandsFromMovementList(FindIterable<org.bson.Document> movIterDoc) {
        List<String> toSql = new ArrayList<>();
        for (org.bson.Document doc : movIterDoc) {
            try {
                int salaEntrada = Integer.parseInt(doc.get("SalaEntrada").toString());
                int salaSaida = Integer.parseInt(doc.get("SalaSaida").toString());
                toSql.add("call introduzirPassagem(" + idExperience + "," + salaEntrada + ","
                        + salaSaida + ",\"" + doc.get("Hora") + "\")");
            } catch (NumberFormatException e) {
                toSql.add("call introduzirErroExperiencia(" + idExperience + ",\"" + doc.get("Hora") + "\",\""
                        + "SalaEntrada: " + doc.get("SalaEntrada").toString().replace("\"", "") + ",SalaSaida: "
                        + doc.get("SalaSaida").toString().replace("\"", "") + "\")");
            }
        }
        return toSql;
    }

    public void getLocalVariables() throws SQLException {
        VarSet.Vars vars = Main.mt.globalVars.getVars();
        idExperience = vars.getId_experiencia();
        DataHoraFim = vars.getData_hora_fim();
        periodicidade = vars.getPeriodicidade();
    }


}
