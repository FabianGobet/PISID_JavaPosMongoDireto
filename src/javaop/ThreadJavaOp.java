package javaop;

import com.mongodb.MongoException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import main.VarSet;
import org.bson.Document;
import main.Main;
import main.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;


public class ThreadJavaOp extends Thread{

    private VarSet globalVars;
    private MongoCollection<Document> getMoveCol;
    private MongoCollection<Document> getManageCol;
    private MongoCollection<Document> getTempCol;
    private Connection sqlConn;

    private final String sqlQuery = "SELECT * FROM javaop";

    public ThreadJavaOp() {
        this.globalVars = Main.mt.globalVars;
    }

    private void initConn() throws InterruptedException {
        boolean flag = true;
        while (flag) {
            try {
                sqlConn = Main.mt.getConnectionSql();
                this.getManageCol = Main.mt.getManageCol;
                this.getMoveCol = Main.mt.getMoveCol;
                this.getTempCol = Main.mt.getTempCol;
                this.getManageCol.find(new Document("idExp", -1));
                flag = false;
                Main.documentLabel.append("ThreadJavaOp: Ligação Estabelecida.\n");
            } catch (SQLException | MongoException e) {
                Main.documentLabel.append("ThreadJavaOp waiting for connections...\n");
                sleep(1000);
            }
        }
    }

    private void initConn(Exception e) throws InterruptedException {
        boolean flag = true;
        while (flag) {
            try {
                if(e instanceof MongoException){
                    this.getManageCol = Main.mt.getManageCol;
                    this.getMoveCol = Main.mt.getMoveCol;
                    this.getTempCol = Main.mt.getTempCol;
                    this.getManageCol.find(new Document("idExp", -1));
                }
                else if(e instanceof SQLException) {
                    sqlConn = Main.mt.getConnectionSql();
                }
                flag = false;
                Main.documentLabel.append("ThreadJavaOp: Ligação Estabelecida.\n");
            } catch (SQLException | MongoException e2) {
                sleep(1000);
            }
        }
    }


    public void run() {
        try {
            initConn();
        } catch (InterruptedException e) {
            Main.documentLabel.append("ThreadJavaOp: Interrompida, a terminar.\n");
            return;
        }
        while (true) {
            try {
                doRegularWork();
            } catch (InterruptedException ie) {
                Main.documentLabel.append("ThreadJavaOp: Interrompida, a terminar.\n");
                return;
            } catch (MongoException | SQLException e){

                try {
                    sleep(1000);
                    Main.documentLabel.append("ThreadJavaOp: Ligação perdida. A tentar reconectar.\n");
                    initConn(e);
                } catch (InterruptedException ex) {
                    Main.documentLabel.append("ThreadJavaOp: Interrompida, a terminar.\n");
                    return;
                }

            }
        }
    }

    private void doRegularWork() throws SQLException, InterruptedException, MongoException {
        ResultSet vars = fetchSqlData();
        if (vars.next()) {
            int idExp = vars.getInt("id_experiencia");
            MongoCursor<Document> manageEntry = getMazeManageEntry(idExp);
            int numExp = getCorrectNumExp(vars.getTimestamp("data_hora_inicio"));
            if (manageEntry.hasNext()) {
                Document data = manageEntry.next();
                if (data.getInteger("populada") == 1 && vars.getInt("populada") == 0) {
                    resetData(data.getInteger("idExp"));
                    setMazeManageEntry(vars, idExp, numExp);
                }
                globalVars.setVars(vars);
            } else if (numExp != -1) {
                setMazeManageEntry(vars, idExp, numExp);
                globalVars.setVars(vars);
            }
        } else {
            globalVars.cleanVars();
        }
        sleep(1000);
    }

    private void setMazeManageEntry(ResultSet vars, int idExp, int numExp) throws SQLException, InterruptedException, MongoException {
        BigDecimal fator_outlier = vars.getBigDecimal("fator_tamanho_outlier_sample");
        int segundos_abertura = vars.getInt("segundos_abertura_portas_exterior");
        int outlier_sample_size = fator_outlier.multiply(BigDecimal.valueOf(segundos_abertura)).multiply(BigDecimal.valueOf(Mainthread.DADOS_SEGUNDO)).intValue();
        Document doc = new Document("idExp", idExp)
                .append("numExp", numExp)
                .append("outlierSampleSize", outlier_sample_size).append("populada", 0);
        getManageCol.insertOne(doc);
    }

    private void resetData(Integer idExp) throws MongoException{
        getManageCol.deleteOne(new Document("idExp", new Document("$eq", idExp)));
    }

    private MongoCursor<Document> getMazeManageEntry(int idExp) {
        Document mongoQuery = new Document("idExp", new Document("$eq", idExp));
        return getManageCol.find(mongoQuery).iterator();
    }

    private ResultSet fetchSqlData() throws SQLException {
        return sqlConn.createStatement().executeQuery(sqlQuery);
    }

    private Set<Integer> getNumsExpAfterTimestamp(Timestamp idExpStartTime) throws MongoException{
        Set<Integer> ls = new HashSet<>();
        String time = idExpStartTime.toString();

        Document matchStage = new Document("$match", new Document("Hora", new Document("$gt", time)));
        Document groupStage = new Document("$group", new Document("_id", "$numExp"));
        Document sortStage = new Document("$sort", new Document("_id", 1));
        Document projectStage = new Document("$project", new Document("_id", 0).append("numExp", "$_id"));
        Document limitStage = new Document("$limit", 2);
        List<Document> pipeline = Arrays.asList(matchStage, groupStage, sortStage, projectStage, limitStage);
        MongoCursor<Document> result1 = getTempCol.aggregate(pipeline).iterator();
        MongoCursor<Document> result2 = getMoveCol.aggregate(pipeline).iterator();
        while (result1.hasNext()) {
            ls.add(result1.next().getInteger("numExp"));
        }
        while (result2.hasNext()) {
            ls.add(result2.next().getInteger("numExp"));
        }
        return ls;
    }

    private boolean isCorrectNumExp(int numExp, Timestamp idExpStartTime) throws MongoException{
        boolean valid = false;
        Document matchStage = new Document("$match",
                new Document("Hora", new Document("$lte", idExpStartTime.toString())).append("numExp",
                        new Document("$eq", numExp)));
        Document groupStage = new Document("$group", new Document("_id", "$numExp"));
        Document limitStage = new Document("$limit", 1);
        List<Document> pipeline = Arrays.asList(matchStage, groupStage, limitStage);
        MongoCursor<Document> result = getTempCol.aggregate(pipeline).iterator();
        if (!result.hasNext())
            valid = true;
        return valid;
    }

    private int getCorrectNumExp(Timestamp idExpStartTime) {
        int result = -1;
        Set<Integer> ls = new HashSet<>();
        for (int numExp : getNumsExpAfterTimestamp(idExpStartTime))
            if (isCorrectNumExp(numExp, idExpStartTime))
                ls.add(numExp);
        if (!ls.isEmpty())
            result = Collections.min(ls);
        return result;
    }


}
