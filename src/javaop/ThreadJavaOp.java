package javaop;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.session.ClientSession;

import main.Main;
import main.Mainthread;
import main.VarSet;

public class ThreadJavaOp extends Thread {

    private VarSet globalVars;
    private MongoCollection<Document> getMoveCol;
    private MongoCollection<Document> getManageCol;
    private MongoCollection<Document> getTempCol;
    public Connection sqlConn;

    private final String sqlQuery = "SELECT * FROM javaop";

    public ThreadJavaOp(VarSet globalvars, Connection sqlConn, MongoCollection<Document> getManageCol,
            MongoCollection<Document> getMoveCol,
            MongoCollection<Document> getTempCol) {
        this.globalVars = globalvars;
        this.getManageCol = getManageCol;
        this.getMoveCol = getMoveCol;
        this.getTempCol = getTempCol;
        this.sqlConn = sqlConn;
    }

    public void run() {
        try {
            while (true) {
                ResultSet vars = fetchSqlData();
                if (vars.next()) {
                    int idExp = vars.getInt("id_experiencia");
                    MongoCursor<Document> manageEntry = getMazeManageEntry(idExp);
                    int numExp = getCorrectNumExp(vars.getTimestamp("data_hora_inicio"));
                    if (manageEntry.hasNext()) {
                        globalVars.setVars(vars);
                    } else if (numExp != -1) {
                        BigDecimal fator_outlier = vars.getBigDecimal("fator_tamanho_outlier_sample");
                        int segundos_abertura = vars.getInt("segundos_abertura_portas_exterior");
                        int outlier_sample_size = fator_outlier.multiply(BigDecimal.valueOf(segundos_abertura)).multiply(BigDecimal.valueOf(Mainthread.DADOS_SEGUNDO)).intValue();
                        Document doc = new Document("idExp", idExp)
                                .append("numExp", numExp)
                                .append("outlierSampleSize", outlier_sample_size);
                        getManageCol.insertOne(doc);
                        globalVars.setVars(vars);
                    }
                } else {
                    globalVars.cleanVars();
                }
                sleep(1000);
            }
        } catch (Exception e) {
            Main.documentLabel.append("JavaOp thread interrupted" + e);
        }
    }

    private MongoCursor<Document> getMazeManageEntry(int idExp) {
        Document mongoQuery = new Document("idExp", new Document("$eq", idExp));
        return getManageCol.find(mongoQuery).iterator();
    }

    private ResultSet fetchSqlData() {
        ResultSet vars = null;
        Statement stmt;
        try {
            stmt = sqlConn.createStatement();
            return stmt.executeQuery(sqlQuery);

        } catch (SQLException e) {
            Main.documentLabel.append("Error while fetching javaop data: " + e);
        }
        return vars;
    }

    private Set<Integer> getNumsExpAfterTimestamp(Timestamp idExpStartTime) {
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

    private boolean isCorrectNumExp(int numExp, Timestamp idExpStartTime) {
        Boolean valid = false;
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
