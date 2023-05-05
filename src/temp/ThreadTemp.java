package temp;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import main.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;
import temp.TempSensorThread.outlierSample;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.client.model.Sorts.ascending;

public class ThreadTemp extends Thread {

    private MongoCollection tempCol;
    private MongoCollection mazeManageCol;
    private Connection sqlConn;

    private long periodicidade = 0;
    private int idExperience;
    private Date DataHoraFim;
    private int temperaturaMaxima;
    private int temperaturaMinima;
    private int numSensores;
    private double fatorAlertaAmarelo;
    private double fatorAlertaLaranja;
    private double fatorAlertaVermelho;
    private double temperaturaIdeal;
    private double variacaoTemperatura;

    private CountDownLatch countDownLatch;

    public List<String> InsertToSql = Collections.synchronizedList(new ArrayList<String>());
    public List<String> CallToSql = Collections.synchronizedList(new ArrayList<String>());

    public ThreadTemp(MongoCollection tempCol, MongoCollection mazeManageCol) {
        this.tempCol = tempCol;
        this.mazeManageCol = mazeManageCol;

    }

    public void run() {
        try {
            initConn();
        } catch (InterruptedException e) {
            Main.documentLabel.append("ThreadTemp interrupted (code 0), a terminar....\n");
            return;
        }
        while (true) {
            try {
                doRegularWork();
            } catch (InterruptedException ie) {
                Main.documentLabel.append("ThreadTemp interrupted (code 0), a terminar....\n");
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

    private void doRegularWork() throws InterruptedException, MongoTimeoutException, MongoSocketReadException, MongoSocketOpenException, SQLException {
        if(Main.mt.globalVars.isPopulated()) {
            getLocalVariables();

            FindIterable<Document> mazeManageIterDoc = mazeManageCol.find(Filters.eq("idExp", idExperience));
            int numExp = mazeManageIterDoc.first().getInteger("numExp");
            int docSize;
            FindIterable<Document> tempIterDoc;
            if (mazeManageIterDoc.first().getString("lastTemp") == null) {
                tempIterDoc = tempCol.find(Filters.eq("numExp", numExp)).sort(ascending("Hora", "_id"));
                docSize = (int) tempCol.count(Filters.eq("numExp", numExp));
            } else {
                DBObject lastTempObject = BasicDBObject.parse(mazeManageIterDoc.first().getString("lastTemp"));
                Document aux = new Document();
                aux.append("_id", new ObjectId(lastTempObject.get("_id").toString()));
                docSize = (int) tempCol.count(Filters.and(Filters.eq("numExp", numExp),
                        Filters.gte("Hora", lastTempObject.get("Hora")), Filters.gt("_id", aux.get("_id"))));
                tempIterDoc = tempCol.find(Filters.and(Filters.eq("numExp", numExp),
                                Filters.gte("Hora", lastTempObject.get("Hora")), Filters.gt("_id", aux.get("_id"))))
                        .sort(ascending("Hora", "_id"));
            }

            Pair<HashMap<Integer, TempSensorThread>, HashMap<Integer, List<outlierSample>>> pair = populateSensorThreads(
                    tempIterDoc);
            HashMap<Integer, TempSensorThread> sensorThreadsMap = pair.getLeft();
            HashMap<Integer, List<outlierSample>> OSListMap = pair.getRight();
            startSensorThreads(sensorThreadsMap);

            if (DataHoraFim != null)
                if (!DataHoraFim.toString().isEmpty())
                    CallToSql.add("call experienciaPopulada(" + idExperience + ")");

            closeDeal(numExp, docSize, tempIterDoc, OSListMap);
            sleep(periodicidade);
        } else
            sleep(1000);
    }

    private void closeDeal(int numExp, int docSize, FindIterable<Document> tempIterDoc, HashMap<Integer, List<outlierSample>> OSListMap) throws MongoTimeoutException, MongoSocketReadException, MongoSocketOpenException, SQLException {

        sqlConn.setAutoCommit(false);
        ClientSession session = Main.mt.getMongoSession();
        session.startTransaction();

        try {
            List<PreparedStatement> lps = new ArrayList<>();
            for (String s : InsertToSql) lps.add(sqlConn.prepareStatement(s));
            for (PreparedStatement pst : lps) pst.execute();

            List<CallableStatement> lcs = new ArrayList<>();
            for (String s : CallToSql) lcs.add(sqlConn.prepareCall(s));
            for (CallableStatement cs : lcs) cs.execute();

            putOutlierSamplesInMongo(OSListMap, numExp);

            if (docSize != 0) {
                Document lastTempDocument = tempIterDoc.skip(docSize -
                        1).first();
                String lastTempString = "{_id:\"" + lastTempDocument.get("_id") +
                        "\", Hora: \""
                        + lastTempDocument.get("Hora") + "\"}";

                mazeManageCol.findOneAndUpdate(Filters.eq("idExp", idExperience),
                        Updates.set("lastTemp", lastTempString));
            }

            sqlConn.commit();
            session.commitTransaction();
        } catch (Exception e) {
            if (sqlConn != null) sqlConn.rollback();
            if (session != null) session.abortTransaction();
        } finally {
            if (sqlConn != null) sqlConn.setAutoCommit(true);
            if (session != null) session.close();
            CallToSql.clear();
            InsertToSql.clear();
        }


    }

    private void putOutlierSamplesInMongo(HashMap<Integer, List<outlierSample>> OSListMap, int numExp) {
        for (Map.Entry<Integer, List<outlierSample>> OSList : OSListMap.entrySet()) {
            List<Document> OSdocs = new ArrayList<>();
            for (outlierSample os : OSList.getValue()) {
                OSdocs.add(new Document("Hora", os.getHora()).append("Leitura", os.getTemperatura()));
            }

            mazeManageCol.findOneAndUpdate(Filters.eq("numExp", numExp),
                    Updates.set("arraySensor" + OSList.getKey(), OSdocs));
        }
    }

    public void startSensorThreads(HashMap<Integer, TempSensorThread> map) {
        countDownLatch = new CountDownLatch(map.size());
        for (Map.Entry<Integer, TempSensorThread> sensorThread : map.entrySet()) {
            sensorThread.getValue().setCountDownLatch(countDownLatch);
            sensorThread.getValue().start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Pair<HashMap<Integer, TempSensorThread>, HashMap<Integer, List<outlierSample>>> populateSensorThreads(
            FindIterable<Document> tempIterDoc) {
        HashMap<Integer, TempSensorThread> SensorThreadMap = new HashMap<>();
        HashMap<Integer, List<outlierSample>> OSListMap = new HashMap<>();
        for (Document doc : tempIterDoc) {
            try {
                double leitura = Double.parseDouble(doc.get("Leitura").toString());
                int sensor = Integer.parseInt(doc.get("Sensor").toString());
                if (leitura > temperaturaMinima && leitura < temperaturaMaxima && sensor <= numSensores) {
                    if (SensorThreadMap.containsKey(sensor))
                        SensorThreadMap.get(sensor).IS.add(doc);
                    else {
                        List<outlierSample> OSList = new ArrayList<>();
                        OSListMap.put(sensor, OSList);
                        SensorThreadMap.put(sensor, new TempSensorThread(mazeManageCol, sensor, Integer.parseInt(
                                doc.get("numExp").toString()), idExperience, fatorAlertaAmarelo, fatorAlertaLaranja,
                                fatorAlertaVermelho, temperaturaIdeal, variacaoTemperatura, this, OSList));
                        SensorThreadMap.get(sensor).IS.add(doc);
                    }
                } else
                    throw new NumberFormatException();
            } catch (NumberFormatException e) {
                CallToSql.add("call introduzirErroExperiencia(" + idExperience + ",\"" + doc.get("Hora") + "\",\""
                        + "Leitura: " + doc.get("Leitura").toString().replace("\"", "") + ",Sensor: "
                        + doc.get("Sensor").toString().replace("\"", "") + "\")");

            }
        }
        return new ImmutablePair<>(
                SensorThreadMap, OSListMap);
    }

    public void getLocalVariables() throws SQLException {
        VarSet.Vars vars = Main.mt.globalVars.getVars();

        idExperience = vars.getId_experiencia();
        DataHoraFim = vars.getData_hora_fim();
        temperaturaMaxima = vars.getTemperatura_maxima();
        temperaturaMinima = vars.getTemperatura_minima();
        temperaturaIdeal = vars.getTemperatura_ideal().doubleValue();
        numSensores = vars.getNum_sensores();
        fatorAlertaAmarelo = vars.getFator_alerta_amarelo().doubleValue();
        fatorAlertaLaranja = vars.getFator_alerta_laranja().doubleValue();
        fatorAlertaVermelho = vars.getFator_alerta_vermelho().doubleValue();
        variacaoTemperatura = vars.getVariacao_temperatura().doubleValue();
        periodicidade = vars.getPeriodicidade();


    }

}
