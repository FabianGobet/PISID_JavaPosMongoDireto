package temp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.mongodb.BasicDBObject;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;

import main.Mainthread;
import main.VarSet;
import temp.TempSensorThread.outlierSample;

import java.sql.*;

import static com.mongodb.client.model.Sorts.ascending;

public class ThreadTemp extends Thread {

    private MongoCollection tempCol;
    private MongoCollection mazeManageCol;
    private Connection conn;
    private Mainthread mainThread;

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

    public ThreadTemp(MongoCollection tempCol, MongoCollection mazeManageCol, Connection conn, Mainthread mainThread) {
        this.tempCol = tempCol;
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
            System.out.println("\nAll docs");
            for (Document doc : tempIterDoc) {
                System.out.println(doc);

            }

            Pair<HashMap<Integer, TempSensorThread>, HashMap<Integer, List<outlierSample>>> pair = populateSensorThreads(
                    tempIterDoc);
            HashMap<Integer, TempSensorThread> sensorThreadsMap = pair.getLeft();
            HashMap<Integer, List<outlierSample>> OSListMap = pair.getRight();
            startSensorThreads(sensorThreadsMap);

            if (DataHoraFim != null)
                if (!DataHoraFim.toString().isEmpty())
                    CallToSql.add("call experienciaPopulada(" + idExperience + ")");

            // QUANDO EXECUTAR OS COMANDOS SQL ATENCAO QUE ALGUNS SAO INSERT E OUTROS CALL E
            // LIMPA OS
            System.out.println("\nInsert Commands");
            for (String a : InsertToSql) {
                System.out.println(a);
            }

            System.out.println("\nCall Commands");
            for (String a : CallToSql) {
                System.out.println(a);
            }

            mainThread.sqlTransaction(() -> {
                mainThread.mongoTransaction(() -> {
                    putOutlierSamplesInMongo(mazeManageCol, OSListMap, numExp);
                    System.out.println("\n docSize " + docSize);
                    if (docSize != 0) {
                        Document lastTempDocument = tempIterDoc.skip(docSize -
                                1).first();
                        String lastTempString = "{_id:\"" + lastTempDocument.get("_id") +
                                "\", Hora: \""
                                + lastTempDocument.get("Hora") + "\"}";
                        System.out.println(lastTempString);
                        mazeManageCol.findOneAndUpdate(Filters.eq("idExp", idExperience),
                                Updates.set("lastTemp", lastTempString));
                    }
                });
                executeSqlCommands();
            });

        }
    }

    private void putOutlierSamplesInMongo(MongoCollection mazeManageIterDoc,
            HashMap<Integer, List<outlierSample>> OSListMap, int numExp) {
        for (Map.Entry<Integer, List<outlierSample>> OSList : OSListMap.entrySet()) {
            List<Document> OSdocs = new ArrayList<Document>();
            for (outlierSample os : OSList.getValue()) {
                OSdocs.add(new Document("Hora", os.getHora()).append("Leitura", os.getTemperatura()));
            }

            mazeManageCol.findOneAndUpdate(Filters.eq("numExp", numExp),
                    Updates.set("arraySensor" + OSList.getKey(), OSdocs));
        }
    }

    public void startSensorThreads(HashMap<Integer, TempSensorThread> map) {
        countDownLatch = new CountDownLatch(map.size());
        // tirar depois
        for (Map.Entry<Integer, TempSensorThread> sensorThread : map.entrySet()) {
            System.out.println("sensor: " + sensorThread.getKey());
            for (Document doc : sensorThread.getValue().IS) {
                System.out.println(doc);
            }
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
        HashMap<Integer, TempSensorThread> SensorThreadMap = new HashMap<Integer, TempSensorThread>();
        HashMap<Integer, List<outlierSample>> OSListMap = new HashMap<Integer, List<outlierSample>>();
        for (Document doc : tempIterDoc) {
            try {
                double leitura = Double.parseDouble(doc.get("Leitura").toString());
                int sensor = Integer.parseInt(doc.get("Sensor").toString());
                if (leitura > temperaturaMinima && leitura < temperaturaMaxima && sensor <= numSensores) {
                    if (SensorThreadMap.containsKey(sensor))
                        SensorThreadMap.get(sensor).IS.add(doc);
                    else {
                        List<outlierSample> OSList = new ArrayList<outlierSample>();
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
        return new ImmutablePair<HashMap<Integer, TempSensorThread>, HashMap<Integer, List<outlierSample>>>(
                SensorThreadMap, OSListMap);
    }

    public void getLocalVariables() {
        try {
            VarSet varSet = mainThread.globalVars;
            while (!varSet.isPopulated())
                Thread.sleep(1000);
            VarSet.Vars vars = varSet.getVars();

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
            periodicidade = calcPeriodicidade(vars.getTempo_max_periodicidade(),
                    vars.getSegundos_abertura_portas_exterior(),
                    vars.getTempo_entre_experiencia(),
                    vars.getFator_periodicidade().longValue());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // idExperience = 1;
        // DataHoraFim = null;
        // temperaturaMaxima = 400;
        // temperaturaMinima = -200;
        // numSensores = 2;
        // fatorAlertaAmarelo = 0.5;
        // fatorAlertaLaranja = 0.6;
        // fatorAlertaVermelho = 0.8;
        // variacaoTemperatura = 6;
        // periodicidade = 3000;
        // temperaturaIdeal = 10;

    }

    public long calcPeriodicidade(int segMaxPer, int segAbrirPortas, int segMaxTempExp, long factorPer) {
        return segMaxPer - (segMaxPer / ((segAbrirPortas / segMaxTempExp) * factorPer + 1));
    }

    public void executeSqlCommands() {

        for (String insertCmd : InsertToSql) {
            try {
                PreparedStatement ps = conn.prepareStatement(insertCmd);
                ps.execute();
                System.out.println("insert executed");
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        for (String callCmd : CallToSql) {
            try {
                CallableStatement cs = conn.prepareCall(callCmd);
                cs.execute();
                System.out.println("call executed");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        CallToSql.clear();
        InsertToSql.clear();
    }
}
