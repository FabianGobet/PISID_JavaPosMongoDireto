package temp;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

public class TempSensorThread extends Thread {
    public List<Document> IS;
    private List<outlierSample> OS;
    private CountDownLatch countDownLatch;
    private MongoCollection mazeManageCol;
    private int sensor;
    private int numExp;
    private int idExp;
    private double limiteAmareloPos;
    private double limiteLaranjaPos;
    private double limiteVermelhoPos;
    private double limiteAmareloPre;
    private double limiteLaranjaPre;
    private double limiteVermelhoPre;
    private List<String> insertCmdToSql;
    private List<String> callCmdToSql;
    private ThreadTemp threadTemp;
    private final double MIN_IQR = 1;

    public TempSensorThread(MongoCollection mazeManageCol, int sensor, int numExp, int idExp, double fatorAlertaAmarelo,
            double fatorAlertaLaranja, double fatorAlertaVermelho, double temperaturaIdeal,
            double variacaoTemperatura, ThreadTemp threadTemp, List<outlierSample> OS) {
        this.mazeManageCol = mazeManageCol;
        IS = new ArrayList<Document>();
        this.OS = OS;
        this.sensor = sensor;
        this.numExp = numExp;
        this.idExp = idExp;
        this.insertCmdToSql = new ArrayList<String>();
        this.callCmdToSql = new ArrayList<String>();
        this.limiteAmareloPos = temperaturaIdeal + fatorAlertaAmarelo * variacaoTemperatura;
        this.limiteLaranjaPos = temperaturaIdeal + fatorAlertaLaranja * variacaoTemperatura;
        this.limiteVermelhoPos = temperaturaIdeal + fatorAlertaVermelho * variacaoTemperatura;

        this.limiteAmareloPre = temperaturaIdeal - fatorAlertaAmarelo * variacaoTemperatura;
        this.limiteLaranjaPre = temperaturaIdeal - fatorAlertaLaranja * variacaoTemperatura;
        this.limiteVermelhoPre = temperaturaIdeal - fatorAlertaVermelho * variacaoTemperatura;
        this.threadTemp = threadTemp;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void run() {
        FindIterable<Document> mazeManageIterDoc = mazeManageCol.find(Filters.eq("numExp", numExp));
        int outlierSampleSize = Integer.parseInt(mazeManageIterDoc.first().get("outlierSampleSize").toString());
        
        populateOS(mazeManageIterDoc);
        
        for (Document ISdoc : IS) {
            //System.out.println("ISDoc " + sensor + ": " + ISdoc);
            if (OS.size() < 3) {
                OS.add(new outlierSample(ISdoc));
                createInsertCmdAndCheckRange(ISdoc);
            } else if (OS.size() >= 3 && OS.size() < outlierSampleSize) {
                OS.add(new outlierSample(ISdoc));
                if (checkIfOutlier(Double.parseDouble(ISdoc.get("Leitura").toString()))) {
                    callCmdToSql.add("call introduzirErroExperiencia(" + idExp + ",\"" + ISdoc.get("Hora") + "\",\""
                            + "Leitura: " + ISdoc.get("Leitura").toString() + ",Sensor: "
                            + ISdoc.get("Sensor").toString() + "\")");
                } else
                    createInsertCmdAndCheckRange(ISdoc);
            } else if (OS.size() >= 3 && OS.size() >= outlierSampleSize) {
                removeOldestOutlierSample();
                OS.add(new outlierSample(ISdoc));

                if (checkIfOutlier(Double.parseDouble(ISdoc.get("Leitura").toString()))) {
                    callCmdToSql.add("call introduzirErroExperiencia(" + idExp + ",\"" + ISdoc.get("Hora") + "\",\""
                            + "Leitura: " + ISdoc.get("Leitura").toString() + ",Sensor: "
                            + ISdoc.get("Sensor").toString() + "\")");
                } else
                    createInsertCmdAndCheckRange(ISdoc);
            }
        }

        threadTemp.CallToSql.addAll(callCmdToSql);
        threadTemp.InsertToSql.addAll(insertCmdToSql);
        countDownLatch.countDown();
    }

    private void createInsertCmdAndCheckRange(Document ISdoc) {
        insertCmdToSql
                .add("INSERT INTO medicaotemperatura(IdExperiencia, DataHora, Leitura, Sensor) VALUES(" + idExp
                        + ",\"" + ISdoc.get("Hora")
                        + "\"," + ISdoc.get("Leitura") + "," + ISdoc.get("Sensor") + ")");
        checkTempRange(ISdoc);
    }

    private boolean checkIfOutlier(double leitura) {
        Collections.sort(OS, (os1, os2) -> (os1.getTemperatura() <= os2.getTemperatura()) ? -1 : 1);
        double Q1;
        double Q3;
        boolean removeLater = false;
        if (OS.size() % 2 != 0) {
            removeLater = true;
            OS.add(OS.size() / 2 + 1, OS.get(OS.size() / 2));
        }
        if (OS.size() / 2 % 2 != 0) {
            Q1 = OS.get((OS.size() / 2) / 2).getTemperatura();
            Q3 = OS.get(3 * ((OS.size() / 2) / 2) + 1).getTemperatura();
        } else {
            Q1 = (OS.get((OS.size() / 2) / 2 - 1).getTemperatura() + OS.get((OS.size() / 2) / 2).getTemperatura()) / 2d;
            Q3 = (OS.get(3 * ((OS.size() / 2) / 2) - 1).getTemperatura()
                    + OS.get(3 * ((OS.size() / 2) / 2)).getTemperatura()) / 2d;
        }

        if (removeLater)
            OS.remove(OS.size() / 2);

        double IQR;
        if(Q3 - Q1 < MIN_IQR) IQR = MIN_IQR;
        else IQR = Q3 - Q1;
        double upperFence = Q3 + (1.5 * IQR);
        double lowerFence = Q1 - (1.5 * IQR);
        if (leitura < lowerFence || leitura > upperFence) {
           // System.out.println("Vetor de OS quando " + leitura + " foi outlier");
            /*for(outlierSample os : OS) {
                System.out.println(os.getTemperatura());
            }*/
           // System.out.println("is outlier");
            return true;
            }
        return false;
    }

    private void removeOldestOutlierSample() {
        int oldestOutlierSampleid = 0;
        for (int i = 1; i < OS.size(); i++) {
            if (OS.get(i).getTimestamp().compareTo(OS.get(oldestOutlierSampleid).getTimestamp()) < 0) {
                oldestOutlierSampleid = i;
            }
        }
        OS.remove(oldestOutlierSampleid);

    }

    private void populateOS(FindIterable<Document> mazeManageIterDoc) {
        if (mazeManageIterDoc.first().get("arraySensor" + sensor) != null) {
            List<Document> OSdocs = mazeManageIterDoc.first().get("arraySensor" + sensor,
                    new ArrayList<Document>().getClass());
            for (Document doc : OSdocs) {
                OS.add(new outlierSample(doc));
            }
        }
    }

    private void checkTempRange(Document doc) {
        double leitura = Double.parseDouble(doc.get("Leitura").toString());
        //double leituraAbs = Math.abs(leitura);
        if ((leitura >= limiteAmareloPos && leitura < limiteLaranjaPos) || (leitura <= limiteAmareloPre
                && leitura > limiteLaranjaPre) )
            callCmdToSql.add("call introduzirAlerta(3," + idExp + ","
                    + "\"Temperatura de " + leitura + " graus dentro do limite definido como Alerta Amarelo\")");
        else if ((leitura >= limiteLaranjaPos && leitura < limiteVermelhoPos) || (leitura <= limiteLaranjaPre
                && leitura > limiteVermelhoPre))
            callCmdToSql.add("call introduzirAlerta(2," + idExp + ","
                    + "\"Temperatura de " + leitura + " graus dentro do limite definido como Alerta Laranja\")");
        else if (leitura>= limiteVermelhoPos || leitura <= limiteVermelhoPre)
            callCmdToSql.add("call introduzirAlerta(1," + idExp + ","
                    + "\"Temperatura de " + leitura + " graus dentro do limite definido como Alerta Vermelho\")");

    }

    public class outlierSample {
        Date timestamp;
        String hora;
        double temperatura;

        private outlierSample(Document doc) {
            try {
                this.timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS").parse(doc.get("Hora").toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            this.temperatura = Double.parseDouble(doc.get("Leitura").toString());
            this.hora = doc.get("Hora").toString();
        }

        public double getTemperatura() {
            return temperatura;
        }

        public Date getTimestamp() {
            return timestamp;
        }

        public String getHora() {
            return hora;
        }
    }

}
