package mov;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;


import main.Mainthread;
import main.VarSet;

import java.sql.*;

import static com.mongodb.client.model.Sorts.ascending;

public class ThreadMov extends Thread {

    private MongoCollection moveCol;
    private MongoCollection mazeManageCol;
    private Connection conn;
    private Mainthread mainThread;

    private long periodicidade = 0;
    private int idExperience;
    private Date DataHoraFim;

    public ThreadMov(MongoCollection moveCol, MongoCollection mazeManageCol, Connection conn, Mainthread mainThread) {
        this.moveCol = moveCol;
        this.mazeManageCol = mazeManageCol;
        this.conn = conn;
        this.mainThread = mainThread;
        //System.out.println("done");

    }

    public void run() {
        //System.out.println("entrou");
        while (true) {

            try {
                Thread.sleep(periodicidade);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            getLocalVariables();
            //System.out.println("run 1");
            FindIterable<org.bson.Document> mazeManageIterDoc = mazeManageCol.find(Filters.eq("idExp", idExperience));
            int numExp = mazeManageIterDoc.first().getInteger("numExp");
            FindIterable<org.bson.Document> movIterDoc;
            int docSize;
            if (mazeManageIterDoc.first().getString("lastMov") == null) {
                movIterDoc = moveCol.find(Filters.eq("numExp", numExp)).sort(ascending("Hora", "_id"));
                docSize = (int) moveCol.count(Filters.eq("numExp", numExp));
            } else {
                DBObject lastMovObject = BasicDBObject.parse(mazeManageIterDoc.first().getString("lastMov"));
                org.bson.Document aux = new org.bson.Document();
                aux.append("_id", new ObjectId(lastMovObject.get("_id").toString()));
                docSize = (int) moveCol.count(Filters.and(Filters.eq("numExp", numExp),
                        Filters.gte("Hora", lastMovObject.get("Hora")), Filters.gt("_id", aux.get("_id"))));

                movIterDoc = moveCol.find(Filters.and(Filters.eq("numExp", numExp),
                        Filters.gte("Hora", lastMovObject.get("Hora")), Filters.gt("_id", aux.get("_id"))))
                        .sort(ascending("Hora", "_id"));

            }
            //System.out.println("run 2");

            /*
            for (org.bson.Document doc : movIterDoc) {
                System.out.println(doc);

            }
            */

            List<String> toSql = createSqlCommandsFromMovementList(movIterDoc);

            if (DataHoraFim != null)
                if (!DataHoraFim.toString().isEmpty())
                    toSql.add("call experienciaPopulada(" + idExperience + ")");

            /*
            for (String a : toSql) {
                System.out.println(a);
            }
            */
            //System.out.println("run 3");
            mainThread.sqlTransaction(() -> {
                mainThread.mongoTransaction(() -> {
                    //System.out.println(docSize);
                    if (docSize != 0) {
                        //System.out.println(docSize);
                        org.bson.Document lastMovDocument = movIterDoc.skip(docSize - 1).first();
                        String lastMoveString = "{_id:\"" + lastMovDocument.get("_id") + "\", Hora: \""
                                + lastMovDocument.get("Hora") + "\"}";
                        //System.out.println(lastMoveString);
                        mazeManageCol.findOneAndUpdate(Filters.eq("idExp", idExperience),
                                Updates.set("lastMov", lastMoveString));
                    }
                });
                executeSqlCommands(toSql);
                //System.out.println("run 4");
            });

        }
    }

    public List<String> createSqlCommandsFromMovementList(FindIterable<org.bson.Document> movIterDoc) {
        List<String> toSql = new ArrayList<String>();
        for (org.bson.Document doc : movIterDoc) {
            try {
                int salaEntrada = Integer.parseInt(doc.get("SalaEntrada").toString());
                int salaSaida = Integer.parseInt(doc.get("SalaSaida").toString());
                toSql.add("call introduzirPassagem(" + idExperience + "," + salaSaida + ","
                        + salaEntrada + ",\"" + doc.get("Hora") + "\")");
            } catch (NumberFormatException e) {
                toSql.add("call introduzirErroExperiencia(" + idExperience + ",\"" + doc.get("Hora") + "\",\""
                        + "SalaEntrada: " + doc.get("SalaEntrada").toString().replace("\"", "") + ",SalaSaida: "
                        + doc.get("SalaSaida").toString().replace("\"", "") + "\")");

            }
        }
        return toSql;
    }

    public void getLocalVariables() {
        //System.out.println("getLocalVariables");
        try {
            VarSet varSet = mainThread.globalVars;
            while (!varSet.isPopulated()) {
                //System.out.println("sleepy");
                Thread.sleep(1000);
            }
            VarSet.Vars vars = varSet.getVars();
            idExperience = vars.getId_experiencia();
            DataHoraFim = vars.getData_hora_fim();
            periodicidade = calcPeriodicidade(vars.getTempo_max_periodicidade(),
                    vars.getSegundos_abertura_portas_exterior(), vars.getTempo_entre_experiencia(),
                    vars.getFator_periodicidade().longValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void executeSqlCommands(List<String> commands) {
        for (String cmd : commands) {
            try {
                System.out.println(cmd);
                CallableStatement cs = conn.prepareCall(cmd);
                System.out.println(cs.execute());
                //System.out.println("executed");
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    public long calcPeriodicidade(int segMaxPer, int segAbrirPortas, int segMaxTempExp, long factorPer) {
        return segMaxPer - (segMaxPer / ((segAbrirPortas / segMaxTempExp) * factorPer + 1));
    }

}
