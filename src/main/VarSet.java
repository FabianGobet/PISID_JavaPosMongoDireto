package main;

import java.math.BigDecimal;
import java.sql.*;

public class VarSet {

    private ResultSet javaopSet;

    public VarSet() {
    }

    public synchronized boolean isPopulated() {
        return javaopSet != null;
    }

    public void cleanVars() {
        javaopSet = null;
    }

    public synchronized void setVars(ResultSet javaopSet) {
        this.javaopSet = javaopSet;
    }

    public synchronized Vars getVars() throws SQLException {
        return new Vars(javaopSet);
    }

    public class Vars {

        int id_experiencia, num_sensores,
                tempo_max_periodicidade, temperatura_maxima, temperatura_minima, tempo_entre_experiencia,
                segundos_abertura_portas_exterior;
        Timestamp data_hora_inicio, data_hora_fim;
        BigDecimal temperatura_ideal, variacao_temperatura, fator_periodicidade, fator_tamanho_outlier_sample,
                fator_alerta_vermelho, fator_alerta_laranja, fator_alerta_amarelo;

        public Vars(ResultSet res) throws SQLException {
            this.id_experiencia = Integer.valueOf(res.getInt("id_experiencia"));
            this.fator_alerta_vermelho = new BigDecimal(res.getBigDecimal("fator_alerta_vermelho").toString());
            this.fator_alerta_laranja = new BigDecimal(res.getBigDecimal("fator_alerta_laranja").toString());
            this.fator_alerta_amarelo = new BigDecimal(res.getBigDecimal("fator_alerta_amarelo").toString());
            this.num_sensores = Integer.valueOf(res.getInt("num_sensores"));
            this.tempo_max_periodicidade = Integer.valueOf(res.getInt("tempo_max_periodicidade"));
            this.temperatura_maxima = Integer.valueOf(res.getInt("temperatura_maxima"));
            this.temperatura_minima = Integer.valueOf(res.getInt("temperatura_minima"));
            this.tempo_entre_experiencia = Integer.valueOf(res.getInt("tempo_entre_experiencia"));
            this.segundos_abertura_portas_exterior = Integer.valueOf(res.getInt("segundos_abertura_portas_exterior"));
            this.data_hora_inicio = (Timestamp) res.getTimestamp("data_hora_inicio").clone();
            Timestamp temp = res.getTimestamp("data_hora_fim");
            this.data_hora_fim = temp==null? null: (Timestamp) temp.clone();
            this.temperatura_ideal = new BigDecimal(res.getBigDecimal("temperatura_ideal").toString());
            this.variacao_temperatura = new BigDecimal(res.getBigDecimal("variacao_temperatura").toString());
            this.fator_periodicidade = new BigDecimal(res.getBigDecimal("fator_periodicidade").toString());
            this.fator_tamanho_outlier_sample = new BigDecimal(
                    res.getBigDecimal("fator_tamanho_outlier_sample").toString());
        }

        public int getId_experiencia() {
            return id_experiencia;
        }

        public BigDecimal getFator_alerta_vermelho() {
            return fator_alerta_vermelho;
        }

        public BigDecimal getFator_alerta_laranja() {
            return fator_alerta_laranja;
        }

        public BigDecimal getFator_alerta_amarelo() {
            return fator_alerta_amarelo;
        }

        public int getNum_sensores() {
            return num_sensores;
        }

        public int getTempo_max_periodicidade() {
            return tempo_max_periodicidade;
        }

        public int getTemperatura_maxima() {
            return temperatura_maxima;
        }

        public int getTemperatura_minima() {
            return temperatura_minima;
        }

        public int getTempo_entre_experiencia() {
            return tempo_entre_experiencia;
        }

        public int getSegundos_abertura_portas_exterior() {
            return segundos_abertura_portas_exterior;
        }

        public Timestamp getData_hora_inicio() {
            return data_hora_inicio;
        }

        public Timestamp getData_hora_fim() {
            return data_hora_fim;
        }

        public BigDecimal getTemperatura_ideal() {
            return temperatura_ideal;
        }

        public BigDecimal getVariacao_temperatura() {
            return variacao_temperatura;
        }

        public BigDecimal getFator_periodicidade() {
            return fator_periodicidade;
        }

        public BigDecimal getFator_tamanho_outlier_sample() {
            return fator_tamanho_outlier_sample;
        }
    }

}
