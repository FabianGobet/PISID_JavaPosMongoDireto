package main;

import java.sql.*;


public class Teste {
    public static void main(String[] args) {

        // kr.pJ.x95#
        try {
            System.out.println("Trying...");
            Connection conn = DriverManager.getConnection("jdbc:mariadb://46.189.143.63:3306/micelab", "root",
                    "kr.pJ.x95#");
            System.out.println(conn.isValid(5));
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
