package br.com.pvmeira.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LocalDataBase {

    private final Connection connection;

    public LocalDataBase(String name) throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:service-users/target/" + name + ".db");

    }

    public PreparedStatement preparedStatement(String sql) throws SQLException {
        return this.connection.prepareStatement(sql);
    }

    public  void createIfNotExists(String sql) {
        try{
            connection.createStatement().execute(sql);
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
