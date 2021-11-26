
import java.sql.*;

public class ConnectDb {
    public static void main(String[] args) {
        String jdbcurl="jdbc:mysql://localhost:3306/employee_payrollDB?useSSL=false";
        String userName="root";
        String password="Kunal123";
        Connection con;
        Statement statement ;
        try{

            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver Loaded");
            con= DriverManager.getConnection(jdbcurl,userName,password);
            statement= con.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from employee_payroll");
            while(resultSet.next()){
              
               String employeeName=resultSet.getString("employee_name");
               int employeeSalary=resultSet.getInt("salary");
                System.out.print(employeeName+" ");
                System.out.print(employeeSalary+ " " );
                
            }

        }catch (Exception e){
           e.printStackTrace();
        }


    }
}