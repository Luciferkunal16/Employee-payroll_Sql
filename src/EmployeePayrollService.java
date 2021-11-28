package EmployeePayrollSQL;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;

public class EmployeePayrollService {
    String jdbcurl = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";

    String userName = "root";
    String password = "Kunal123";
    Connection con = DriverManager.getConnection(jdbcurl, userName, password);
    HashMap<String,EmployeePayroll> hashMapEmployee=new HashMap<String,EmployeePayroll>();
    Scanner inp = new Scanner(System.in);

    public EmployeePayrollService() throws SQLException {
    }

    void retriveEmployeeData() {
        ResultSet resultSet;
        try {

            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver Loaded");

            PreparedStatement stmt = con.prepareStatement("select * from employee_payroll");
            resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String employeeName = resultSet.getString("employee_name");
                int salary = resultSet.getInt("salary");

                System.out.println(id + " " + employeeName + " " + salary );
                EmployeePayroll employee = new EmployeePayroll(id, employeeName, salary);
                hashMapEmployee.put(employeeName,employee);

            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    void retriveLocalData() {
        hashMapEmployee.forEach((key,value)-> System.out.println(key +" " +value));

    }

    void insertDataIntoDatabase() throws ParseException {
        System.out.println("Enter The Id");
        int id = inp.nextInt();
        System.out.println("Enter The Name Of Employee");
        String employeeName = inp.next();
        System.out.println("Enter The salary");
        int salary = inp.nextInt();

        EmployeePayroll employeePayroll = new EmployeePayroll(id, employeeName, salary);
        hashMapEmployee.put(employeeName,employeePayroll);
        ResultSet resultSet;
        try {

            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver Loaded");

            PreparedStatement stmt = con.prepareStatement("insert into employee_payroll (id,employee_name,salary) values (?,?,?)");
            stmt.setInt(1,id);
            stmt.setString(2,employeeName);
            stmt.setInt(3,salary);
            stmt.executeUpdate();




    } catch(
    Exception e)

    {
        e.printStackTrace();
    }

}

void updateSalary(){
    System.out.println("Enter the name of employee whose salary you want to update ");
    String employeeName=inp.next();

    if(hashMapEmployee.containsKey(employeeName)==true){
        hashMapEmployee.remove(employeeName);
        try {
            System.out.println("Enter the update salary");
            int updatedSalary=inp.nextInt();
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver Loaded");

            PreparedStatement stmt = con.prepareStatement("update employee_payroll set salary=? where employee_name=?");
            stmt.setInt(1,updatedSalary);
            stmt.setString(2,employeeName);
            stmt.executeUpdate();
            retriveEmployeeData();





        } catch(
                Exception e)

        {
            e.printStackTrace();
        }


    }
    else{
        System.out.println("Employee not exist");
    }


}


    public static void main(String[] args) throws Exception {
        Scanner inp = new Scanner(System.in);
        EmployeePayrollService obj=new EmployeePayrollService();
        int choice;
    do{
        System.out.println("Welcome to menu");
        System.out.println("1)Get All Detail of Employee");
        System.out.println("2)View Data From ArrayList");
        System.out.println("3)Insert Data into Database");
        System.out.println("4)Update Salary");
        choice=inp.nextInt();
        if (choice==1){
            obj.retriveEmployeeData();
        }
        else if(choice==2){
            obj.retriveLocalData();
        }
        else if(choice==3){
            obj.insertDataIntoDatabase();
        }
        else if(choice==4){
            obj.updateSalary();
        }
    }while(choice!=10);


    }
}
