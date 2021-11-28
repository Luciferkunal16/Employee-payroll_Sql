package EmployeePayrollSQL;

public class EmployeePayroll {
    int id;
    String name;
    int salary;
    EmployeePayroll(int id, String name, int salary){
        this.id=id;
        this.name=name;
        this.salary=salary;
    }

    void setId(int id){
    this.id=id;
    }
    int getId(){
        return id;
    }
    void setName(String name){
        this.name=name;

    }
     public String getName(){
        return name;
     }
     void setSalary(int salary){
        this.salary=salary;
     }
     int getSalary(){
        return salary;
     }
     @Override
    public String toString(){
        return "[ ID = "+id+" Name = "+name+" salary = "+salary+" ]";
     }


}
