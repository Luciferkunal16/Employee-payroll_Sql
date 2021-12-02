package com.bridgelabz.employeepayrolljdbcapp;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.bridgelabz.employeepayrolljdbcapp.EmployeePayrollException.ExceptionType;

public class EmployeePayrollDBService {
	
	private PreparedStatement employeePayrollDataStatement;
	private static EmployeePayrollDBService employeePayrollDBService;
	
	private EmployeePayrollDBService() {
		
	}
	
	public static EmployeePayrollDBService getInstance() {
		if(employeePayrollDBService == null)
			employeePayrollDBService = new EmployeePayrollDBService();
		return employeePayrollDBService;
	}

	private Connection getConnection() throws SQLException {
		
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "Kunal123";
		Connection connection;
		
		System.out.println("Connecting to the database : "+jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("Connection is Succcessfully Established!! "+connection);
		
		return connection;
	}
	
	private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet) {
		
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		
		try {
			while(resultSet.next()) {
				int id = resultSet.getInt("employee_id");
				String name = resultSet.getString("employee_name");
				double basicSalary = resultSet.getDouble("basic_salary");
				LocalDate startDate = resultSet.getDate("start_date").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(id, name, basicSalary, startDate));
			}
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
		
	}
	
	public List<EmployeePayrollData> getEmployeePayrollData(String name) {
		
		List<EmployeePayrollData> employeePayrollList = null;
		if(this.employeePayrollDataStatement == null)
			this.preparedStatementForEmployeeData();
		try {
			employeePayrollDataStatement.setString(1,name);
			ResultSet resultSet = employeePayrollDataStatement.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);	
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
	}
	
	public List<EmployeePayrollData> readData(){
		
		String sqlStatement = "SELECT * FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id;";
		return this.getEmployeePayrollDataUsingDB(sqlStatement);
	}
	
	private void preparedStatementForEmployeeData() {
		
		try {
			Connection connection = this.getConnection();
			String sqlStatement = "SELECT * FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id WHERE employee_name = ?;";
			employeePayrollDataStatement = connection.prepareStatement(sqlStatement);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
	}
	
	private void preparedStatementForEmployeeDataBasedOnStartDate() {
		
		try {
			Connection connection = this.getConnection();
			String sqlStatement = "SELECT * FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id WHERE start_date BETWEEN ? AND ?;";
			employeePayrollDataStatement = connection.prepareStatement(sqlStatement);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
	}
	
	private List<EmployeePayrollData> getEmployeePayrollDataUsingDB (String sqlStatement) {
		
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
	}
	
	public int updateEmployeeData(String name, double salary) {
		
		return this.updateEmployeeDataUsingStatement(name,salary);
	}	

	public int updateEmployeeDataUsingStatement(String name, double salary) {
		
		String sqlStatement = String.format("UPDATE employee_payroll SET basic_salary = %.2f WHERE employee_id IN (SELECT employee_id FROM employee WHERE employee_name = '%s');", salary, name);
		
		try (Connection connection = getConnection()){
			Statement statement = connection.createStatement();
			return statement.executeUpdate(sqlStatement);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(ExceptionType.UPDATE_FAILED, exception.getMessage());
		}
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnNameUsingStatement(String name) {
		
		String sqlStatement = String.format("SELECT * FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id WHERE employee_name = '%s';",name);
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
		
	}
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnStartDateUsingStatement(LocalDate startDate, LocalDate endDate) {
		
		String sqlStatement = String.format("SELECT * FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id WHERE start_date BETWEEN '%s' AND '%s';",startDate, endDate);
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
	}
	
	public List<Double> getSumOfSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, SUM(basic_salary) AS TotalSalary FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id GROUP BY gender;";
		List<Double> sumOfSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("TotalSalary");
				sumOfSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return sumOfSalaryBasedOnGender;
	}
	
	public List<Double> getAverageOfSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, AVG(basic_salary) AS AverageSalary FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id GROUP BY gender;";
		List<Double> averageOfSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("AverageSalary");
				averageOfSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return averageOfSalaryBasedOnGender;
	}
	
	public List<Double> getMinimumSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, MIN(basic_salary) AS MinimumSalary FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id GROUP BY gender;";
		List<Double> MinimumSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("MinimumSalary");
				MinimumSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());

		}
		return MinimumSalaryBasedOnGender;
	}
	
	public List<Double> getMaximumSalaryBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, MAX(basic_salary) AS MaximumSalary FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id GROUP BY gender;";
		List<Double> MaximumSalaryBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				double salary = resultSet.getDouble("MaximumSalary");
				MaximumSalaryBasedOnGender.add(salary);
			}
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return MaximumSalaryBasedOnGender;
	}
	
	public List<Integer> getCountOfEmployeesBasedOnGenderUsingStatement() {
		
		String sqlStatement = "SELECT gender, COUNT(gender) AS CountBasedOnGender FROM employee JOIN employee_payroll ON employee.employee_id = employee_payroll.employee_id GROUP BY gender;";
		List<Integer> CountBasedOnGender = new ArrayList<>();
				
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				int count = resultSet.getInt("CountBasedOnGender");
				CountBasedOnGender.add(count);
			}
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return CountBasedOnGender;
	}	
	
	public List<EmployeePayrollData> getEmployeeDetailsBasedOnStartDateUsingPreparedStatement(String startDate, String endDate) {
		
		List<EmployeePayrollData> employeePayrollList = null;
		if(this.employeePayrollDataStatement == null)
			this.preparedStatementForEmployeeDataBasedOnStartDate();
		try {
			employeePayrollDataStatement.setString(1,startDate);
			employeePayrollDataStatement.setString(2,endDate);
			ResultSet resultSet = employeePayrollDataStatement.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);	
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return employeePayrollList;
	}
	
	public EmployeePayrollData addEmployeeToPayroll(int id, String name, double salary, long phoneNumber, LocalDate startDate, String gender, int companyId) {
		
		EmployeePayrollData employeePayrollData = null;
		String sql = String.format("INSERT INTO employee (employee_id, employee_name, gender, start_date, phone_number, company_id) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');", id, name, gender, Date.valueOf(startDate), phoneNumber, companyId);
		
		try (Connection connection = this.getConnection()){
			
			Statement statement = connection.createStatement();
			int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
			if(rowAffected == 1) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if(resultSet.next())
					id = resultSet.getInt("employee_id");
			}
			employeePayrollData = new EmployeePayrollData(id, name, salary, startDate);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		
		return employeePayrollData;
		
	}
	
	public EmployeePayrollData addEmployeeToUpdatedDatabase(int id, String name, double salary, long phoneNumber, LocalDate startDate, String gender, int companyId) {
		
		int employeeId = -1;
		Connection connection = null;
		EmployeePayrollData employeePayrollData = null;
		
		try {
			connection = this.getConnection();
			connection.setAutoCommit(false);
		}
		catch(SQLException exception) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		try (Statement statement = connection.createStatement()){
			
			String sql = String.format("INSERT INTO employee (employee_id, employee_name, gender, start_date, phone_number, company_id) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');", id, name, gender, Date.valueOf(startDate), phoneNumber, companyId);
			
			int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
			if(rowAffected == 1) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if(resultSet.next())
					employeeId = resultSet.getInt(1);
			}
			
		}
		catch(SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException exception) {
				throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.CONNECTION_FAILED, exception.getMessage());
			}
		}
		
		try(Statement statement = connection.createStatement()){

			double deductions = salary * 0.2;
			double taxablePay = salary - deductions;
			double tax = taxablePay * 0.1;
			double netPay = salary - tax;
			String sqlQuery = String.format("INSERT INTO employee_payroll(employee_id, basic_salary, deductions, taxable_pay, tax, net_pay) values ('%s', '%s', '%s', '%s', '%s','%s')",id, salary, deductions, taxablePay, tax, netPay);
			int rowAffected = statement.executeUpdate(sqlQuery);
			if (rowAffected == 1) {
				employeePayrollData = new EmployeePayrollData(employeeId, name, salary, startDate);
			}			
		}
		catch(SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException exception) {
				throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.CONNECTION_FAILED, exception.getMessage());
			}
		}
		
		try {
			connection.commit();
		} catch (SQLException e) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.COMMIT_FAILED, e.getMessage());
		}
		finally {
			if(connection != null)
				try {
					connection.close();
				} 
			catch (SQLException e) {
				throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.RESOURCES_NOT_CLOSED_EXCEPTION, e.getMessage());
			}
		}
		return employeePayrollData;
	}
	
	public List<EmployeePayrollData> deleteEmployeeFromDatabase(String name) {
		
		String query = String.format("UPDATE employee SET is_active = false WHERE employee_name = '%s';", name);
		
		try (Connection connection = this.getConnection()) {
			Statement statement = connection.createStatement();
			statement.executeUpdate(query);
			return this.readData();
			
		} catch (SQLException e) {
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.CONNECTION_FAILED, e.getMessage());
		}
	}

	public int getEmployeeActiveStatus(String name) {
		
		String sqlStatement = String.format("SELECT is_active FROM employee WHERE employee_name = '%s';", name);
		int isActive = 1;
		
		try (Connection connection = getConnection();){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlStatement);
			while(resultSet.next()) {
				isActive = resultSet.getInt("is_active");
			}
		}
		catch(SQLException exception){
			throw new EmployeePayrollException(EmployeePayrollException.ExceptionType.DATABASE_EXCEPTION, exception.getMessage());
		}
		return isActive;
	}
}
