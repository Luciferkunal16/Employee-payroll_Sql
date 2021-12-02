package com.bridgelabz.employeepayrolljdbcapp;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class EmployeePayrollData {
	
	public int employeeId;
	public String employeeName;
	public LocalDate startDate;
	public PayrollDetails payrollDetails = new PayrollDetails();
	public CompanyDetails companyDetails = new CompanyDetails();
	public List<Department> departmentList = new ArrayList<Department>();
	public Address address = new Address();

	public EmployeePayrollData(Integer id, String name, Double salary) {
		
		this.employeeId = id;
		this.employeeName = name;
		this.payrollDetails.setBasicSalary(salary);
	}
	
	public EmployeePayrollData(Integer id, String name, Double salary, LocalDate startDate) {
		this(id,name,salary);
		this.startDate = startDate;
	}
	
	public PayrollDetails getPayrollDetails() {
		return payrollDetails;
	}

	public void setPayrollDetails(PayrollDetails payrollDetails) {
		this.payrollDetails = payrollDetails;
	}
	
	public CompanyDetails getCompanyDetails() {
		return companyDetails;
	}

	public void setCompanyDetails(CompanyDetails companyDetails) {
		this.companyDetails = companyDetails;
	}
	
	public int getEmployeeId() {
		return employeeId;
	}

	public String getEmployeeName() {
		return employeeName;
	}

	public LocalDate getStartDate() {
		return startDate;
	}

	public List<Department> getDepartment() {
		return departmentList;
	}

	public Address getAddress() {
		return address;
	}

	public void setEmployeeId(int employeeId) {
		this.employeeId = employeeId;
	}

	public void setEmployeeName(String employeeName) {
		this.employeeName = employeeName;
	}

	public void setStartDate(LocalDate startDate) {
		this.startDate = startDate;
	}

	public void setDepartment(List<Department> department) {
		this.departmentList = department;
	}

	public void setAddress(Address address) {
		this.address = address;
	}
	
	@Override
	public String toString() {
		
		return "EmployeeId: "+employeeId+", EmployeeName: "+employeeName+", EmployeeSalary: "+payrollDetails.basicSalary;
	}
	
	@Override
	public boolean equals(Object object) {
		if(this == object)
			return true;
		if(object == null || getClass() != object.getClass())
			return false;
		EmployeePayrollData that = (EmployeePayrollData) object;
		return employeeId == that.employeeId;
	}
	
}
