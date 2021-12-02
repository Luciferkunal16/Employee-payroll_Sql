package com.bridgelabz.employeepayrolljdbcapp;

public class EmployeePayrollException extends RuntimeException {
	
	enum ExceptionType {
		DATABASE_EXCEPTION, 
		UPDATE_FAILED, 
		CONNECTION_FAILED, 
		RESOURCES_NOT_CLOSED_EXCEPTION, 
		COMMIT_FAILED
	}

	public ExceptionType type;

	public EmployeePayrollException(ExceptionType type, String message) {
		super(message);
		this.type = type;
	}

}