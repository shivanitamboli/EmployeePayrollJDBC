package com.bridgelab;

import java.time.LocalDate;
import java.util.List;

public class EmployeePayrollService {

	private final EmployeePayrollDBService employeePayrollDBService;

	public enum IOService {
		DB_IO
	}

	private List<EmployeePayrollData> employeePayrollList;

	public EmployeePayrollService() {
		employeePayrollDBService = EmployeePayrollDBService.getInstance();
	}

	/**
	 * Purpose : To get the list of employee payroll from the database
	 *
	 * @param ioService : taking as a parameter
	 * @return the list of employees
	 */
	public List<EmployeePayrollData> readEmployeePayrollData(IOService ioService) throws EmployeePayrollException {
		if (ioService.equals(IOService.DB_IO))
			this.employeePayrollList = employeePayrollDBService.readData();
		return this.employeePayrollList;
	}

	/**
	 * Purpose : Retrieving the data up to given date range
	 *
	 * @param ioService : taking enum
	 * @param startDate : taking the starting date
	 * @param endDate   : taking the end date
	 * @return the details of the employees within the given range
	 * @throws EmployeePayrollException if the details of the employees are not
	 *                                  found
	 */
	public List<EmployeePayrollData> readEmployeePayrollDataForDateRange(IOService ioService, LocalDate startDate,
			LocalDate endDate) throws EmployeePayrollException {
		if (ioService.equals(IOService.DB_IO))
			return employeePayrollDBService.getEmployeePayrollForDateRange(startDate, endDate);
		return null;
	}

	/**
	 * Purpose : To update the employee salary in the database
	 *
	 * @param name   : taking name of the employee
	 * @param salary : taking salary of the employee
	 * @throws EmployeePayrollException if employee details are not found
	 */
	public void updateEmployeeSalary(String name, double salary) throws EmployeePayrollException {
		int result = employeePayrollDBService.updateEmployeeData(name, salary);
		if (result == 0)
			return;
		EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
		if (employeePayrollData != null)
			employeePayrollData.salary = salary;
	}

	/**
	 * Purpose : Checking the employee payroll data list for the name
	 *
	 * @param name : takes name of the employee
	 * @return the data if found else null
	 */
	private EmployeePayrollData getEmployeePayrollData(String name) {
		return this.employeePayrollList.stream()
				.filter(employeePayrollDataItem -> employeePayrollDataItem.name.equals(name)).findFirst().orElse(null);
	}

	/**
	 * Purpose : To check the employee payroll data is synced with database
	 *
	 * @param name : takes name of the employee
	 * @return the details after using equals() method to compare the name
	 * @throws EmployeePayrollException if data is not exists
	 */
	public boolean checkEmployeePayrollInSyncWithDB(String name) throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollDataList = employeePayrollDBService.getEmployeePayrollData(name);
		return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
	}

	/**
	 * Purpose : To update the employee salary in the database using prepared
	 * statement
	 *
	 * @param name   : taking the name of employee for matching with
	 *               EmployeePayrollData list
	 * @param salary : taking the salary of assigned employee for updating and
	 *               passing in EmployeePayrollData list
	 * @throws EmployeePayrollException if the assigned employee details are not
	 *                                  found
	 */
	public void updateEmployeeSalaryUsingPreparedStatement(String name, double salary) throws EmployeePayrollException {
		int result = employeePayrollDBService.updateEmployeeDataPreparedStatement(name, salary);
		if (result == 0)
			return;
		EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
		if (employeePayrollData != null)
			employeePayrollData.salary = salary;
	}
}