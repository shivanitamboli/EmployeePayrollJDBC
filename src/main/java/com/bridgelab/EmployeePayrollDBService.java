package com.bridgelab;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class EmployeePayrollDBService {
	private PreparedStatement employeePayrollDataStatement;
	private static EmployeePayrollDBService employeePayrollDBService;

	private EmployeePayrollDBService() {
	}

	/**
	 * Purpose : To create connection with database
	 *
	 * @return the connection path
	 * @throws SQLException if the connection is not done
	 */
	private Connection getConnection() throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "1SACHINkore7a";
		Connection connection;
		System.out.println("Connecting to database:" + jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("Connection is successful!!!!" + connection);
		return connection;
	}

	/**
	 * Purpose : To update the salary in the database using Statement Interface
	 *
	 * @param name   : takes the name of the employee
	 * @param salary : takes the salary of the employee
	 * @return the updated query
	 * @throws EmployeePayrollException if employee details are not found
	 */
	private int updateEmployeeDataUsingStatement(String name, double salary) throws EmployeePayrollException {
		String sql = String.format("UPDATE employee_payroll SET salary = %.2f WHERE name = '%s';", salary, name);
		try (Connection connection = this.getConnection()) {
			Statement statement = connection.createStatement();
			return statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw new EmployeePayrollException(
					"Please check the updateEmployeeDataUsingStatement() for detailed information");
		}
	}

	/**
	 * Purpose : Assigning the values of the attributes in a list
	 *
	 * @param resultSet : taking the values of the employee
	 * @return the employee payroll list
	 * @throws EmployeePayrollException if the details of the employee is not found
	 */
	private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet) throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		try {
			while (resultSet.next()) {
				int id = resultSet.getInt("id");
				String name = resultSet.getString("name");
				double salary = resultSet.getDouble("salary");
				LocalDate startDate = resultSet.getDate("start_date").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(id, name, salary, startDate));
			}
		} catch (SQLException e) {
			throw new EmployeePayrollException(
					"Please check the getEmployeePayrollData(resultSet) for detailed information");
		}
		return employeePayrollList;
	}

	/**
	 * Purpose : To create the connection for executing query and assigned the value
	 * in a list
	 *
	 * @param sql : taking the values from database for reading
	 * @return the employee payroll list
	 * @throws EmployeePayrollException if employee details are not found
	 */
	private List<EmployeePayrollData> getEmployeePayrollDataUsingDB(String sql) throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		try (Connection connection = this.getConnection()) {
			Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery(sql);
			while (result.next()) {
				int id = result.getInt("id");
				String name = result.getString("name");
				double salary = result.getDouble("salary");
				LocalDate startDate = result.getDate("start_date").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(id, name, salary, startDate));
			}
		} catch (SQLException e) {
			throw new EmployeePayrollException(
					"Please Check the getEmployeePayrollDataUsingDB() for detailed information");
		}
		return employeePayrollList;
	}

	/**
	 * Purpose : To get the details of a particular employee from the database using
	 * PreparedStatement Interface
	 *
	 * @throws EmployeePayrollException if the details of that particular employee
	 *                                  is not found
	 */
	private void preparedStatementForEmployeeData() throws EmployeePayrollException {
		try {
			Connection connection = this.getConnection();
			String sql = "SELECT * FROM employee_payroll WHERE name = ?";
			employeePayrollDataStatement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			throw new EmployeePayrollException(
					"Please check the prepareStatementForEmployeeData() for detailed information");
		}
	}

	/**
	 * Purpose : To update the employee salary in the database using
	 * PreparedStatement Interface
	 *
	 * @param name   : takes the name of that particular employee
	 * @param salary : takes the salary of that particular employee
	 * @return the updated salary of that assigned employee
	 * @throws EmployeePayrollException if the assigned employee details is not
	 *                                  found
	 */
	private int updateEmployeeDataUsingPreparedStatement(String name, double salary) throws EmployeePayrollException {
		String sql = "UPDATE employee_payroll SET salary = ? WHERE name = ?";
		try (Connection connection = this.getConnection()) {
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setDouble(1, salary);
			statement.setString(2, name);
			return statement.executeUpdate();
		} catch (SQLException e) {
			throw new EmployeePayrollException(
					"Please check the updateEmployeePreparedStatement() for detailed information");
		}
	}

	/**
	 * Purpose : For creating a singleton object
	 *
	 * @return the classpath
	 */
	public static EmployeePayrollDBService getInstance() {
		if (employeePayrollDBService == null)
			employeePayrollDBService = new EmployeePayrollDBService();
		return employeePayrollDBService;
	}

	/**
	 * Purpose : To read employee payroll from database using JDBC
	 *
	 * @return employees details
	 */
	public List<EmployeePayrollData> readData() throws EmployeePayrollException {
		String sql = "SELECT * FROM employee_payroll";
		return getEmployeePayrollDataUsingDB(sql);
	}

	/**
	 * Purpose : To update the salary of the employee in the database using
	 * Statement Interface
	 *
	 * @param name   : takes the name of the employee
	 * @param salary : takes the salary of the employee to update
	 * @return the updated query
	 * @throws EmployeePayrollException if employee details are not found
	 */
	public int updateEmployeeData(String name, double salary) throws EmployeePayrollException {
		return this.updateEmployeeDataUsingStatement(name, salary);
	}

	/**
	 * Purpose : To get the list of employee payroll data using the assigned name
	 *
	 * @param name : taking name of the employee
	 * @return the details of the assigned employee
	 * @throws EmployeePayrollException if the assigned employee details are not
	 *                                  found
	 */
	public List<EmployeePayrollData> getEmployeePayrollData(String name) throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollList = null;
		if (this.employeePayrollDataStatement == null)
			this.preparedStatementForEmployeeData();
		try {
			employeePayrollDataStatement.setString(1, name);
			ResultSet resultSet = employeePayrollDataStatement.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		} catch (SQLException e) {
			throw new EmployeePayrollException(
					"Please check the getEmployeePayrollData(name) for detailed information");
		}
		return employeePayrollList;
	}

	/**
	 * Purpose : To read the data for a certain date range from the database
	 *
	 * @param startDate : taking the starting date
	 * @param endDate   : taking the end date
	 * @return the details of the employees
	 * @throws EmployeePayrollException if the details of the employees are not
	 *                                  found
	 */
	public List<EmployeePayrollData> getEmployeePayrollForDateRange(LocalDate startDate, LocalDate endDate)
			throws EmployeePayrollException {
		String sql = String.format("SELECT * FROM employee_payroll WHERE start_date BETWEEN '%s' AND '%s';",
				Date.valueOf(startDate), Date.valueOf(endDate));
		return this.getEmployeePayrollDataUsingDB(sql);
	}

	/**
	 * Purpose : To update the salary in the database using PreparedStatement
	 * Interface
	 *
	 * @param name   : takes the name of that particular employee
	 * @param salary : takes the salary of that particular employee
	 * @return the updated salary of that assigned employee
	 * @throws EmployeePayrollException if the assigned employee details is not
	 *                                  found
	 */
	public int updateEmployeeDataPreparedStatement(String name, double salary) throws EmployeePayrollException {
		return this.updateEmployeeDataUsingPreparedStatement(name, salary);
	}
}