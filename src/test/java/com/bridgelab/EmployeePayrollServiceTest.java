package com.bridgelab;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.bridgelab.EmployeePayrollService.IOService.DB_IO;

public class EmployeePayrollServiceTest {
	EmployeePayrollService employeePayrollService = null;

	@BeforeEach
	void setUp() {
		employeePayrollService = new EmployeePayrollService();
	}

	@Test
	void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount() throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(DB_IO);
		Assertions.assertEquals(4, employeePayrollData.size());
	}

	@Test
	void givenNewSalaryForEmployee_WhenUpdated_ShouldSyncWithDB() throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(DB_IO);
		employeePayrollService.updateEmployeeSalary("Terisa", 500000.00);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa");
		Assertions.assertTrue(result);
	}

	@Test
	void givenNewSalaryForEmployee_WhenUpdated_ShouldSyncWithDB_UsingPreparedStatement()
			throws EmployeePayrollException {
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(DB_IO);
		employeePayrollService.updateEmployeeSalaryUsingPreparedStatement("Terisa", 300000.00);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa");
		Assertions.assertTrue(result);
	}

	@Test
	void givenDateRange_WhenRetrieved_ShouldMatchEmployeeCount() throws EmployeePayrollException {
		employeePayrollService.readEmployeePayrollData(DB_IO);
		LocalDate startDate = LocalDate.of(2018, 01, 01);
		LocalDate endDate = LocalDate.now();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService
				.readEmployeePayrollDataForDateRange(DB_IO, startDate, endDate);
		Assertions.assertEquals(4, employeePayrollData.size());
	}

	@Test
	void givenPayrollData_WhenAverageSalaryRetrievedByGender_ShouldReturnProperValue() throws EmployeePayrollException {
		employeePayrollService.readEmployeePayrollData(DB_IO);
		Map<String, Double> averageSalaryByGender = employeePayrollService.readAverageSalaryByGender(DB_IO);
		Assertions.assertTrue(
				averageSalaryByGender.get("M").equals(200000.00) && averageSalaryByGender.get("F").equals(500000.00));
	}

	@Test
	void givenPayrollData_WhenMinimumSalaryRetrievedByGender_ShouldReturnProperValue() throws EmployeePayrollException {
		employeePayrollService.readEmployeePayrollData(DB_IO);
		Map<String, Double> minimumSalaryByGender = employeePayrollService.readMinimumSalaryByGender(DB_IO);
		Assertions.assertTrue(
				minimumSalaryByGender.get("M").equals(100000.00) && minimumSalaryByGender.get("F").equals(500000.00));
	}

	@Test
	void givenPayrollData_WhenMaximumSalaryRetrievedByGender_ShouldReturnProperValue() throws EmployeePayrollException {
		employeePayrollService.readEmployeePayrollData(DB_IO);
		Map<String, Double> maximumSalaryByGender = employeePayrollService.readMaximumSalaryByGender(DB_IO);
		Assertions.assertTrue(
				maximumSalaryByGender.get("M").equals(300000.00) && maximumSalaryByGender.get("F").equals(500000.00));
	}

	@Test
	void givenPayrollData_WhenAdditionOFSalaryRetrievedByGender_ShouldReturnProperValue()
			throws EmployeePayrollException {
		employeePayrollService.readEmployeePayrollData(DB_IO);
		Map<String, Double> totalSalaryByGender = employeePayrollService.readSumOfSalaryByGender(DB_IO);
		Assertions.assertTrue(
				totalSalaryByGender.get("M").equals(600000.00) && totalSalaryByGender.get("F").equals(500000.00));
	}

	@Test
	void givenPayrollData_WhenCountForEmployeesRetrievedByGender_ShouldReturnProperValue()
			throws EmployeePayrollException {
		employeePayrollService.readEmployeePayrollData(DB_IO);
		Map<String, Double> totalNumOfEmployeesByGender = employeePayrollService.readTotalNumOfEmployeesByGender(DB_IO);
		Assertions.assertTrue(totalNumOfEmployeesByGender.get("M") == 3 && totalNumOfEmployeesByGender.get("F") == 1);
	}
}