package com.bridgelab;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

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
}