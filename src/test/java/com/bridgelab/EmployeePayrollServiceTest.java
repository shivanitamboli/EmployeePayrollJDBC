package com.bridgelab;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.bridgelab.EmployeePayrollService.IOService.DB_IO;

public class EmployeePayrollServiceTest {
	EmployeePayrollService employeePayrollService = null;

	@BeforeEach
	void setUp() {
		employeePayrollService = new EmployeePayrollService();
	}

	@Test
	void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount() {
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
}