package com.bridgelab;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;

import static com.bridgelab.EmployeePayrollService.IOService.DB_IO;

public class EmployeePayrollServiceTest {
	@Test
	void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readEmployeePayrollData(DB_IO);
		Assertions.assertEquals(4, employeePayrollData.size());
	}
}