package com.lits.kundera.test;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

public abstract class BaseTest {
    public static final String TABLES_PHYSICIAN = "physicians";
    public static final String TABLES_PATIENT = "patients";
    public static final String TABLES_MEDICAL_RECORD = "medical_records";

    public static final String COLUMN_PHYSICIAN_CLINIC_NAME = "clinic_name";
    public static final String COLUMN_PHYSICIAN_FULL_NAME = "full_name";
    public static final String COLUMN_PHYSICIAN_SPECIALIZATION = "specialization";

    public static final String COLUMN_PATIENT_FIRST_NAME = "first_name";
    public static final String COLUMN_PATIENT_LAST_NAME = "last_name";
    public static final String COLUMN_PATIENT_DATE_OF_BIRTH = "date_of_birth";

    public static final String COLUMN_MEDICAL_RECORD_TYPE = "type";
    public static final String COLUMN_MEDICAL_RECORD_DESCRIPTION = "description";
    public static final String COLUMN_MEDICAL_RECORD_DATE_PERFORMED = "date_performed";

    private Table physiciansTable;
    private Table patientsTable;
    private Table medicalRecordsTable;

    private Connection connection;

    /**
     * Setting up tables to query them in tests
     *
     * @throws IOException
     */
    private void beforeSuite() throws IOException {
        Configuration configuration = new Configuration(true);
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        connection = ConnectionFactory.createConnection(configuration);

        physiciansTable = connection.getTable(TableName.valueOf(TABLES_PHYSICIAN));
        patientsTable = connection.getTable(TableName.valueOf(TABLES_PATIENT));
        medicalRecordsTable = connection.getTable(TableName.valueOf(TABLES_MEDICAL_RECORD));

    }

    /**
     * Custom test method that should be implemented by students
     *
     * @throws IOException
     */
    public abstract void customTest() throws IOException;

    /**
     * Main method where all the stuff happens
     *
     * @throws IOException
     */
    public void runSuite() throws IOException {
        beforeSuite();
        mainTest();
        customTest();
        afterSuite();
    }

    /**
     * Main test method that will check all the meta
     *
     * @throws IOException
     */
    private void mainTest() throws IOException {
        testNoEmptyTables();
        testColumns();
        testResultContainsAllColumns();
    }

    /**
     * Main test method that check whether all columns are present
     *
     * @throws IOException
     */
    private void testColumns() throws IOException {
        testPhysicianColumns();
        testPatientColumns();
        testMedicalRecordColumns();
    }

    /**
     * Main test method that check whether all columns are present in MR table
     *
     * @throws IOException
     */
    private void testMedicalRecordColumns() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_DATE_PERFORMED.getBytes());
        ResultScanner scanner = medicalRecordsTable.getScanner(scan);
        boolean presentDatePerformed = scanner.iterator().next().containsColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_DATE_PERFORMED.getBytes());

        if(!presentDatePerformed) {
            System.out.println("FAIL : No column Date Performed present in Medical Record table");
            exit();
        }

        scan = new Scan();
        scan.addColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_DESCRIPTION.getBytes());
        scanner = medicalRecordsTable.getScanner(scan);
        boolean presentDescription = scanner.iterator().next().containsColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_DESCRIPTION.getBytes());

        if(!presentDescription) {
            System.out.println("FAIL : No column Description present in Medical Record table");
            exit();
        }

        scan = new Scan();
        scan.addColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_TYPE.getBytes());
        scanner = medicalRecordsTable.getScanner(scan);
        boolean presentType = scanner.iterator().next().containsColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_TYPE.getBytes());

        if(!presentType) {
            System.out.println("FAIL : No column Type present in Medical Record table");
            exit();
        }
    }

    /**
     * Main test method that check whether all columns are present in Physician table
     *
     * @throws IOException
     */
    private void testPhysicianColumns() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_CLINIC_NAME.getBytes());
        ResultScanner scanner = physiciansTable.getScanner(scan);
        boolean presentClinicName = scanner.iterator().next().containsColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_CLINIC_NAME.getBytes());

        if(!presentClinicName) {
            System.out.println("FAIL : No column Clinic Name present in Physician table");
            exit();
        }

        scan = new Scan();
        scan.addColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_FULL_NAME.getBytes());
        scanner = physiciansTable.getScanner(scan);
        boolean presentFullName = scanner.iterator().next().containsColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_FULL_NAME.getBytes());

        if(!presentFullName) {
            System.out.println("FAIL : No column Full Name present in Physician table");
            exit();
        }

        scan = new Scan();
        scan.addColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_SPECIALIZATION.getBytes());
        scanner = physiciansTable.getScanner(scan);
        boolean presentSpecialization = scanner.iterator().next().containsColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_SPECIALIZATION.getBytes());

        if(!presentSpecialization) {
            System.out.println("FAIL : No column Specialization present in Physician table");
            exit();
        }
    }

    /**
     * Main test method that check whether all columns are present in Patient table
     *
     * @throws IOException
     */
    private void testPatientColumns() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_FIRST_NAME.getBytes());
        ResultScanner scanner = patientsTable.getScanner(scan);
        boolean presentFirstName = scanner.iterator().next().containsColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_FIRST_NAME.getBytes());

        if(!presentFirstName) {
            System.out.println("FAIL : No column First Name present in Patient table");
            exit();
        }

        scan = new Scan();
        scan.addColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_LAST_NAME.getBytes());
        scanner = patientsTable.getScanner(scan);
        boolean presentLastName = scanner.iterator().next().containsColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_LAST_NAME.getBytes());

        if(!presentLastName) {
            System.out.println("FAIL : No column Last Name present in Patient table");
            exit();
        }

        scan = new Scan();
        scan.addColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_DATE_OF_BIRTH.getBytes());
        scanner = patientsTable.getScanner(scan);
        boolean presentDateOfBirth = scanner.iterator().next().containsColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_DATE_OF_BIRTH.getBytes());

        if(!presentDateOfBirth) {
            System.out.println("FAIL : No column Date Of Birth present in Patient table");
            exit();
        }
    }

    /**
     * Main test method that check whether data is present in table
     *
     * @throws IOException
     */
    private void testNoEmptyTables() throws IOException {
        testNotEmptyPhysicians();
        testNotEmptyPatients();
        testNotEmptyMedicalRecords();
    }

    /**
     * Main test method that check whether data is present in Physician table
     *
     * @throws IOException
     */
    private void testNotEmptyPhysicians() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = physiciansTable.getScanner(scan);

        boolean hasRecords = scanner.iterator().hasNext();

        if(!hasRecords) {
            System.out.println("FAIL : No data present in Physician table");
            exit();
        }
    }

    /**
     * Main test method that check whether data is present in Patient table
     *
     * @throws IOException
     */
    private void testNotEmptyPatients() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = patientsTable.getScanner(scan);

        boolean hasRecords = scanner.iterator().hasNext();

        if(!hasRecords) {
            System.out.println("FAIL : No data present in Patient table");
            exit();
        }
    }

    /**
     * Main test method that check whether data is present in Medical Record table
     *
     * @throws IOException
     */
    private void testNotEmptyMedicalRecords() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = medicalRecordsTable.getScanner(scan);

        boolean hasRecords = scanner.iterator().hasNext();

        if(!hasRecords) {
            System.out.println("FAIL : No data present in Medical Record table");
            exit();
        }
    }

    /**
     * Main test method that check whether all columns are present in data in table
     *
     * @throws IOException
     */
    private void testResultContainsAllColumns() throws IOException {
        boolean hasAllColumnsPhysician = testPhysicianResultContainsAllColumns();

        if(!hasAllColumnsPhysician) {
            System.out.println("FAIL : Any Result have all columns in Physician table");
            exit();
        }

        boolean hasAllColumnsPatient = testPatientResultContainsAllColumns();

        if(!hasAllColumnsPatient) {
            System.out.println("FAIL : Any Result have all columns in Patient table");
            exit();
        }
        boolean hasAllColumnsMedicalRecord = testMedicalRecordResultContainsAllColumns();

        if(!hasAllColumnsMedicalRecord) {
            System.out.println("FAIL : Any Result have all columns in Medical Record table");
            exit();
        }
    }

    /**
     * Main test method that check whether all columns are present in data in Physician table
     *
     * @throws IOException
     */
    private boolean testPhysicianResultContainsAllColumns() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = physiciansTable.getScanner(scan);
        Iterator<Result> resultIterator = scanner.iterator();

        while (resultIterator.hasNext()) {
            Result result = resultIterator.next();

            if(!result.isEmpty()) {
                boolean containsFullName = result.containsColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_FULL_NAME.getBytes());
                boolean containsClinicName = result.containsColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_CLINIC_NAME.getBytes());
                boolean containsSpecialization = result.containsColumn(TABLES_PHYSICIAN.getBytes(), COLUMN_PHYSICIAN_SPECIALIZATION.getBytes());
                if(containsFullName && containsClinicName && containsSpecialization) {
                    return true;
                }

            }
        }
        return false;
    }

    /**
     * Main test method that check whether all columns are present in data in Patient table
     *
     * @throws IOException
     */
    private boolean testPatientResultContainsAllColumns() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = patientsTable.getScanner(scan);
        Iterator<Result> resultIterator = scanner.iterator();

        while (resultIterator.hasNext()) {
            Result result = resultIterator.next();

            if(!result.isEmpty()) {
                boolean containsFirstName = result.containsColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_FIRST_NAME.getBytes());
                boolean containsLastName = result.containsColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_LAST_NAME.getBytes());
                boolean containsDateOfBirth = result.containsColumn(TABLES_PATIENT.getBytes(), COLUMN_PATIENT_DATE_OF_BIRTH.getBytes());
                if(containsFirstName && containsLastName && containsDateOfBirth) {
                    return true;
                }

            }
        }
        return false;
    }

    /**
     * Main test method that check whether all columns are present in data in MR table
     *
     * @throws IOException
     */
    private boolean testMedicalRecordResultContainsAllColumns() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = medicalRecordsTable.getScanner(scan);
        Iterator<Result> resultIterator = scanner.iterator();

        while (resultIterator.hasNext()) {
            Result result = resultIterator.next();

            if(!result.isEmpty()) {
                boolean containsDatePerformed = result.containsColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_DATE_PERFORMED.getBytes());
                boolean containsType = result.containsColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_TYPE.getBytes());
                boolean containsDescription = result.containsColumn(TABLES_MEDICAL_RECORD.getBytes(), COLUMN_MEDICAL_RECORD_DESCRIPTION.getBytes());
                if(containsDatePerformed && containsType && containsDescription) {
                    return true;
                }

            }
        }
        return false;
    }

    /**
     * Checking the difference between full scan with value comparison and
     * prefix scan that has the half of the key to see how fast the prefix scan is
     *
     * @param medicalRecordType type Of medical record we're searching for
     * @param patientId patient id as a part of a medical record key
     * @throws IOException
     */
    public static void prefixScanTest(String medicalRecordType, byte[] patientId, Table medicalRecordsHbaseTable) throws IOException {

        System.out.println("======= Starting... =======");
        System.out.println("Preparing full scan");
        //configuring scan to search only for efined medical record type
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(TABLES_MEDICAL_RECORD.getBytes(),
                COLUMN_MEDICAL_RECORD_TYPE.getBytes(), CompareFilter.CompareOp.EQUAL, medicalRecordType.getBytes());

        Scan fullScan = new Scan();
        fullScan.setFilter(singleColumnValueFilter);
        //start counting the time
        long startTime = System.currentTimeMillis();

        ResultScanner resultScanner = medicalRecordsHbaseTable.getScanner(fullScan);
        scannerCheck(resultScanner, medicalRecordType, startTime);

        //now with prefix
        System.out.println("Preparing prefix scan");
        Scan prefixScan = Util.createKeyPrefixScan(ArrayUtils.addAll(patientId, new byte[16]));
        prefixScan.setFilter(singleColumnValueFilter);

        startTime = System.currentTimeMillis();

       resultScanner = medicalRecordsHbaseTable.getScanner(prefixScan);
       scannerCheck(resultScanner, medicalRecordType, startTime);

        System.out.println("======= Finishing... =======");
    }

    private static void scannerCheck(ResultScanner resultScanner, String medicalRecordType, long startTime) throws IOException {
        long executionTime;
        if(resultScanner != null && resultScanner.iterator().hasNext()) {
            while(resultScanner.iterator().hasNext()) {
                Result result = resultScanner.next();
                if(result != null  && !result.isEmpty()) {
                    byte[] value = result.getValue(TABLES_MEDICAL_RECORD.getBytes(),
                            COLUMN_MEDICAL_RECORD_TYPE.getBytes());

                    if(Arrays.equals(value, medicalRecordType.getBytes())) {
                        System.out.println("Record found");
                        executionTime = System.currentTimeMillis() - startTime;
                        System.out.printf("It took %d ms to find the record", executionTime);
                        System.out.println();
                        return;
                    }
                }
            }
        } else {
            System.out.println("Something went wrong, no record found. Check whether record with that type exists. Aborting.");
        }
    }

    /**
     * Cleaning all connections
     *
     * @throws IOException
     */
    private void afterSuite() throws IOException {
        System.out.println("SUCCESS : All tests passed.");
        physiciansTable.close();
        patientsTable.close();
        medicalRecordsTable.close();

        connection.close();
    }

    public void exit() {
        System.out.println("There are tests failures. Exiting now.");
        System.exit(1);
    }
}
